#!/usr/bin/env bash

# End-to-end pipeline:
#  - local synthetic file generation
#  - upload to HDFS via namenode
#  - local access simulation
#  - Spark feature extraction via dockerized Spark

set -euo pipefail

# -----------------------------
# Paths & parameters
# -----------------------------
ROOT="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="${ROOT}/src"
DOCKER_DIR="${ROOT}/docker"
HADOOP_CONF_HOST_DIR="${DOCKER_DIR}/hadoop_conf"

NAMENODE="namenode"
SPARK_SERVICE="spark"

HDFS_DIR="/user/root/synth"
LOCAL_SYNTH_DIR="${ROOT}/local_synth"

METADATA_CSV="${SRC_DIR}/metadata.csv"
ACCESS_LOG="${SRC_DIR}/access.log"

OUT_DIR="${ROOT}/output"

NUM_FILES="${1:-200}"
DURATION="${2:-600}"
CLIENTS="dn1,dn2,dn3"

# -----------------------------
# Helpers
# -----------------------------
die(){ echo "ERROR: $*" >&2; exit 1; }
info(){ echo ">>> $*"; }

command -v docker >/dev/null || die "docker not found"
command -v docker-compose >/dev/null || die "docker-compose not found"
command -v python3 >/dev/null || die "python3 not found on host"

# -----------------------------
# 1) Local synthetic file generation
# -----------------------------
info "1) Generating ${NUM_FILES} synthetic files locally"

rm -rf "${LOCAL_SYNTH_DIR}" "${METADATA_CSV}"
mkdir -p "${LOCAL_SYNTH_DIR}"

python3 - <<PYGEN
import os, random, csv
from datetime import datetime, timedelta

out_dir = "${LOCAL_SYNTH_DIR}"
n = int("${NUM_FILES}")
nodes = "${CLIENTS}".split(",")

os.makedirs(out_dir, exist_ok=True)
manifest = []

for i in range(n):
    size = random.randint(1024, 1024*1024)
    fname = f"synth_{i}.bin"
    local_path = os.path.join(out_dir, fname)

    with open(local_path, "wb") as f:
        f.write(os.urandom(size))

    creation = datetime.utcnow() - timedelta(days=random.random()*365)
    manifest.append({
        "path": os.path.join("${HDFS_DIR}", fname),
        "creation_ts": creation.isoformat()+"Z",
        "primary_node": random.choice(nodes),
        "size_bytes": size,
        "category": random.choices(
            ["hot","shared","moderate","archival"],
            weights=[0.10,0.20,0.50,0.20]
        )[0]
    })

with open("${METADATA_CSV}", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=manifest[0].keys())
    w.writeheader()
    w.writerows(manifest)

print(f"Wrote metadata.csv with {len(manifest)} entries")
PYGEN

# -----------------------------
# 2) Upload files to HDFS
# -----------------------------
info "2) Uploading files to HDFS via ${NAMENODE}"

DOCKER_TMP="/tmp/local_synth"
docker exec "${NAMENODE}" bash -c "rm -rf ${DOCKER_TMP} && mkdir -p ${DOCKER_TMP}"
docker cp "${LOCAL_SYNTH_DIR}/." "${NAMENODE}:${DOCKER_TMP}/"

docker exec "${NAMENODE}" bash -c "
  hdfs dfs -mkdir -p ${HDFS_DIR} || true
  for f in ${DOCKER_TMP}/*; do
    hdfs dfs -put -f \$f ${HDFS_DIR}/
  done
  hdfs dfs -ls ${HDFS_DIR} | head
"

docker cp "${METADATA_CSV}" "${NAMENODE}:/opt/synth-code/metadata.csv"

# -----------------------------
# 3) Local access simulation
# -----------------------------
info "3) Running access_simulator locally (${DURATION}s)"

rm -f "${ACCESS_LOG}"

python3 "${SRC_DIR}/access_simulator.py" \
  --manifest "${METADATA_CSV}" \
  --out "${ACCESS_LOG}" \
  --duration_seconds "${DURATION}" \
  --clients "${CLIENTS}"

docker cp "${ACCESS_LOG}" "${NAMENODE}:/opt/synth-code/access.log"

# -----------------------------
# 4) Extract Hadoop config (robust)
# -----------------------------
info "4) Extracting Hadoop configuration from namenode"

rm -rf "${HADOOP_CONF_HOST_DIR}"
mkdir -p "${HADOOP_CONF_HOST_DIR}"

CONF_PATH=$(docker exec "${NAMENODE}" bash -c '
  for p in /opt/hadoop/etc/hadoop /hadoop/etc/hadoop /etc/hadoop; do
    [ -d "$p" ] && echo "$p" && exit 0
  done
  exit 1
') || die "Could not locate Hadoop config directory in namenode"

info "Detected Hadoop conf dir: ${CONF_PATH}"

for f in core-site.xml hdfs-site.xml yarn-site.xml; do
  docker cp "${NAMENODE}:${CONF_PATH}/${f}" "${HADOOP_CONF_HOST_DIR}/" \
    || die "Missing ${f} in ${CONF_PATH}"
done

# -----------------------------
# Patch YARN config for Docker networking & resources
# -----------------------------
info "4b) Patching yarn-site.xml for Docker networking and resources"

YARN_SITE="${HADOOP_CONF_HOST_DIR}/yarn-site.xml"

[ -f "${YARN_SITE}" ] || die "yarn-site.xml not found at ${YARN_SITE}"

# ---- helper: add or replace property safely ----
add_or_replace_prop() {
  local pname="$1"
  local pvalue="$2"

  if grep -q "<name>${pname}</name>" "${YARN_SITE}"; then
    # replace existing value
    sed -i \
      "s#<name>${pname}</name>[[:space:]]*<value>.*</value>#<name>${pname}</name><value>${pvalue}</value>#" \
      "${YARN_SITE}"
  else
    # insert new property before closing tag
    sed -i \
      "/<\/configuration>/i \
  <property>\n\
    <name>${pname}</name>\n\
    <value>${pvalue}</value>\n\
  </property>" \
      "${YARN_SITE}"
  fi
}

# ---- mandatory fixes ----
add_or_replace_prop yarn.resourcemanager.hostname resourcemanager

# ---- resource configuration (prevents ACCEPTED forever) ----
add_or_replace_prop yarn.nodemanager.resource.memory-mb 4096
add_or_replace_prop yarn.scheduler.maximum-allocation-mb 4096
add_or_replace_prop yarn.scheduler.minimum-allocation-mb 512
add_or_replace_prop yarn.nodemanager.resource.cpu-vcores 2

info "yarn-site.xml patched successfully"

HDFS_OUTPUT_DIR="/opt/synth-code/features_out" 

docker exec "${NAMENODE}" bash -c "
  hdfs dfs -rm -r -skipTrash ${HDFS_OUTPUT_DIR} || true
"

# -----------------------------
# 5) Run Spark feature extraction
# -----------------------------
info "5) Running Spark feature extraction (compute_features.py)"

rm -rf "${SRC_DIR}/features_out"
mkdir -p "${SRC_DIR}/features_out"

SPARK_OUTPUT_TEMP="/tmp/spark_output" 
FINAL_MOUNT_PATH="/opt/synth-code/features_out"

docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --master local[*] \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  --conf spark.hadoop.fs.permissions.umask-mode=000 \
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
  /opt/synth-code/compute_features.py \
  --manifest file:///opt/synth-code/metadata.csv \
  --access_log file:///opt/synth-code/access.log \
  --out ${SPARK_OUTPUT_TEMP}

  docker exec spark bash -c "
  # Remove the final destination directory first, using a shell command
  rm -rf ${FINAL_MOUNT_PATH}
  
  # Move the temporary output folder to the final mounted directory
  mv ${SPARK_OUTPUT_TEMP} ${FINAL_MOUNT_PATH}
"

# -----------------------------
# 6) Collect outputs
# -----------------------------
info "6) Collecting outputs into ${OUT_DIR}"

rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}"

cp -r "${SRC_DIR}/features_out/." "${OUT_DIR}/"

info "Pipeline complete"
info "Results available in: ${OUT_DIR}"