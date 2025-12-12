#!/usr/bin/env bash
# run_pipeline.sh - pipeline that uses local generation + upload workaround (no python in namenode required)
# Usage: ./run_pipeline.sh [NUM_FILES] [DURATION_SECONDS]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="${ROOT}/src"
DOCKER_DIR="${ROOT}/docker"
HADOOP_CONF_HOST_DIR="${DOCKER_DIR}/hadoop_conf"
NAMENODE="namenode"
SPARK_SERVICE="spark"
HDFS_DIR="/user/root/synth"
LOCAL_SYNTH_DIR="${ROOT}/local_synth"
METADATA_CSV="${ROOT}/src/metadata.csv"
ACCESS_LOG="${ROOT}/src/access.log"
OUT_DIR="${ROOT}/output"
NUM_FILES="${1:-200}"
DURATION="${2:-600}"
CLIENTS="dn1,dn2,dn3"

# --- Helpers ---
die(){ echo "ERROR: $*" >&2; exit 1; }
info(){ echo ">>> $*"; }

# check docker-compose / docker available
command -v docker >/dev/null || die "docker not found. Start Docker Desktop / daemon."
command -v docker-compose >/dev/null || die "docker-compose not found."

# check python3 on host for local generation/simulation
command -v python3 >/dev/null || die "python3 not found on host. Install python3 to run generator & simulator locally."

# 1) local synthetic file generation (create files and metadata CSV)
info "1) Generating ${NUM_FILES} synthetic files locally in ${LOCAL_SYNTH_DIR} and writing metadata to ${METADATA_CSV}"
rm -rf "${LOCAL_SYNTH_DIR}" "${METADATA_CSV}"
mkdir -p "${LOCAL_SYNTH_DIR}"

python3 - <<PYGEN
import os, random, csv, argparse
from datetime import datetime, timedelta

out_dir = os.environ.get("LOCAL_SYNTH_DIR", "${LOCAL_SYNTH_DIR}")
n = int(os.environ.get("NUM_FILES", "${NUM_FILES}"))
min_size = 1024
max_size = 1024*1024
nodes = "${CLIENTS}".split(",")
os.makedirs(out_dir, exist_ok=True)
manifest = []
for i in range(n):
    size = random.randint(min_size, max_size)
    fname = f"synth_{i}.bin"
    path = os.path.join(out_dir, fname)
    # write random bytes (fast)
    with open(path, "wb") as f:
        f.write(os.urandom(size))
    delta_days = random.random() * 365.0
    creation = datetime.utcnow() - timedelta(days=delta_days)
    primary_node = random.choice(nodes)
    category = random.choices(["hot","shared","moderate","archival"], weights=[0.10,0.20,0.50,0.20])[0]
    manifest.append({
        "path": os.path.join("${HDFS_DIR}", fname),
        "creation_ts": creation.isoformat()+"Z",
        "primary_node": primary_node,
        "size_bytes": size,
        "category": category,
        "local_path": path
    })
# write manifest for simulator / later steps (metadata.csv)
meta_csv = "${METADATA_CSV}"
os.makedirs(os.path.dirname(meta_csv), exist_ok=True)
with open(meta_csv, "w", newline='') as f:
    writer = csv.DictWriter(f, fieldnames=["path","creation_ts","primary_node","size_bytes","category","local_path"])
    writer.writeheader()
    for r in manifest:
        writer.writerow(r)
print(f"WROTE {meta_csv} with {len(manifest)} entries")
PYGEN

# 2) upload files into Namenode container then put to HDFS from inside container
info "2) Copying generated files into '${NAMENODE}' container and putting into HDFS '${HDFS_DIR}'."
# copy local_synth into container's /tmp/local_synth
DOCKER_TMP="/tmp/local_synth"
docker exec "${NAMENODE}" bash -c "rm -rf ${DOCKER_TMP} && mkdir -p ${DOCKER_TMP}"
# copy files in batch using docker cp (copy the whole directory)
docker cp "${LOCAL_SYNTH_DIR}/." "${NAMENODE}:${DOCKER_TMP}/"

# now inside container: make HDFS dir and put files
docker exec -i "${NAMENODE}" bash -c "hdfs dfs -mkdir -p ${HDFS_DIR} || true; for f in ${DOCKER_TMP}/*; do echo 'putting' \$f; hdfs dfs -put -f \$f ${HDFS_DIR}/ || true; done; echo 'HDFS ls:'; hdfs dfs -ls -R ${HDFS_DIR} | head -n 50"

# copy the metadata CSV into /opt/synth-code (so simulator or spark job can find it)
info "Copying metadata CSV into ${NAMENODE}:/opt/synth-code/metadata.csv"
docker cp "${METADATA_CSV}" "${NAMENODE}:/opt/synth-code/metadata.csv"

# 3) run access_simulator locally (it writes access.log in src/)
info "3) Running access_simulator locally to produce ${ACCESS_LOG} (duration ${DURATION}s)."
python3 "${SRC_DIR}/access_simulator.py" --manifest "${METADATA_CSV}" --out "${ACCESS_LOG}" --duration_seconds "${DURATION}" --clients "${CLIENTS}"

# 4) copy access.log into namenode container (so spark job can read it from /opt/synth-code)
info "4) Copying access.log into ${NAMENODE}:/opt/synth-code/access.log"
docker cp "${ACCESS_LOG}" "${NAMENODE}:/opt/synth-code/access.log"

# 5) copy hadoop config files from namenode to host for spark's HADOOP_CONF_DIR
info "5) Copying Hadoop config files from ${NAMENODE} to ${HADOOP_CONF_HOST_DIR}"
mkdir -p "${HADOOP_CONF_HOST_DIR}"
for f in core-site.xml hdfs-site.xml yarn-site.xml; do
  set +e
  docker cp "${NAMENODE}:/opt/hadoop/etc/hadoop/${f}" "${HADOOP_CONF_HOST_DIR}/" 2>/dev/null
  rc=$?
  set -e
  if [ $rc -ne 0 ]; then
    echo "Warning: could not copy ${f} from namenode; check path /opt/hadoop/etc/hadoop/${f} inside the container"
  else
    echo "Copied ${f}"
  fi
done

# 6) run spark-submit inside spark service (docker-compose run)
info "6) Running spark-submit in '${SPARK_SERVICE}' container (YARN client mode). This will submit compute_features.py."
# Ensure features_out dir exists (in src)
rm -rf "${SRC_DIR}/features_out" || true
mkdir -p "${SRC_DIR}/features_out"

docker-compose -f "${DOCKER_DIR}/docker-compose.yml" run --rm -v "${HADOOP_CONF_HOST_DIR}:/opt/hadoop-conf:ro" -v "${SRC_DIR}:/opt/synth-code:ro" --entrypoint "" "${SPARK_SERVICE}" \
  bash -c "export HADOOP_CONF_DIR=/opt/hadoop-conf && \
           SPARK_HOME=\$(ls -d /opt/spark* 2>/dev/null | head -n1 || true) && \
           if [ -x \"/opt/spark/bin/spark-submit\" ]; then SPARK_SUBMIT=/opt/spark/bin/spark-submit; elif [ -x \"/usr/bin/spark-submit\" ]; then SPARK_SUBMIT=/usr/bin/spark-submit; else SPARK_SUBMIT=spark-submit; fi && \
           echo 'Using spark-submit:' \$SPARK_SUBMIT && \
           \$SPARK_SUBMIT --master yarn --deploy-mode client /opt/synth-code/compute_features.py --manifest /opt/synth-code/metadata.csv --access_log /opt/synth-code/access.log --out /opt/synth-code/features_out"

# 7) copy results from namenode (or spark) to host output
info "7) Collecting results into ${OUT_DIR}"
mkdir -p "${OUT_DIR}"
# try to copy from namenode's mounted src dir (/opt/synth-code/features_out)
set +e
docker cp "${NAMENODE}:/opt/synth-code/features_out/." "${OUT_DIR}/" 2>/dev/null
rc=$?
set -e
if [ $rc -ne 0 ]; then
  # fallback: try copying from spark container (may have written into mounted src)
  echo "Fallback: copying from local src/features_out if present"
  if [ -d "${SRC_DIR}/features_out" ]; then
    cp -r "${SRC_DIR}/features_out/." "${OUT_DIR}/"
  fi
fi

info "Pipeline complete. Output directory: ${OUT_DIR}"
info "Inspect CSV(s) under ${OUT_DIR} (look for part-*.csv or _SUCCESS file)."
