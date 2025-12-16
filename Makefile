# Makefile for running Hadoop docker cluster + pipeline
DC_DIR = docker
DC = docker-compose -f $(DC_DIR)/docker-compose.yml
COMPOSE_PROJECT_DIR := $(shell basename $(CURDIR))
HADOOP_CONF_HOST_DIR = $(DC_DIR)/hadoop_conf
SPARK_CONTAINER = spark
NAMENODE_CONTAINER = namenode

.PHONY: up down logs build spark-shell gen sim pipeline copy-conf clean output

up:
	 $(DC) up -d --build

down:
	 $(DC) down -v

logs:
	 $(DC) logs --tail 200 -f

build:
	 $(DC) build

# Copy Hadoop config files from the namenode container to host dir so spark container can use them.
copy-conf:
	@mkdir -p $(HADOOP_CONF_HOST_DIR)
	@echo "Copying Hadoop config files from $(NAMENODE_CONTAINER) to $(HADOOP_CONF_HOST_DIR)..."
	-docker exec $(NAMENODE_CONTAINER) bash -c "ls /opt/hadoop/etc/hadoop || true"
	-docker cp $(NAMENODE_CONTAINER):/opt/hadoop/etc/hadoop/core-site.xml $(HADOOP_CONF_HOST_DIR)/ || true
	-docker cp $(NAMENODE_CONTAINER):/opt/hadoop/etc/hadoop/hdfs-site.xml $(HADOOP_CONF_HOST_DIR)/ || true
	-docker cp $(NAMENODE_CONTAINER):/opt/hadoop/etc/hadoop/yarn-site.xml $(HADOOP_CONF_HOST_DIR)/ || true
	@echo "Done."

# Run generator inside namenode (needs hdfs cli inside that container)
gen:
	@echo "Running generator.py inside $(NAMENODE_CONTAINER)..."
	docker exec -i $(NAMENODE_CONTAINER) bash -c "cd /opt/synth-code && python3 generator.py --n 200 --hdfs_dir /user/root/synth --out_manifest /opt/synth-code/metadata.csv"

# Run simulator inside namenode
sim:
	@echo "Running access_simulator.py inside $(NAMENODE_CONTAINER)..."
	docker exec -i $(NAMENODE_CONTAINER) bash -c "cd /opt/synth-code && python3 access_simulator.py --manifest /opt/synth-code/metadata.csv --out /opt/synth-code/access.log --duration_seconds 600 --clients dn1,dn2,dn3"

# Run spark-submit from the spark container.
# This target uses docker-compose run so spark container uses the same network and mounted volumes.
spark:
	@echo "Running spark-submit inside spark container (apache/spark image)"
	docker-compose -f $(DC_DIR)/docker-compose.yml run --rm \
	  -v $(CURDIR)/$(DC_DIR)/hadoop_conf:/opt/hadoop-conf \
	  -v $(CURDIR)/src:/opt/synth-code \
	  --entrypoint "" $(SPARK_CONTAINER) \
	  bash -c '\
	    export HADOOP_CONF_DIR=/opt/hadoop-conf; \
	    /opt/spark/bin/spark-submit \
	      --master yarn \
	      --deploy-mode client \
	      /opt/synth-code/compute_features.py \
	      --manifest /opt/synth-code/metadata.csv \
	      --access_log /opt/synth-code/access.log \
	      --out /opt/synth-code/features_out \
	  '

# Full pipeline: up -> gen -> sim -> spark -> collect outputs
pipeline: up wait-gen sim spark output

# Wait for services to become healthy (simple fixed sleep)
wait-gen:
	@echo "Waiting 20s for services to start..."
	@sleep 20

output:
	@echo "Collecting outputs to ./output"
	@mkdir -p output
	# copy features_out (coalesced CSV will be in a part-*.csv inside /opt/synth-code/features_out)
	-docker exec $(NAMENODE_CONTAINER) bash -c "ls -la /opt/synth-code/features_out || true"
	-docker cp $(NAMENODE_CONTAINER):/opt/synth-code/features_out ./output/ || true
	@echo "Outputs copied to ./output (inspect CSVs there)"

clean:
	@echo "Removing local hadoop_conf and output"
	-rm -rf $(HADOOP_CONF_HOST_DIR) output

