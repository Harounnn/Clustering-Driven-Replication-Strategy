.PHONY: up down logs exec-nn exec-dn fs-ls fs-put

COMPOSE_DIR = docker
COMPOSE = docker-compose -f $(COMPOSE_DIR)/docker-compose.yml

up:
	cd $(COMPOSE_DIR) && $(COMPOSE) up -d

down:
	cd $(COMPOSE_DIR) && $(COMPOSE) down -v

logs:
	cd $(COMPOSE_DIR) && $(COMPOSE) logs --tail=200 -f

exec-nn:
	docker exec -it namenode bash

exec-dn:
	docker exec -it datanode bash

fs-ls:
	docker exec -it namenode bash -c "hdfs dfs -ls -R /"

fs-put:
	docker exec -i namenode bash -c "hdfs dfs -mkdir -p $(dir $(hdfs)) || true; hdfs dfs -put -f - $(hdfs)" < $(local)
