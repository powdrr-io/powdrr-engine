
.PHONY:service
service:
	docker build -t powdrr/service:latest --build-arg TARGET=powdrr-io-service --build-arg PORT=7784 .

.PHONY:engine
engine:
	docker build -t powdrr/engine:latest --build-arg TARGET=powdrr-io-engine --build-arg PORT=9200 .
