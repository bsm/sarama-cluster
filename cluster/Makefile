SCALA_VERSION= 2.10
KAFKA_VERSION= 0.8.1
KAFKA_DIR= kafka_$(SCALA_VERSION)-$(KAFKA_VERSION)
KAFKA_SRC= http://www.mirrorservice.org/sites/ftp.apache.org/kafka/$(KAFKA_VERSION)/$(KAFKA_DIR).tgz
KAFKA_ROOT= test/$(KAFKA_DIR)
JENV_VARS= KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:test/log4j.properties" KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"

default: test

test: deps kill-kafka kill-zookeeper start-zookeeper start-kafka
	@go test
	@make kill-kafka
	@make kill-zookeeper

.PHONY: test

deps:
	go get

start-zookeeper: $(KAFKA_ROOT) /tmp/sarama-cluster-test
	$(JENV_VARS) $(KAFKA_ROOT)/bin/kafka-run-class.sh -name zookeeper -loggc org.apache.zookeeper.server.ZooKeeperServerMain test/zookeeper.properties & echo "$$!" > "test/zookeeper.pid"

kill-zookeeper:
	if [ -f test/zookeeper.pid ]; then kill -9 `cat test/zookeeper.pid`; rm -f test/zookeeper.pid; fi
	@rm -rf /tmp/sarama-cluster-test/zookeeper

start-kafka: $(KAFKA_ROOT) /tmp/sarama-cluster-test
	$(JENV_VARS) $(KAFKA_ROOT)/bin/kafka-run-class.sh -name kafkaServer -loggc kafka.Kafka test/server.properties & echo "$$!" > "test/kafka.pid"

kill-kafka:
	if [ -f test/kafka.pid ]; then kill -9 `cat test/kafka.pid`; rm -f test/kafka.pid; fi
	@rm -rf /tmp/sarama-cluster-test/kafka

/tmp/sarama-cluster-test:
	@mkdir -p $@

$(KAFKA_ROOT):
	@mkdir -p $(dir $@)
	cd $(dir $@) && curl $(KAFKA_SRC) | tar xz
