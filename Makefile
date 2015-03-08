SCALA_VERSION?= 2.11
KAFKA_VERSION?= 0.8.2.0
KAFKA_DIR= kafka_$(SCALA_VERSION)-$(KAFKA_VERSION)
KAFKA_SRC= http://www.mirrorservice.org/sites/ftp.apache.org/kafka/$(KAFKA_VERSION)/$(KAFKA_DIR).tgz
KAFKA_ROOT= _test/$(KAFKA_DIR)

default: test

test: testdeps
	go test ./... -ginkgo.slowSpecThreshold=20

testfull: testdeps
	go test ./... -ginkgo.slowSpecThreshold=20
	go test ./... -ginkgo.slowSpecThreshold=20 -cpu=2
	go test ./... -ginkgo.slowSpecThreshold=20 -short -race

testdeps: $(KAFKA_ROOT)

.PHONY: test testfull testdeps

# ---------------------------------------------------------------------

$(KAFKA_ROOT):
	@mkdir -p $(dir $@)
	cd $(dir $@) && curl $(KAFKA_SRC) | tar xz

start_zookeeper:
	$(KAFKA_ROOT)/bin/kafka-run-class.sh -name zookeeper org.apache.zookeeper.server.ZooKeeperServerMain _test/zookeeper.properties

start_kafka:
	KAFKA_HEAP_OPTS='-Xmx1G -Xms1G' $(KAFKA_ROOT)/bin/kafka-run-class.sh -name kafkaServer kafka.Kafka _test/server.properties
