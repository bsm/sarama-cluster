SCALA_VERSION= 2.10
KAFKA_VERSION= 0.8.1.1
KAFKA_DIR= kafka_$(SCALA_VERSION)-$(KAFKA_VERSION)
KAFKA_SRC= http://www.mirrorservice.org/sites/ftp.apache.org/kafka/$(KAFKA_VERSION)/$(KAFKA_DIR).tgz
KAFKA_ROOT= test/$(KAFKA_DIR)

default: test

test: deps
	go test

fuzztest: deps
	FUZZ=1 go test

.PHONY: test

deps: $(KAFKA_ROOT)
	go get -t ./...

$(KAFKA_ROOT):
	@mkdir -p $(dir $@)
	cd $(dir $@) && curl $(KAFKA_SRC) | tar xz
