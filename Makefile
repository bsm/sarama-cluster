SCALA_VERSION?= 2.11
KAFKA_VERSION?= 0.9.0.0
KAFKA_DIR= kafka_$(SCALA_VERSION)-$(KAFKA_VERSION)
KAFKA_SRC= http://www.mirrorservice.org/sites/ftp.apache.org/kafka/$(KAFKA_VERSION)/$(KAFKA_DIR).tgz
KAFKA_ROOT= testdata/$(KAFKA_DIR)

default: vet errcheck test

vet:
	go vet ./...

errcheck:
	errcheck -ignoretests ./...

test: testdeps
	KAFKA_DIR=$(KAFKA_DIR) go test ./... -ginkgo.slowSpecThreshold=60

testrace: testdeps
	KAFKA_DIR=$(KAFKA_DIR) go test ./... -ginkgo.slowSpecThreshold=60 -v -race

testdeps: $(KAFKA_ROOT)

.PHONY: test testdeps vet errcheck

# ---------------------------------------------------------------------

$(KAFKA_ROOT):
	@mkdir -p $(dir $@)
	cd $(dir $@) && curl $(KAFKA_SRC) | tar xz
