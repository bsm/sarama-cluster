PKG=$(shell go list ./... | grep -v vendor)
KAFKA_VERSION=1.0.1

default: vet test

vet:
	go vet $(PKG)

test:
	go test $(PKG) -ginkgo.slowSpecThreshold=60

test-verbose:
	go test $(PKG) -ginkgo.slowSpecThreshold=60 -v

test-race:
	go test $(PKG) -ginkgo.slowSpecThreshold=60 -v -race

.PHONY: vet test test-race test-verbose

scenario.up:
	docker-compose -f testdata/docker-compose-${KAFKA_VERSION}.yml up

scenario.rm:
	docker-compose -f testdata/docker-compose-${KAFKA_VERSION}.yml rm -f

.PHONY: scenario.up scenario.rm

doc: README.md

.PHONY: doc

# ---------------------------------------------------------------------

README.md: README.md.tpl $(wildcard *.go)
	becca -package $(subst $(GOPATH)/src/,,$(PWD))
