SHELL := /bin/bash
PKG := github.com/Clever/mongo-op-throttler
SUBPKGS := $(addprefix $(PKG)/, apply convert operation)
PKGS := $(PKG) $(SUBPKGS)
GOLINT := $(GOPATH)/bin/golint
.PHONY: mocks $(PKG) test testenv build
GOVERSION := $(shell go version | grep 1.5)
ifeq "$(GOVERSION)" ""
  $(error must be running Go version 1.5)
endif

export MONGO_URL ?= mongodb://localhost:27017/test
GOVERSION := $(shell go version | grep 1.5)                                     
ifeq "$(GOVERSION)" ""                                                          
		$(error must be running Go version 1.5)                                     
endif 
export GO15VENDOREXPERIMENT=1

test: $(PKGS)

build:
	go build

$(GOLINT):
	go get github.com/golang/lint/golint

$(PKGS): $(GOLINT)
	gofmt -w=true $(GOPATH)/src/$@/*.go
	$(GOLINT) $(GOPATH)/src/$@/*.go
	go vet $(@)
	go test -v $@

