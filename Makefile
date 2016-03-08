include golang.mk
.DEFAULT_GOAL := test # override default goal set in library makefile

.PHONY: mocks $(PKG) test testenv build
SHELL := /bin/bash
PKG := github.com/Clever/mongo-op-throttler
PKGS := $(shell go list ./... | grep -v /vendor)
EXECUTABLE := $(shell basename $(PKG))
$(eval $(call golang-version-check,1.5))

export MONGO_URL ?= mongodb://localhost:27017/test

all: test build

build:
	go build -o bin/$(EXECUTABLE) $(PKG)

test: $(PKGS)
$(PKGS): golang-test-all-deps
	$(call golang-test-all,$@)

vendor: golang-godep-vendor-deps
	$(call golang-godep-vendor,$(PKGS))
