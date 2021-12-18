# AWS Data Wrangler Lambda Layer build
# This makefile creats a layer from the pre-built github distribution
# The awswrangler lambda layer includes various pre-built binaries that are
# not available from a pip installation
MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := all
.DELETE_ON_ERROR:
.SUFFIXES:

# Lambda layer URL - update every so often
# AWS_WRANGLER_LAYER_URL := https://github.com/awslabs/aws-data-wrangler/releases/download/2.5.0/awswrangler-layer-2.5.0-py3.8.zip
AWS_WRANGLER_LAYER_URL := https://github.com/awslabs/aws-data-wrangler/releases/download/2.7.0/awswrangler-layer-2.7.0-py3.8.zip

# Logical resource name should match the resource name in the sam template
AWS_WRANGLER_RESOURCE_NAME := AwsDataWranglerLayer

all: build
.PHONY: all

SAM_BUILD_DIR := ../../.aws-sam/build
# use this directory for running the Makefile outside of a container build
TARGET_DIR := $(SAM_BUILD_DIR)/$(AWS_WRANGLER_RESOURCE_NAME)

# sam cli container builds sets ARTIFACTS_DIR - override on non container builds
ARTIFACTS_DIR ?= $(TARGET_DIR)

TARGET_ZIP := $(notdir $(AWS_WRANGLER_LAYER_URL))
TARGET := $(ARTIFACTS_DIR)/python/awswrangler/__init__.py
$(TARGET):
	mkdir -p '$(ARTIFACTS_DIR)'
	cd '$(ARTIFACTS_DIR)' && \
		curl -L -O '$(AWS_WRANGLER_LAYER_URL)' && \
		unzip '$(TARGET_ZIP)' && \
		rm '$(TARGET_ZIP)'

build-$(AWS_WRANGLER_RESOURCE_NAME): $(TARGET)
.PHONY: build-$(AWS_WRANGLER_RESOURCE_NAME)

build: build-$(AWS_WRANGLER_RESOURCE_NAME)
.PHONY: build