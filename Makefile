.DEFAULT_GOAL := deploy

AWSACCOUNTID = $(shell aws sts get-caller-identity --query Account --output text)
S3BUCKET = $(shell aws cloudformation describe-stack-resource --stack-name EDW-AttunityInfrastructure-Resources --logical-resource-id AttunityAssetsBucket | jq -r ".StackResourceDetail.PhysicalResourceId")
PWD = $(shell pwd)
PROJECT = $(shell basename -s .git `git config --get remote.origin.url`)


ifeq ($(AWSACCOUNTID),624334648735)
	AWS_ACCOUNT = playpen
endif
ifeq ($(AWSACCOUNTID),951049156144)
	AWS_ACCOUNT = nprod
endif
ifeq ($(AWSACCOUNTID),392068977215)
	AWS_ACCOUNT = prod
endif

STACK_NAME := $(PROJECT)-$(AWS_ACCOUNT)

deploy:
	$(info	Push Files to $(AWS_ACCOUNT) POC ENV)

local-build:
	$(info	Build Docker locally)
	docker build --rm --build-arg AIRFLOW_DEPS="aws" -t puckel/docker-airflow .
local:
	$(info	Run Docker locally)
	docker-compose -f docker-compose-LocalExecutor.yml up






