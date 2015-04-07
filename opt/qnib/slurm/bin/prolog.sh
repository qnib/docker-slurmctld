#!/bin/bash

#SLURM_JOB_NAME=PingPong
#SLURM_JOB_GROUP=cluser
#SLURM_JOBID=5
#SLURM_JOB_EXIT_CODE2=0:0
#SLURM_JOB_DERIVED_EC=0
#SLURM_JOB_ID=5
#PWD=/
#SLURM_JOB_USER=cluser
#SLURM_JOB_EXIT_CODE=0
#SLURM_JOB_UID=3000
#SLURM_JOB_NODELIST=4ecc63b2e376,b4653fe23782
#SHLVL=1
#SLURM_JOB_GID=3000
#SLURM_CLUSTER_NAME=qnib
#SLURM_JOB_PARTITION=qnib
#SLURM_JOB_CONSTRAINTS=(null)
#SLURM_JOB_ACCOUNT=(null)
#_=/usr/bin/env

curl -X PUT -d "${SLURM_JOB_USER}" http://consul.service.consul:8500/v1/kv/slurm/job/${SLURM_JOBID}/user
curl -X PUT -d "$(date +%s)" http://consul.service.consul:8500/v1/kv/slurm/job/${SLURM_JOBID}/start
curl -X PUT -d "${SLURM_JOB_NODELIST}" http://consul.service.consul:8500/v1/kv/slurm/job/${SLURM_JOBID}/nodelist

