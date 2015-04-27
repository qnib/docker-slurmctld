#!/bin/bash

curl -X PUT -d "$(date +%s)" http://consul.service.consul:8500/v1/kv/slurm/job/${SLURM_JOBID}/end
curl -X PUT -d "${SLURM_JOB_EXIT_CODE2}" http://consul.service.consul:8500/v1/kv/slurm/job/${SLURM_JOBID}/exit_code2
curl -X PUT -d "${SLURM_JOB_DERIVED_EC}" http://consul.service.consul:8500/v1/kv/slurm/job/${SLURM_JOBID}/derived_ec
