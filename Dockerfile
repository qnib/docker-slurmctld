###### Slurmctld images
# A docker image that provides a slurmctld
FROM qnib/slurm
MAINTAINER "Christian Kniep <christian@qnib.org>"

ADD etc/supervisord.d/slurmctld.ini /etc/supervisord.d/
ADD etc/consul.d/check_slurmctld.json /etc/consul.d/
