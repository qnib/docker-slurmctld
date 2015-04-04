FROM qnib/slurm:syslog
MAINTAINER "Christian Kniep <christian@qnib.org>"

ADD etc/supervisord.d/slurmctld.ini /etc/supervisord.d/
ADD etc/consul.d/check_slurmctld.json /etc/consul.d/
ADD etc/supervisord.d/slurmctld_update.ini /etc/supervisord.d/
