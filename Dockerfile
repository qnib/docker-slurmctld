###### Slurmctld images
# A docker image that provides a slurmctld
FROM qnib/slurm
MAINTAINER "Christian Kniep <christian@qnib.org>"

RUN pip install graphitesend
ADD etc/supervisord.d/slurmctld.ini /etc/supervisord.d/
ADD etc/consul.d/check_slurmctld.json /etc/consul.d/
ADD opt/qnib/slurm/bin/ /opt/qnib/slurm/bin/

# Slurmstats
ADD etc/supervisord.d/slurmstats.ini /etc/supervisord.d/
ADD opt/qnib/slurm/bin/slurmstats.py /opt/qnib/slurm/bin/
# Scratch setup
ADD etc/supervisord.d/scratchsetup.ini /etc/supervisord.d/
ADD opt/qnib/slurm/bin/scratchsetup.sh /opt/qnib/slurm/bin/
