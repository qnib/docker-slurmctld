###### Slurmctld images
# A docker image that provides a slurmctld
FROM qnib/slurm
MAINTAINER "Christian Kniep <christian@qnib.org>"

RUN yum install -y freetype-devel libpng-devel && \
    pip install clustershell networkx matplotlib
ADD etc/supervisord.d/slurmctld.ini /etc/supervisord.d/
ADD etc/consul.d/check_slurmctld.json /etc/consul.d/
ADD opt/qnib/slurm/bin/ /opt/qnib/slurm/bin/

# Slurmstats
ADD etc/supervisord.d/slurmstats.ini /etc/supervisord.d/
ADD opt/qnib/slurm/bin/slurmstats.py /opt/qnib/slurm/bin/
# Scratch setup
ADD etc/supervisord.d/scratchsetup.ini /etc/supervisord.d/
ADD opt/qnib/bin/scratchsetup.sh /opt/qnib/bin/
