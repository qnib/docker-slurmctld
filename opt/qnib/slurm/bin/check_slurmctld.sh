#!/bin/bash

echo -n "Check if 'slurmctld' is running... "
supervisorctl status slurmctld|grep RUNNING
EC=$?
if [ ${EC} -ne 0 ];then
    echo "[FAIL]"
else
    echo "[OK]"
fi
exit ${EC}
