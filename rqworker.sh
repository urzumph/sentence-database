#!/bin/bash
LOGFILE=/var/log/rqworker.log
renice -n 20 $$
truncate -s 0 $LOGFILE
source bin/activate
export PYTHONIOENCODING=utf-8
python manage.py rqworker >> $LOGFILE 2>&1
deactivate
