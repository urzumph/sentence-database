#!/bin/bash
LOGFILE=/var/log/rqworker.log
renice -n 20 $$
truncate -s 0 $LOGFILE
source bin/activate
python manage.py rqworker >> $LOGFILE 2>&1
deactivate
