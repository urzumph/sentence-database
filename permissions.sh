BASE=/var/www/sdb
ADMIN=ec2-user
WEBGRP=apache

# Base directory
chown $ADMIN:$WEBGRP $BASE
chmod 775 $BASE

# Sentence file database
mkdir -p $BASE/data
chmod 755 $BASE/data
chown $ADMIN:$WEBGRP $BASE/data/*
chmod 640 $BASE/data/*

# SQLite DB
chown $ADMIN:$WEBGRP $BASE/db.sqlite3
chmod 660 $BASE/db.sqlite3

# Picklefile directory
mkdir -p $BASE/pickles
chown $ADMIN:$WEBGRP $BASE/pickles
chmod 770 $BASE/pickles
