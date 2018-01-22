#!/bin/bash
sudo -u apache bash -c "source bin/activate; python manage.py rqworker --burst ; deactivate"
