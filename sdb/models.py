# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models

# Create your models here.
class Searches(models.Model):
    keyword = models.CharField(max_length=200)