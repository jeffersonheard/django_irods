from django.db import models as m
from django.contrib.auth.models import User

from icommands import Session
from django.conf import settings
import envoy
import os
import socket
from uuid import uuid4
# Create your models here.

class RodsEnvironment(m.Model):
    owner = m.ForeignKey(User)
    host = m.CharField(verbose_name='Hostname', max_length=255)
    port = m.IntegerField()
    def_res = m.CharField(verbose_name="Default resource", max_length=255)
    home_coll = m.CharField(verbose_name="Home collection", max_length=255)
    cwd = m.TextField(verbose_name="Working directory")
    username = m.CharField(max_length=255)
    zone = m.TextField()
    auth = m.TextField(verbose_name='Password')
