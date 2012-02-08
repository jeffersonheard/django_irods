from django.db import models as m
from django.contrib.auth.models import User

# Create your models here.

class RodsEnvironment(m.Model):
    owner = m.ForeignKey(User)
    host = m.CharField(verbose_name='Hostname', max_length=255)
    port = m.IntegerField()
    def_res = m.CharField(verbose_name="Default resource")
    home_coll = m.CharField(verbose_name="Home collection")
    cwd = m.TextField(verbose_name="Working directory")
    username = m.CharField(max_length=255)
    zone = m.TextField()
    auth = m.TextField(verbose_name='Password')
