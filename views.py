# Create your views here.
from . import models as m
from .icommands import Session, GLOBAL_SESSION
from uuid import uuid4
from django.conf import settings
from django.http import HttpResponse
from django_irods import icommands

def download(request, *args, **kwargs):
    if 'environment' in kwargs:
        environment = int(kwargs['environment'])
        environment = m.RodsEnvironment.objects.get(pk=environment)
        session = Session("/tmp/django_irods", settings.IRODS_ICOMMANDS_PATH, session_id=uuid4())
        session.create_environment(environment)
        session.run('iinit', None, environment.auth)
    elif getattr(settings, 'IRODS_GLOBAL_SESSION', False):
        session = GLOBAL_SESSION
    elif icommands.ACTIVE_SESSION:
        session = icommands.ACTIVE_SESSION
    else:
        raise KeyError('settings must have IRODS_GLOBAL_SESSION set if there is no environment object')

    options = ('-',) # we're redirecting to stdout.
    path = request.GET['path']

    proc = session.run_safe('iget', None, path, *options)
    response = HttpResponse(proc.stdout.read(), content_type='application-x/octet-stream')
    response['Content-Disposition'] = 'attachment; filename="{name}"'.format(name=path.split('/')[-1])
    return response


def list(request, *args, **kwargs):
    if 'environment' in kwargs:
        environment = int(kwargs['environment'])
        environment = m.RodsEnvironment.objects.get(pk=environment)
        session = Session("/tmp/django_irods", settings.IRODS_ICOMMANDS_PATH, session_id=uuid4())
        session.create_environment(environment)
        session.run('iinit', None, environment.auth)
    elif getattr(settings, 'IRODS_GLOBAL_SESSION', False):
        session = GLOBAL_SESSION
    else:
        raise KeyError('settings must have IRODS_GLOBAL_SESSION set if there is no environment object')

    options = ('-',) # we're redirecting to stdout.

    proc = session.run_safe('ils', None, *options)
    response = HttpResponse(proc.stdout)
    return response
