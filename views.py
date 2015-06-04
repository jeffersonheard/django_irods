# Create your views here.
from . import models as m
from .icommands import Session, GLOBAL_SESSION
from django_irods import icommands

from uuid import uuid4
import os

from django.conf import settings
from django.http import HttpResponse, FileResponse
from rest_framework.decorators import api_view

from hs_core.views.utils import authorize
from hs_core.hydroshare.hs_bagit import create_bag_by_irods

@api_view(['GET'])
def download(request, path, *args, **kwargs):
    
    split_path_strs = path.split('/')
    if split_path_strs[0] == 'bags':
        res_id = os.path.splitext(split_path_strs[1])[0]
    else:
        res_id = split_path_strs[0]
    _, authorized, _ = authorize(request, res_id, edit=True, full=True, view=True, superuser=True, raises_exception=False)
    if not authorized:
        response = HttpResponse()
        response.content = "<h1>You do not have permission to download this resource!</h1>"
        return response

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

    # do on-demand bag creation
    bag_modified = False
    if request:
        bag_modified = request.session.get('bag_modified', False)
        if 'bag_modified' in request.session:
            del request.session['bag_modified']
    if bag_modified:
        create_bag_by_irods(res_id)
    options = ('-',) # we're redirecting to stdout.
    proc = session.run_safe('iget', None, path, *options)
    response = FileResponse(proc.stdout, content_type='application-x/octet-stream')
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
