from celery.task import task
from tempfile import mkdtemp, tempdir
from django.conf import settings
import icommands as irods
from ga_irods import models
from django.core.exceptions import ObjectDoesNotExist

class IRODSException(Exception):
    pass

@task
def ibatch(environ, *icommands):
    """Do a batch of icommands.  Requires settings.ICOMMANDS_DIR to be set.

    :param environ: a ga_models.RodsEnvironment object
    :param icommands: a list of 2-tuples of (moniker|None, [command, arg1,... argnN])

    Returns a dictionary of the stdout,stderr tuples of all commands with monikers attached::

        >>> ibatch(my_env,
                (None, "iinit"),
                ("command1", ["iget" "foobar.txt"]),
                (None, "iexit"))

        ---
        { "command1" : ('this is a bit of text', '') }

    """
    if not hasattr(settings, "ICOMMANDS_DIR"):
        raise IRODSException("settings.ICOMMANDS_DIR must be set")

    session_root = mkdtemp(prefix='ga_irods')
    session = irods.RodsSession(tempdir, settings.ICOMMANDS_DIR, session_root)

    session.createEnvFiles(environ)

    response = {}
    for moniker, command in icommands:
        if moniker:
            response[moniker] = session.runCmd(*command)
        else:
            session.runCmd(*command)

    session.deleteEnvFiles()
    return response

