from django.conf import settings
from icommands import Session, GLOBAL_SESSION, SessionException
from django.utils.deconstruct import deconstructible

@deconstructible
class IrodsAccount():
    def __init__(self, option=None):
        self.session = GLOBAL_SESSION if getattr(settings, 'IRODS_GLOBAL_SESSION', False) else Session()
        if self.session != GLOBAL_SESSION:
            self.session.run('iinit', None, self.session.create_environment().auth)

    def create(self, uname):
        self.session.admin('mkuser', uname, "rodsuser")

    def setPassward(self, uname, upwd):
        self.session.admin('moduser', uname, "password", upwd)
