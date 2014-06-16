from django.conf import settings
from django.core.files.storage import Storage
from icommands import Session


class IrodsStorage(Storage):
    def __init__(self, option=None):
        self.session = getattr(settings, "IRODS_GLOBAL_SESSION", Session())
        if not hasattr(settings, 'IRODS_GLOBAL_SESSION'):
            self.session.run('iinit')

    def _open(self, name, mode='rb'):
        self.session.run("iget", [name, "tempfile." + name])
        return open(name)

    def _save(self, name, content):
        self.session.run("iput", [content.name, name])
        return name

    def delete(self, name):
        self.session.run("irm", ["-f", name])

    def exists(self, name):
        stdout = self.session.run("ils", [name])[0]
        return stdout != ""

    def listdir(self, path):
        stdout = self.session.run("ils", [path])[0].split("\n")
        listing = ( [], [] )
        directory = stdout[0][0:-2]
        directory_prefix = "  C- " + directory + "/"
        for i in range(1, len(stdout)):
            if stdout[i][:len(directory_prefix)] == directory_prefix:
                listing[0].append(stdout[i][len(directory_prefix):])
            else:
                listing[1].append(stdout[i].strip)
        return listing

    def size(self, name):
        stdout = self.session.run("ils", ["-l", name])[0].split()
        return int(stdout[1])

    def url(self, name):
        return "/django_irods/download/{name}/".format(name=name)

