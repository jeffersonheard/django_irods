"""
The following is a list of the IRODS celery tasks and a brief description of
what each does:

iadmin   - perform irods administrator operations (irods admins only).
ibun     - upload/download structured (tar) files.
ichksum  - checksum one or more data-objects or collections.
ichmod   - change access permissions to collections or data-objects.
icp      - copy a data-object (file) or collection (directory) to another.
iexecmd  - remotely execute special commands.
ifsck    - check if local files/directories are consistent with the associated objects/collections in iRODS.
iget     - get a file from iRODS.
ilocate  - search for data-object(s) OR collections (via a script).
ils      - list collections (directories) and data-objects (files).
ilsresc  - list iRODS resources and resource-groups.
imcoll   - manage mounted collections and associated cache.
imeta    - add/remove/copy/list/query user-defined metadata.
imiscsvrinfo - retrieve basic server information.
imkdir   - make an irods directory (collection).
imv      - move/rename an irods data-object (file) or collection (directory).
ipasswd  - change your irods password.
iphybun  - physically bundle files (admin only).
iphymv   - physically move a data-object to another storage resource.
ips      - display iRODS agent (server) connection information.
iput     - put (store) a file into iRODS.
iqdel    - remove a delayed rule (owned by you) from the queue.
iqmod    - modify certain values in existing delayed rules (owned by you).
iqstat   - show the queue status of delayed rules.
iquest   - issue a question (query on system/user-defined metadata).
iquota   - show information on iRODS quotas (if any).
ireg     - register a file or directory/files/subdirectories into iRODS.
irepl    - replicate a file in iRODS to another storage resource.
irm      - remove one or more data-objects or collections.
irmtrash - remove data-objects from the trash bin.
irsync   - synchronize collections between a local/irods or irods/irods.
irule    - submit a rule to be executed by the iRODS server.
iscan    - check if local file or directory is registered in irods.
isysmeta - show or modify system metadata.
itrim    - trim down the number of replicas of data-objects.
iuserinfo- show information about your iRODS user account.
ixmsg    - send/receive iRODS xMessage System messages.
"""

from celery import registry
from celery.task import Task, task
from celery.task.sets import subtask
from icommands import Session
from ga_irods import models as m
from uuid import uuid4
import envoy
import os
import tempfile
import requests
from django.conf import settings

@task(ignore_result=True)
def test_callback(data):
    print data


class IRODSTask(Task):

    @property
    def session(self):
        if not hasattr(self, '_session'):
            self._session = Session("/tmp/ga_irods", settings.ICOMMANDS_DIR, session_id=uuid4())
            self._session.create_environment(self._environment)
            self._session.run('iinit', None, self._environment.auth)
        return self._session

    def mount(self, local_name, collection=None):
        if not hasattr(self, '_mounted_collections'):
            self._mounted_collections = {}

        if local_name not in self._mounted_collections:
            if collection:
                self._session.run('icd', collection)
            self._session.run('irodsFs', None, self.collection(local_name))
            self._mounted_collections[local_name] = collection
        elif collection != self._mounted_collections[local_name]:
            raise IRODSException("Trying to remount a directory with a different collection")

        return self.collection(local_name)


    def collection(self, name):
        return os.path.join(self.session.root, name)

    def unmount(self, local_name):
        if not hasattr(self, '_mounted_collections'):
            return None

        if local_name in self._mounted_collections:
            envoy.run("fusermount -uz {local_name}".format(local_name=self.collection(local_name)))

    def run(self, environment):
        self._environment = m.RodsEnvironment.objects.get(pk=environment)

    def __del__(self):
        if hasattr(self, '_mounted_collections'):
            for name in self._mounted_collections:
                self.unmount(name)
        if hasattr(self, '_session'):
            self._session.run('iexit')
            self._session.delete_environment()

CHUNK_SIZE=8192

#@task
class IGet(IRODSTask):
    def run(self, environment, path, callback=None, post=None, post_name=None, *options):
        """
        Usage: iget [-fIKPQrUvVT] [-n replNumber] [-N numThreads] [-X restartFile]
        [-R resource] srcDataObj|srcCollection ... destLocalFile|destLocalDir

        Usage : iget [-fIKPQUvVT] [-n replNumber] [-N numThreads] [-X restartFile]
        [-R resource] srcDataObj|srcCollection

        Usage : iget [-fIKPQUvVT] [-n replNumber] [-N numThreads] [-X restartFile]
        [-R resource] srcDataObj ... -

        Get data-objects or collections from irods space, either to the specified
        local area or to the current working directory.

        If the destLocalFile is '-', the files read from the server will be
        written to the standard output (stdout). Similar to the UNIX 'cat'
        command, multiple source files can be specified.

        The -X option specifies that the restart option is on and the restartFile
        input specifies a local file that contains the restart info. If the
        restartFile does not exist, it will be created and used for recording
        subsequent restart info. If it exists and is not empty, the restart info
        contained in this file will be used for restarting the operation.
        Note that the restart operation only works for uploading directories and
        the path input must be identical to the one that generated the restart file

        The -Q option specifies the use of the RBUDP transfer mechanism which uses
        the UDP protocol for data transfer. The UDP protocol is very efficient
        if the network is very robust with few packet losses. Two environment
        variables - rbudpSendRate and rbudpPackSize are used to tune the RBUDP
        data transfer. rbudpSendRate is used to throttle the send rate in
        kbits/sec. The default rbudpSendRate is 600,000. rbudpPackSize is used
        to set the packet size. The dafault rbudpPackSize is 8192. The -V option
        can be used to show the loss rate of the transfer. If the lost rate is
        more than a few %, the sendrate should be reduced.

        The -T option will renew the socket connection between the client and
        server after 10 minutes of connection. This gets around the problem of
        sockets getting timed out by the firewall as reported by some users.

        Options are:
        * -f  force - write local files even it they exist already (overwrite them)
        * -I  redirect connection - redirect the connection to connect directly
               to the best (determiined by the first 10 data objects in the input
               collection) resource server.
        * -K  verify the checksum
        * -n  replNumber - retrieve the copy with the specified replica number
        * -N  numThreads - the number of thread to use for the transfer. A value of
               0 means no threading. By default (-N option not used) the server
               decides the number of threads to use.
        * -P  output the progress of the download.
        * -r  recursive - retrieve subcollections
        * -R  resource - the preferred resource
        * -T  renew socket connection after 10 minutes
        * -Q  use RBUDP (datagram) protocol for the data transfer
        * -v  verbose
        * -V  Very verbose
             restartFile input specifies a local file that contains the restart info.
        * -X  restartFile - specifies that the restart option is on and the
             restartFile input specifies a local file that contains the restart info.
        * -h  this help

        :param environment: a dict or primary key of the RodsEnvironment model that governs this session
        :param path: the path to get from
        :param callback: a registered Celery task that can be called as a subtask with the entire contents of the file that was gotten (file must fit in memory)
        :param post: a URL to which the results of the iget can be POSTed.  File can be larger than available memory.
        :param post_name: the filename that the POST will be given.
        :param options: any of the above command line options.
        :return:
        """

        super(IGet, self).run(environment)

        options += ('-',) # we're redirecting to stdout.

        proc = self.session.run_safe('iget', None, path, *options)
        tmp = tempfile.SpooledTemporaryFile()   # spool to disk if the iget is too large
        chunk = proc.stdout.read(CHUNK_SIZE)
        while chunk:
            tmp.write(chunk)
            chunk = proc.stdout.read(CHUNK_SIZE)

        tmp.flush()
        tmp.seek(0)

        if callback:
            data = tmp.read()
            subtask(callback).delay(data)
            return None
        elif post:
            rsp =  requests.post(post, files={post_name: tmp})
            return {
                'code' : rsp.status_code,
                'content' : rsp.content
            }
        else:
            return tmp.read()

iget = registry.tasks[IGet.name]

#@task
class IPut(IRODSTask):
    def run(self, environment, path, data, *options):
        """
        Usage : iput [-abfIkKPQrTUvV] [-D dataType] [-N numThreads] [-n replNum]
                 [-p physicalPath] [-R resource] [-X restartFile] [--link]
            localSrcFile|localSrcDir ...  destDataObj|destColl
        Usage : iput [-abfIkKPQTUvV] [-D dataType] [-N numThreads] [-n replNum]
                     [-p physicalPath] [-R resource] [-X restartFile] [--link]
                       localSrcFile

        Store a file into iRODS.  If the destination data-object or collection are
        not provided, the current irods directory and the input file name are used.
        The -X option specifies that the restart option is on and the restartFile
        input specifies a local file that contains the restart info. If the
        restartFile does not exist, it will be created and used for recording
        subsequent restart info. If it exists and is not empty, the restart info
        contained in this file will be used for restarting the operation.
        Note that the restart operation only works for uploading directories and
        the path input must be identical to the one that generated the restart file

        If the options -f is used to overwrite an existing data-object, the copy
        in the resource specified by the -R option will be picked if it exists.
        Otherwise, one of the copy in the other resources will be picked for the
        overwrite. Note that a copy will not be made in the specified resource
        if a copy in the specified resource does not already exist. The irepl
        command should be used to make a replica of an existing copy.

        The -I option specifies the redirection of the connection so that it can
        be connected directly to the resource server. This option can improve
        the performance of uploading a large number of small (<32 Mbytes) files.
        This option is only effective if the source is a directory and the -f
        option is not used

        The -Q option specifies the use of the RBUDP transfer mechanism which uses
        the UDP protocol for data transfer. The UDP protocol is very efficient
        if the network is very robust with few packet losses. Two environment
        variables - rbudpSendRate and rbudpPackSize are used to tune the RBUDP
        data transfer. rbudpSendRate is used to throttle the send rate in
        kbits/sec. The default rbudpSendRate is 600,000. rbudpPackSize is used
        to set the packet size. The dafault rbudpPackSize is 8192. The -V option
        can be used to show the loss rate of the transfer. If the lost rate is
        more than a few %, the sendrate should be reduced.

        The -T option will renew the socket connection between the client and
        server after 10 minutes of connection. This gets around the problem of
        sockets getting timed out by the firewall as reported by some users.

        The -b option specifies bulk upload operation which can do up to 50 uploads
        at a time to reduce overhead. If the -b is specified with the -f option
        to overwrite existing files, the operation will work only if there is no
        existing copy at all or if there is an existing copy in the target resource.
        The operation will fail if there are existing copies but not in the
        target resource because this type of operation requires a replication
        operation and bulk replication has not been implemented yet.
        The bulk option does work for mounted collections which may represent the
        quickest way to upload a large number of small files.

        Options are:
        * -a  all - update all existing copy
        * -b  bulk upload to reduce overhead
        * -D  dataType - the data type string
        * -f  force - write data-object even it exists already; overwrite it
        * -I  redirect connection - redirect the connection to connect directly
               to the resource server.
        * -k  checksum - calculate a checksum on the data
        * -K  verify checksum - calculate and verify the checksum on the data
        * --link - ignore symlink.
        * -N  numThreads - the number of thread to use for the transfer. A value of
               0 means no threading. By default (-N option not used) the server
               decides the number of threads to use.
        * -p physicalPath - the physical path of the uploaded file on the sever
        * -P  output the progress of the upload.
        * -Q  use RBUDP (datagram) protocol for the data transfer
        * -R  resource - specifies the resource to store to. This can also be specified
             in your environment or via a rule set up by the administrator.
        * -r  recursive - store the whole subdirectory
        * -T  renew socket connection after 10 minutes
        * -v  verbose
        * -V  Very verbose
        * -X  restartFile - specifies that the restart option is on and the
             restartFile input specifies a local file that contains the restart info.
        * -h  this help

        :param environment: a dict or primary key of the RodsEnvironment model that governs this session
        :param path: the path to store the object in
        :param data: the data object to store
        :param options: any of the above command line options.
        :return: stdout, stderr of the command.
        """
        super(IPut, self).run(environment)

        tmp = tempfile.NamedTemporaryFile('w+b')
        tmp.write(data)
        tmp.flush()
        tmp.seek(0)

        options += (tmp.name, path)

        return self.session.run('iput', None, *options)
iput = registry.tasks[IPut.name]

#@task
class ILs(IRODSTask):
    def run(self, environment, *options):
        """
        Display data Objects and collections stored in irods. Options are:
            * -A  ACL (access control list) and inheritance format
            * -l  long format
            * -L  very long format
            * -r  recursive - show subcollections
            * -v  verbose
            * -V  Very verbose
            * -h  this help

        :param environment: a dict or primary key of the RodsEnvironment model that governs this session
        :param options: any of the above command line options
        :return: stdout, stderr tuple of the command.
        """
        super(ILs, self).run(environment)
        return self.session.run('ils', None, *options)
ils = registry.tasks[ILs.name]

#@task
class IAdmin(IRODSTask):
    name = 'iadmin'
    def iadmin(self, environment, command, *options):
        """
        Usage: iadmin [-hvV] [command]

        A blank execute line invokes the interactive mode, where it
        prompts and executes commands until 'quit' or 'q' is entered.
        Single or double quotes can be used to enter items with blanks.

        Commands are:
        * lu [name[#Zone]] (list user info; details if name entered)
        * lua [name[#Zone]] (list user authentication (GSI/Kerberos Names, if any))
        * luan Name (list users associated with auth name (GSI/Kerberos)
        * lt [name] [subname] (list token info)
        * lr [name] (list resource info)
        * ls [name] (list directory: subdirs and files)
        * lz [name] (list zone info)
        * lg [name] (list group info (user member list))
        * lgd name  (list group details)
        * lrg [name] (list resource group info)
        * lf DataId (list file details; DataId is the number (from ls))
        * mkuser Name[#Zone] Type (make user)
        * moduser Name[#Zone] [ type | zone | comment | info | password ] newValue
        * aua Name[#Zone] Auth-Name (add user authentication-name (GSI/Kerberos)
        * rua Name[#Zone] Auth-Name (remove user authentication name (GSI/Kerberos)
        * rmuser Name[#Zone] (remove user, where userName: name[@department][#zone])
        * mkdir Name [username] (make directory(collection))
        * rmdir Name (remove directory)
        * mkresc Name Type Class Host [Path] (make Resource)
        * modresc Name [name, type, class, host, path, status, comment, info, freespace] Value (mod Resc)
        * rmresc Name (remove resource)
        * mkzone Name Type(remote) [Connection-info] [Comment] (make zone)
        * modzone Name [ name | conn | comment ] newValue  (modify zone)
        * rmzone Name (remove zone)
        * mkgroup Name (make group)
        * rmgroup Name (remove group)
        * atg groupName userName[#Zone] (add to group - add a user to a group)
        * rfg groupName userName[#Zone] (remove from group - remove a user from a group)
        * atrg resourceGroupName resourceName (add (resource) to resource group)
        * rfrg resourceGroupName resourceName (remove (resource) from resource group)
        * at tokenNamespace Name [Value1] [Value2] [Value3] (add token)
        * rt tokenNamespace Name [Value1] (remove token)
        * spass Password Key (print a scrambled form of a password for DB)
        * dspass Password Key (descramble a password and print it)
        * pv [date-time] [repeat-time(minutes)] (initiate a periodic rule to vacuum the DB)
        * ctime Time (convert an iRODS time (integer) to local time; & other forms)
        * suq User ResourceName-or-'total' Value (set user quota)
        * sgq Group ResourceName-or-'total' Value (set group quota)
        * lq [Name] List Quotas
        * cu (calulate usage (for quotas))
        * rum (remove unused metadata (user-defined AVUs)
        * asq 'SQL query' [Alias] (add specific query)
        * rsq 'SQL query' or Alias (remove specific query)
        * help (or h) [command] (this help, or more details on a command)
        Also see 'irmtrash -M -u user' for the admin mode of removing trash and
        similar admin modes in irepl, iphymv, and itrim.
        The admin can also alias as any user via the 'clientUserName' environment
        variable.

        :param environment:
        :param options:
        :return:
        """
        super(IAdmin, self).run(environment)
        return self.session.run('iadmin', None, command, *options)
iadmin = registry.tasks[IAdmin.name]


class IBundle(IRODSTask):
    def run(self, environment, command, *options):
       """
       Usage : ibun -x [-hb] [-R resource] structFilePath
                   irodsCollection

       Usage : ibun -c [-hf] [-R resource] [-D dataType] structFilePath
                   irodsCollection

       Bundle file operations. This command allows structured files such as
       tar files to be uploaded and downloaded to/from iRODS.

       A tar file containing many small files can be created with normal unix
       tar command on the client and then uploaded to the iRODS server as a
       normal iRODS file. The 'ibun -x' command can then be used to extract/untar
       the uploaded tar file. The extracted subfiles and subdirectories will
       appeared as normal iRODS files and sub-collections. The 'ibun -c' command
       can be used to tar/bundle an iRODS collection into a tar file.

       For example, to upload a directory mydir to iRODS::

           tar -chlf mydir.tar -C /x/y/z/mydir .
           iput -Dtar mydir.tar .
           ibun -x mydir.tar mydir

       Note the use of -C option with the tar command which will tar the
       content of mydir but without including the directory mydir in the paths.
       The 'ibun -x' command extracts the tar file into the mydir collection.
       The target mydir collection does not have to exist nor be empty.
       If a subfile already exists in the target collection, the ingestion
       of this subfile will fail (unless the -f flag is set) but the process
       will continue.

       It is generally a good practice to tag the tar file using the -Dtar flag
       when uploading the file using iput. But if the tag is not made,
       the server assumes it is a tar dataType. The dataType tag can be added
       afterward with the isysmeta command. For example:
       isysmeta mod /tempZone/home/rods/mydir.tar datatype 'tar file'

       The following command bundles the iRods collection mydir into a tar file::

            ibun -cDtar mydir1.tar mydir

       If a copy of a file to be bundled does not exist on the target resource,
       a replica will automatically be made on the target resource.
       Again, if the -D flag is not use, the bundling will be done using tar.

       The -b option when used with the -x option, specifies bulk registration
       which does up to 50 rgistrations at a time to reduce overhead.

       Options are:
       * -b  bulk registration when used with -x to reduce overhead
       * -R  resource - specifies the resource to store to. This is optional
         in your environment
       * -D  dataType - the struct file data type. Valid only if the struct file
         does not exist. Currently only one dataType - 't' which specifies
         a tar file type is supported. If -D is not specified, the default is
         a tar file type
       * -x  extract the structFile and register the extracted files and directories
         under the input irodsCollection
       * -c  bundle the files and sub-collection underneath the input irodsCollection
         and store it in the structFilePath
       * -f  force overwrite the struct file (-c) or the subfiles (-x).
       * -h  this help

       :param environment:
       :param options:
       :return:
       """
       super(IBundle, self).run(environment)
       return self.session.run('iadmin', None, command, *options)
ibun = registry.tasks[IBundle.name]


class IChksum(IRODSTask):
    
    def run(self, environment, *options):
        super(IChksum, self).run(environment)
        return self.session.run('ichksum', None, *options)
ichksum = registry.tasks[IChksum.name]


class Ichmod(IRODSTask):

    def run(self, environment, *options):
        super(Ichmod, self).run(environment)
        return self.session.run('ichmod', None, *options)
ichmod = registry.tasks[Ichmod.name]



class Icp(IRODSTask):
     def run(self, environment, *options):
        super(Icp, self).run(environment)
        return self.session.run('icp', None, *options)
icp = registry.tasks[Icp.name]


class Iexecmd(IRODSTask):
    def run(self, environment, *options):
        super(Iexecmd, self).run(environment)
        return self.session.run('iexecmd', None, *options)
iexecmd = registry.tasks[Iexecmd.name]


class Ifsck(IRODSTask):
    def run(self, environment, *options):
        super(Ifsck, self).run(environment)
        return self.session.run('ifsck', None, *options)
ifsck = registry.tasks[Ifsck.name]


class Ilocate(IRODSTask):
    def run(self, environment, *options):
        super(Ilocate, self).run(environment)
        return self.session.run('ilocate', None, *options)
ilocate = registry.tasks[Ilocate.name]


class Ilsresc(IRODSTask):
    def run(self, environment, *options):
        super(Ilsresc, self).run(environment)
        return self.session.run('ilsresc', None, *options)
ilsresc = registry.tasks[Ilsresc.name]


class Imcoll(IRODSTask):
    def run(self, environment, *options):
        super(Imcoll, self).run(environment)
        return self.session.run('imcoll', None, *options)
imcoll = registry.tasks[Imcoll.name]


class Imeta(IRODSTask):
    def run(self, environment, *options):
        super(Imeta, self).run(environment)
        return self.session.run('imeta', None, *options)
imeta = registry.tasks[Imeta.name]


class Imiscserverinfo(IRODSTask):
    def run(self, environment, *options):
        super(Imiscserverinfo, self).run(environment)
        return self.session.run('imiscserverinfo', None, *options)
imiscserverinfo = registry.tasks[Imiscserverinfo.name]


class Imkdir(IRODSTask):
    def run(self, environment, *options):
        super(Imkdir, self).run(environment)
        return self.session.run('imkdir', None, *options)
imkdir = registry.tasks[Imkdir.name]


class Imv(IRODSTask):
    def run(self, environment, *options):
        super(Imv, self).run(environment)
        return self.session.run('imv', None, *options)
imv = registry.tasks[Imv.name]


class Iphybun(IRODSTask):
    def run(self, environment, *options):
        super(Iphybun, self).run(environment)
        return self.session.run('iphybun', None, *options)
iphybun = registry.tasks[Iphybun.name]


class Iphymv(IRODSTask):
    def run(self, environment, *options):
        super(Iphymv, self).run(environment)
        return self.session.run('iphymv', None, *options)
iphymv = registry.tasks[Iphymv.name]


class Ips(IRODSTask):
    def run(self, environment, *options):
        super(Ips, self).run(environment)
        return self.session.run('ips', None, *options)
ips = registry.tasks[Ips.name]


class Iqdel(IRODSTask):
    def run(self, environment, *options):
        super(Iqdel, self).run(environment)
        return self.session.run('iqdel', None, *options)
iqdel = registry.tasks[Iqdel.name]


class Iqmod(IRODSTask):
    def run(self, environment, *options):
        super(Iqmod, self).run(environment)
        return self.session.run('iqmod', None, *options)
iqmod = registry.tasks[Iqmod.name]


class Iqstat(IRODSTask):
    def run(self, environment, *options):
        super(Iqstat, self).run(environment)
        return self.session.run('iqstat', None, *options)
iqstat = registry.tasks[Iqstat.name]



class Iquest(IRODSTask):
    def run(self, environment, *options):
        super(Iquest, self).run(environment)
        return self.session.run('iquest', None, *options)
iquest = registry.tasks[Iquest.name]


class Iquota(IRODSTask):
    def run(self, environment, *options):
        super(Iquota, self).run(environment)
        return self.session.run('iquota', None, *options)
iquota = registry.tasks[Iquota.name]


class Ireg(IRODSTask):
    def run(self, environment, *options):
        super(Ireg, self).run(environment)
        return self.session.run('ireg', None, *options)
ireg = registry.tasks[Ireg.name]


class Irepl(IRODSTask):
    def run(self, environment, *options):
        super(Irepl, self).run(environment)
        return self.session.run('irepl', None, *options)
irepl = registry.tasks[Irepl.name]



class Irm(IRODSTask):
    def run(self, environment, *options):
        super(Irm, self).run(environment)
        return self.session.run('irm', None, *options)
irm = registry.tasks[Irm.name]


class Irmtrash(IRODSTask):
    def run(self, environment, *options):
        super(Irmtrash, self).run(environment)
        return self.session.run('irmtrash', None, *options)
irmtrash = registry.tasks[Irmtrash.name]


class Irsync(IRODSTask):
    def run(self, environment, *options):
        super(Irsync, self).run(environment)
        return self.session.run('irsync', None, *options)
irsync = registry.tasks[Irsync.name]


class Irule(IRODSTask):
    def run(self, environment, *options):
        super(Irule, self).run(environment)
        return self.session.run('irule', None, *options)
irule = registry.tasks[Irule.name]


class Iscan(IRODSTask):
    def run(self, environment, *options):
        super(Iscan, self).run(environment)
        return self.session.run('iscan', None, *options)
iscan = registry.tasks[Iscan.name]


class Isysmeta(IRODSTask):
    def run(self, environment, *options):
        super(Isysmeta, self).run(environment)
        return self.session.run('isysmeta', None, *options)
isysmeta = registry.tasks[Isysmeta.name]


class Itrim(IRODSTask):
    def run(self, environment, *options):
        super(Itrim, self).run(environment)
        return self.session.run('itrim', None, *options)
itrim = registry.tasks[Itrim.name]


class Iuserinfo(IRODSTask):
    def run(self, environment, *options):
        super(Iuserinfo, self).run(environment)
        return self.session.run('iuserinfo', None, *options)
iuserinfo = registry.tasks[Iuserinfo.name]


class Ixmsg(IRODSTask):
    def run(self, environment, *options):
        super(Ixmsg, self).run(environment)
        return self.session.run('ixmsg', None, *options)
ixmsg = registry.tasks[Ixmsg.name]




