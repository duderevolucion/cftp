#!/usr/local/bin/python3
import sys
import os, glob, fnmatch
from functools import wraps
from abc import ABCMeta, abstractmethod 
import cftp.base_exceptions as bftp_ex


# This code is protected under the GNU General Public License, Version 3.
# See https://www.gnu.org/copyleft/gpl.html.
# Author:  Dude Revolucion (dudrevolucion@gmail.com)




###################################################################
# Exception handling decorator
###################################################################

def ExceptionWrapper( func ) :
    """ Adds exception hanndling to instance methods.

    This avoids cluttering individual methods with try/except
    clauses.

    Raises: 
        OSError:  A standard python exception indicating an operating
            system-related exception.
        ValueError:  A standard python exception.  Here it may indicate
            a problem in the extraArgs parameter.
        FTPInvalidCloudLocation:  Attempt to access an invalid
            cloud location.
        FTPInvalidCommand:  User entered an invalid ftp command.
        FTPNoSuchObjectError:  Attempt to access a non-existent object
            (directory or file) in the cloud
        FTPNoSuchDirError:  Attempt to access a remote diretcory
            that does not exist.
        FTPNoSuchFileError:  Attempt to access a non-existent file
            in the cloud.
        FTPIsADirectoryError:  Attempt to access a file that is
            actually a directory.
        FTPObjectAlreadyExistsError:  Attempting to create an object
            (either a file or dircetory) in the cloud, but an object of the
            same name already exists.
        FTPDirNotEmptyError:  Expecting an empty directory, but the
                directory is not actually empty.
        FTPError:  Gracefully handle unanticipated errors.

    """

    @wraps(func)

    def wrapper( *args, **kwargs ) :

        try :
            rVal = func( *args, **kwargs )
            return rVal

        except OSError as osErr :
            print( "OSError:  " + osErr.strerror + " " + osErr.filename )
            sys.exit(1)

        except ValueError as vErr :
            print( 'ValueError:  probably indicates problem with extraArgs parameter.' )

        except bftp_ex.FTPInvalidCloudLocation as e :
            e.errorLog()

        except bftp_ex.FTPInvalidCommand as e :
            e.errorLog()

        except bftp_ex.FTPNoSuchObjectError as e:
            e.errorLog()

        except bftp_ex.FTPNoSuchDirError as e :
            e.errorLog()

        except bftp_ex.FTPNoSuchFileError as e :
            e.errorLog()

        except bftp_ex.FTPIsADirectoryError as e :
            e.errorLog()

        except bftp_ex.FTPObjectAlreadyExistsError as e :
            e.errorLog()

        except bftp_ex.FTPDirNotEmptyError as e :
            e.errorLog()

        except bftp_ex.FTPError as e :
            e.errorLog()

    return wrapper




###################################################################
# Base Ftp Client class definition
###################################################################

class BaseFtpClient :

    """Emulates basic ftp client functionality to access cloud storage (abstract class).

    This abstract class emulates ftp client functionality in order to access an abstract
    cloud storage.  What's abstract in this class is the cloud storage itself.
    In other words, the abstract methods describe general ways of interacting
    with cloud storage, without specifying a particular type of storage.  A subclass
    oriented toward a particular cloud storage service will need to override
    these abstract methods with code geared toward that service.

    This class implements processing of basic ftp client commands.  Such commands
    can be accessed programmatically.  They can also be accessed interactively
    on the command line.  Instance methods in this class implement the commands.
    Abstract helper methods encapsulate specific functionality associated with
    particular cloud implementations.

    This class has four instance attributes.  The cloudStorageLocation specifies
    the Internet-accessible location of the root directory of the cloud storage
    location.  The remoteWorkingDir indicates the current working location beneath
    that root directory.  For example, in Amazon S3, the bucket name would be
    assigned to the cloudStorageLocation.  The isInteractive attribute indicates
    whether a client is running interactively via the CommandLine method.

    Attributes:
        cloudStorageLocation (str):  remote storage location
        localWorkingDir (str):  on the local computer
        remoteWorkingDir (str):  on the remote storage
        isInteractive (Bool):  running interactively via CommandLine

    """

    __metaclass__ = ABCMeta


    ###################################################################
    # Initialization
    ###################################################################


    def __init__( self, isInteractive=False ) :
        """ Create an ftp client; no parameters for abstract class."""

        self.cloudStorageLocation = None      
        self.localWorkingDir = os.getcwd()
        self.remoteWorkingDir = None
        self.isInteractive = False




    ###################################################################
    # Methods for interacting with cloud storage (must override)
    ###################################################################


    @ExceptionWrapper
    def cd( self,dirName ) :
        """ Change cloud directory.

        Probably does not need to be overridden by sublcasses.

        Arguments:
            dirName (str):  directory specifier

        No return value.

        """
    
        remotePath = self.AbsolutePath(dirName)
        if self.IsDir(remotePath) :
            self.remoteWorkingDir = remotePath
        else :
            raise bftp_ex.FTPNoSuchDirError


    @ExceptionWrapper
    def close( self ) :
        """Closes a remote connection.
        
        May need to be overridden by subclasses.

        No return value.

        """

        self.cloudStorageLocation = None
        self.remoteWorkingDir = None


    @ExceptionWrapper
    def delete( self,fileName ) :
        """ Delete a file from the cloud

        This method is independent of a specific cloud service and
        probably will not be overridden by subclasses.  The abstract
        auxiliary method AuxDeleteFromCloud encapsulates functionality
        to delete a file from a particular cloud implementation.  Subclasses
        must override that method.

        Arguments:
            fileName (str):  file to be deleted
        
        No return value.

        Raises:
            FTPIsADirectoryError
            FTPNoSuchObjectError

        """

        remotePath = self.AbsolutePath(fileName) 

        if self.IsFile(remotePath) :
            self.AuxDeleteFromCloud(remotePath)
        elif self.IsDir(remotePath) : 
            raise bftp_ex.FTPIsADirectoryError
        else :
            raise bftp_ex.FTPNoSuchObjectError


    @abstractmethod
    def AuxDeleteFromCloud( self, remotePath ) :
        """ Delete a file from the cloud

        This is an abstract auxiliary method that encapsulates
        cloud-specific functionality.  Subclasses should override
        this method.

        Arguments:
            fileName (str):  file to be deleted (absolute path)
        
        No return value.

        """

        pass


    @ExceptionWrapper
    def get( self,fileName,extraArgs=None ) :
        """Downloads a file from remote cloud location.

        Downloads to current working directory.  Does nothing if
        file parameter is a directory.  Overwrites an existing file
        of the same name the current working directory.  Note
        that if parameter fileName includes directories, then the cloud file
        location is relative to the remote current working directory.
        Suppose in that case fileName is a/b.txt.  Then the file b.txt
        is downloaded from rwd/a/b.txt directly to localWorkingDir/b.txt.
        This method probably does not need to be overridden by
        subclasses.

        Arguments:
            fileName (str):    file to be gotten
            extraArgs (dict):  possibly used by subclasses

        No return value.

        Raises:
            FTPIsADirectoryError
            FTPNoSuchFileError

        """

        localFile = os.path.basename(fileName)
        localPath = self.localWorkingDir + '/' + localFile
        remotePath = self.AbsolutePath(fileName) 

        if self.IsFile( remotePath ) :
            self.AuxGetFromCloud(remotePath,localPath,extraArgs)
        elif self.IsDir( remotePath ) :
            raise bftp_ex.FTPIsADirectoryError
        else :
            raise bftp_ex.FTPNoSuchFileError
            

    @abstractmethod
    def AuxGetFromCloud( self, remotePath, localPath, extraArgs ) :
        """ Get a file from the cloud

        This is an abstract auxiliary method that encapsulates
        cloud-specific functionality.  Subclasses should override
        this method.

        Arguments:
            remotePath (str) : file to be gotten
            localPath (str)  : where to put it
            extraArgs (dict) : possibly useful for subclasses
        
        No return value.

        """

        pass


    @abstractmethod
    def ls( self ) :
        """Lists contents of current working folder in cloud folder.

        Returns a list.  Subclasses must override this with a cloud
        provider-specific implementation.

        """

        pass


    @ExceptionWrapper
    def mdelete( self,args ) :
       """ Deletes multiple files from the cloud.

       Repeatedly deletes files whose name match the pattern(s) specified
       in the function arguments.  Note this function only operates
       on files in the current remote working directory.  Subclasses
       probably do not need to override this method.

       No return value.

       """

       remoteFileList = self.ls()
       for fpattern in args :
           for f in fnmatch.filter( remoteFileList, fpattern ) :
               self.delete(f)


    @ExceptionWrapper
    def mget( self,args,extraArgs=None ) :
        """ Downloads multiple files from the cloud.

        Repeatedly gets files whose name match the pattern(s) specified
        in the function arguments.  Note this function only operates
        on files in the current remote working directory.  Matching
        files are downloaded to the local working directory.  Subclasses
        probably do not need to override this method.

        Arguments:
            args (list):       list of files to be gotten
            extraArgs(dict):   may be used by subclasses

        No return value.

        """

        remoteFileList = self.ls()
        for fpattern in args :
            for f in fnmatch.filter( remoteFileList, fpattern ) :
                self.get(f,extraArgs)


    @ExceptionWrapper
    def mkdir( self,dirName ) :
        """Make a directory in the cloud.

        Does nothing if folder or file of this name already exists in the
        remote working directory.  This method probably does not need
        to be overridden by subclasses.  However, the abstract auxiliary
        method AuxMkDirInCloud encapsulates cloud provider-specific functionality
        for making a directory and would need to be overridden.

        Arguments:
           dirName (str):  directory specifier

        No return value.

        Raises:
            FTPObjectAlreadyExistsError

        """

        remotePath = self.AbsolutePath(dirName)
        
        if not self.IsDir(remotePath) and not self.IsFile(remotePath) :
            self.AuxMkDirInCloud(remotePath)
        else :
            raise bftp_ex.FTPObjectAlreadyExistsError 


    @abstractmethod
    def AuxMkDirInCloud( self, remotePath ) :
        """Make a directory in the cloud.

        This is an abstract auxiliary method that encapsulates
        cloud-specific functionality.  Subclasses should override
        this method.

        Arguments:
            remotePath (str) : path for directory to be created
        
        No return value.

        """

        pass


    @ExceptionWrapper
    def mput( self,args,extraArgs=None ) :
        """ Uploads multiple files to dropbox.

        Invokes python's iglob function on the file pattern(s) specified
        and then repeatedly invokes the put method on the results.  This
        method probably does not need to be overridden by subclasses.

        Attributes:
            args (list):       files to be transferred
            extraArgs(dict):   may be used by subclasses

        No return value.

        """

        for fpattern in args :
            for f in glob.iglob(fpattern) :
                self.put(f,extraArgs)
        

    @abstractmethod
    def open( self, loc ) :
        """Opens a connection to dropbox, returning that connection.

        This method does need to be overridden by subclasses.

        Attributes:
            loc (str):  cloud location to connect with

        No return value.

        """

        pass


    @ExceptionWrapper
    def put( self,fileName,extraArgs=None ) :
        """Uploads a file to the cloud.

        Overwrites an existing file of the same name in the cloud.
        Note that parameter fileName may include folders.  If so, the file
        is uploaded to the current cloud directory.  Probably does
        not need to be overridden by subclasses.  Cloud provider-
        specific functionality is encapsulated in abstract auxiliary
        method AuxPutInCloud, which would need to be overridden.

        Attributes:
            fileName (str):    file to be placed into the cloud
            extraArgs (dict):  may be used by subclasses

        No return value.

        """

        localPath = self.localWorkingDir + '/' + fileName
        (localDir,localFile) = os.path.split(localPath)
        remotePath = self.AbsolutePath(localFile) 
        self.AuxPutInCloud( localPath,remotePath,extraArgs )


    @abstractmethod
    def AuxPutInCloud( self, localPath, remotePath, extraArgs ) :
        """Uploads a file to the cloud.

        This is an abstract auxiliary method that encapsulates
        cloud-specific functionality.  Subclasses should override
        this method.

        Arguments:
            localPath (str)  : file to be transferred to cloud
            remotePath (str) : where to put it
            extraArgs (dict) : possibly useful for subclasses
        
        No return value.

        """

        pass


    @ExceptionWrapper
    def rmdir( self,dirName ) :
        """ Remove cloud folder.

        Probably does not need to be overridden by subclasses.  Cloud
        provider-specific functionality is encapsulated in abstract auxiliary
        method AuxRmDirFromCloud, which would need to be overridden.

        Attributes:  
            dirName (str):  specifies remote directory to be removed.

        No return value.

        Raises:
            FTPDirNotEmptyError
            FTPNoSuchDirError

        """

        remotePath = self.AbsolutePath(dirName)
        if self.IsDir(remotePath) :
            if self.DirEmpty(remotePath) :
                self.AuxRmDirFromCloud( remotePath )
                pass
            else :
                raise bftp_ex.FTPDirNotEmptyError
        else :
            raise bftp_ex.FTPNoSuchDirError


    @abstractmethod
    def AuxRmDirFromCloud( self, remotePath ) :
        """ Remove cloud folder.

        This is an abstract auxiliary method that encapsulates
        cloud-specific functionality.  Subclasses should override
        this method.

        Arguments:
            remotePath (str) : directory to be removed
        
        No return value.

        """

        pass


    @abstractmethod
    def IsDir(self,loc) :
        """ Auxiliary method:  check if specified cloud location is directory.

        Subclasses must override and implement this method.
        This method assumes loc is a valid cloud location identifier.
        Assumes loc is an absolute path, as returned by the
        AbsolutePath auxiliary function below.

        Returns a boolean.

        """

        pass 


    @abstractmethod
    def IsFile(self,loc) :
        """ Auxiliary method:  check if specified location is a file

        Subclasses must override and implement this method.
        This method assumes loc is a valid cloud location identifier.
        Assumes loc is an absolute path, as returned by the
        AbsolutePath auxiliary function.

        Returns a boolean.

        """

        pass


    @ExceptionWrapper
    def DirEmpty(self,loc) :
        """ Auxiliary method:  check if specified directory is empty.

        Subclasses must override and implement this method.        
        This method assumes loc is a valid cloud location identifier.
        Assumes loc is an absolute path, as returned by the
        AbsolutePath auxiliary function.

        Returns a boolean.

        """

        pass


    @abstractmethod
    def AbsolutePath(self,f) :
        """ Auxiliary method:  transform relative cloud path to absolute path.

        Subclasses should override and implement this method.
        What constitutes an absolute path may depend on the cloud
        provider-specific implementation.  

        """

        pass
    

    ###################################################################
    # Methods that do not interact with any cloud implementation
    ###################################################################

    @ExceptionWrapper
    def lcd( self, dirName ) :
        """ Change the working directory on the local machine."""

        os.chdir( dirName )
        self.localWorkingDir = os.getcwd()


    @ExceptionWrapper
    def bye(self) :
        """ Quit. """

        sys.exit(0)
        

    @ExceptionWrapper
    def pwd( self ) :
        """ Print and return current cloud working directory."""
    
        return self.cloudStorageLocation + '/' + self.remoteWorkingDir
 

    ###################################################################
    # Methods for command line processing.
    ###################################################################

    @ExceptionWrapper
    def CommandLine(self) :
        """ Provide interactive command-line ftp interface.

        Provides interactive ftp command-line processing from standard
        input and invokes the appropriate instance method corresponding
        to the parsed command.

        """

        ftpCmdFctLookupNoArgs = {
            'bye'     : self.bye,
            'quit'    : self.bye,
            'close'   : self.close,
            'ls'      : self.ls,
            'pwd'     : self.pwd
        }

        ftpCmdFctLookupOneArg = {
            'open'    : self.open,
            'cd'      : self.cd,
            'delete'  : self.delete,
            'get'     : self.get,
            'lcd'     : self.lcd,
            'mkdir'   : self.mkdir,
            'put'     : self.put,
            'rmdir'   : self.rmdir
        }

        ftpCmdFctLookupMultipleArgs = {
            'mget'    : self.mget,
            'mput'    : self.mput,
            'mdelete' : self.mdelete
        }

        notNeedValidCloudLocation = ( 'open', 'bye', 'quit', 'close', 'lcd' )

        while True :
            line = sys.stdin.readline().split()
            if not line :
                continue
            if self.cloudStorageLocation==None and \
               not line[0] in notNeedValidCloudLocation :
                raise bftp_ex.FTPInvalidCloudLocation
            if ftpCmdFctLookupNoArgs.get( line[0] ) != None :
                rVal = ftpCmdFctLookupNoArgs[ line[0] ]()
            elif ftpCmdFctLookupOneArg.get( line[0] ) != None :
                if len(line) == 2 :
                    rVal = ftpCmdFctLookupOneArg[ line[0] ](line[1])
                else :
                    raise bftp_ex.FTPInvalidCloudLocation
            elif ftpCmdFctLookupMultipleArgs.get( line[0] ) != None :
                rVal = ftpCmdFctLookupMultipleArgs[ line[0] ](line[1:])
            else :
                raise bftp_ex.FTPInvalidCommand
            if rVal :
                print( rVal )
            



