#!/usr/local/bin/python3
import sys
import os
import cftp_exceptions as cftp_ex
from abc import ABCMeta, abstractmethod 


# This code is protected under the GNU General Public License, Version 3.
# See https://www.gnu.org/copyleft/gpl.html.
# Author:  Dude Revolucion (dudrevolucion@gmail.com)


class CFtpClient :

    """Emulates basic ftp client functionality to access cloud storage (abstract class).

    This abstract class emulates ftp client functionality in order to access
    cloud storage.  What's abstract in this class is the cloud storage itself.
    In other words, the abstract methods describe general ways of interacting
    with cloud storage, without specifying a particular type of storage.  A subclass
    oriented toward a particular cloud storage service will need to override
    these abstract methods with code geared toward that service.

    This class implements processing of basic ftp client commands.  Such commands
    can be accessed programmatically.  They can also be accessed interactively
    on the command line.  Instance methods in this class implement the commands.
    Most of these instance methods are abstract, since their implementation
    depends on the cloud provider in use.

    This class has three instance attributes.  The cloudStorageLocation specifies
    the Internet-accessible location of the root directory of the cloud storage
    location.  The remoteWorkingDir indicates the current working location beneath
    that root directory.  For example, in Amazon S3, the bucket name would be
    assigned to the cloudStorageLocation.  

    Attributes:
        cloudStorageLocation (str):  remote storage location
        localWorkingDir (str):  on the local computer
        remoteWorkingDir (str):  on the remote storage

    """

    __metaclass__ = ABCMeta


    ###################################################################
    # Initialization and exception handling
    ###################################################################


    def __init__(self) :
        """ Create an ftp client; no parameters for abstract class."""

        self.cloudStorageLocation = None      
        self.localWorkingDir = os.getcwd()
        self.remoteWorkingDir = None


    def ExceptionWrapper( func ) :
        """ Adds exception hanndling to instance methods.

        This avoids cluttering individual methods with try/except
        clauses.

        Raises: 
            OSError:  A standard python exception indicating an operating
                system-related exception.
            CFTPInvalidCloudLocation:  Attempt to access an invalid
                cloud location.
            CFTPInvalidCommand:  User entered an invalid ftp command.
            CFTPError:  Gracefully handle unanticipated errors.

        """

        def wrapper( *args, **kwargs ) :

            try :
                rVal = func( *args, **kwargs )
                return rVal

            except OSError as osErr :
                print( "OSError:  " + osErr.strerror + " " + osErr.filename )
                sys.exit(1)

            except cftp_ex.CFTPInvalidCloudLocation as e :
                e.errorLog()
                args[0].CommandLine()

            except cftp_ex.CFTPInvalidCommand as e :
                e.errorLog()
                args[0].CommandLine()

            except cftp_ex.CFTPError as e :
                e.errorLog()
                args[0].CommandLine()

        return wrapper




    ###################################################################
    # Methods for interacting with cloud storage (must override)
    ###################################################################


    @ExceptionWrapper
    @abstractmethod
    def cd( self,dirName ) :
        """ Change cloud directory.

        Arguments:
            dirName (str):  directory specifier

        """
    
        self.remoteWorkingDir = dirName
        return 'cd ' + dirName


    @ExceptionWrapper
    @abstractmethod
    def close( self ) :
        """ Close connection to cloud storage location."""
    
        self.cloudStorageLocation = None
        return 'close'


    @ExceptionWrapper
    @abstractmethod
    def delete( self,fileName ) :
        """ Delete cloud file.

        Arguments:
            fileName (str):  file to be deleted
        
        """
    
        return 'delete ' + fileName


    @ExceptionWrapper
    @abstractmethod
    def get( self,fileName ) :
        """ Get file from the cloude.

        Arguments:
            fileName (str):  file to be gotten

        """
    
        return 'get ' + fileName


    @ExceptionWrapper
    @abstractmethod
    def ls( self ) :
        """ List files in a cloud directory."""
    
        return 'ls'


    @ExceptionWrapper
    @abstractmethod
    def mdelete( self,args ) :
        """ Multiple delete files from the cloud.

        Arguements:
            args (list):  list of files to be deleted

        """
    
        return 'mdelete ' + str(args)


    @ExceptionWrapper
    @abstractmethod
    def mget( self,args ) :
        """ Get multiple files from the cloude.

        Arguments:
            args (list):  list of files to be gotten

        """
    
        return 'mget ' + str(args)


    @ExceptionWrapper
    @abstractmethod
    def mkdir( self,dirName ) :
        """Make cloud directory.

        Arguments:
           dirName (str):  directory specifier

        """
    
        return 'mkdir ' + dirName


    @ExceptionWrapper
    @abstractmethod
    def mput( self,args ) :
        """ Transfer multiple files from local host to the cloud.

        Attributes:
            args (list):  files to be transferred

        """
    
        return 'mput ' + str(args)


    @ExceptionWrapper
    @abstractmethod
    def open( self, loc ) :
        """ Open connection to cloud storage location.

        Attributes:
            loc (str):  cloud location to connect with

        """

        self.close()
        self.cloudStorageLocation = None
        return 'open ' + loc


    @ExceptionWrapper
    @abstractmethod
    def put( self,fileName ) :
        """ Put file into the cloud.

        Attributes:
            fileName (str):  file to be placed into the cloud

        """
    
        return 'put ' + fileName


    @ExceptionWrapper
    @abstractmethod
    def rmdir( self,dirName ) :
        """ Remove cloud directory.

        Attributes:  
            dirName (str):  specifies remote directory to be removed.

        """
    
        return 'rmdir ' + dirName


    @ExceptionWrapper
    @abstractmethod
    def IsFile(self,loc) :
        """ Auxiliary method:  check if specified cloud file location is valid (cloud implementation-specific).

        Arguments:
            loc (str):  location to check

        """

        pass


    @ExceptionWrapper
    @abstractmethod
    def IsDir(self,loc) :
        """ Auxiliary method:  check of specified cloud location is a directory.

        Arguments:
            loc (str):  location to check

        """

        return False


    @ExceptionWrapper
    @abstractmethod
    def DirEmpty(self,loc) :
        """ Auxiliary method:  check if specified directory is empty.

        Arguments:
            loc (str):  location to check

        """

        return False

    

    ###################################################################
    # Methods that do not need to be overriden
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
    @abstractmethod
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
                raise cftp_ex.CFTPInvalidCloudLocation
            if ftpCmdFctLookupNoArgs.get( line[0] ) != None :
                rVal = ftpCmdFctLookupNoArgs[ line[0] ]()
            elif ftpCmdFctLookupOneArg.get( line[0] ) != None :
                if len(line) == 2 :
                    rVal = ftpCmdFctLookupOneArg[ line[0] ](line[1])
                else :
                    raise cftp_ex.CFTPInvalidCloudLocation
            elif ftpCmdFctLookupMultipleArgs.get( line[0] ) != None :
                rVal = ftpCmdFctLookupMultipleArgs[ line[0] ](line[1:])
            else :
                raise cftp_ex.CFTPInvalidCommand
            if rVal :
                print( rVal )
            



