#!/usr/local/bin/python3
import sys,boto3,json,os
from abc import ABCMeta, abstractmethod
from functools import wraps
from boto3.s3.transfer import S3Transfer
from cftp.base import BaseFtpClient,ExceptionWrapper
import cftp.base_exceptions as bftp_ex
import cftp.s3_exceptions as s3e


# This code is protected under the GNU General Public License, Version 3.
# See https://www.gnu.org/copyleft/gpl.html.
# Author:  Dude Revolucion (dudrevolucion@gmail.com)



###################################################################
# Exception handling decorator
###################################################################

@ExceptionWrapper
def S3ExceptionWrapper( func ) :
    """ Adds S3-specific exception hanndling to instance methods.

    This avoids cluttering individual methods with try/except
    clauses.

    Raises: 
        S3FTPNoSuchBucketError : Attempt to access non-existent
            S3 bucket.
        S3FTPInvalidObjectParameter:  Attempt to specify invalid
            extraArgs parameter for S3 object.

    """

    @wraps(func)

    def wrapper( *args, **kwargs ) :

        try :
            rVal = func( *args, **kwargs )
            return rVal

        except s3e.S3FTPNoSuchBucketError as e :
            e.errorLog()

        except s3e.S3FTPInvalidObjectParameter as e :
            e.errorLog()

        except :
            raise

    return wrapper




###################################################################
# Base Ftp Client class definition
###################################################################

class S3FtpClient(BaseFtpClient) :
    """Emulates basic ftp client functionality to access Amazon S3 cloud storage.

    This class implements the CFtpClient abstract class in order to provide
    ftp client functionality to access Amazon's Simple Storage Service (S3).

    The ftp commands implemented by this class can be accessed programmatically
    and interactively (via the command line).  Instance methods in this class
    implement the commands. 

    Recall the superclass (BaseFtpClient) has four instance attributes.
    In this subclass, cloudStorageLocation is a string that refers to
    the location of the Amazon S3 bucket.  The remoteWorkingDir attribute
    refers to a location within that bucket.  It can either be a directory
    or a file.  If it's a directory, then there's no trailing forward slash.
    The root directory of the bucket is represented by the empty string.
    If remoteWorkingDir is set to None, then there is no active connection
    with an S3 bucket.  In other words, the close method below sets
    remoteWorkingDir to None.  The open method sets it to either the bucket's
    root directory (the empty string) or to some other user-specified
    directory within the bucket.  The localWorkingDir attribute refers to
    the current working directory on the local host.  The isInteractive
    attribute indicates whether we're interacting with Amazon S3 using
    the CommandLine method in the BaseFtpClient superclass.

    When constructing an S3FTP object, default S3 object parameters
    are specified as follows:  First check for the existence of a file
    in the current working directory called .s3ftp.json.  If that exists,
    load parameters from the file.  If not, then check for a file of the
    same name in the user's home directory and load from there.  If the
    constructor is also called with default parameters, then use those
    to overwrite any parameters loaded from the .s3ftp.json file.

    ISSUE TO CHECK:  See the open method.  What if the loc parameter
    points to a file rather than a folder?  Also, could improve handling
    of invalid extraArgs keys and values for the S3 transfer functions.
    Note that we do nothing to handle bad values for the extraArgs parameter.
    Also the mget, mput, and mdelete methods each handles the situation
    where the file name pattern includes a directory, but they do a
    poor job of informing the user.

    Attributes:
        s3Bucket (boto3.S3Bucket)     :  object representing Amazon S3 bucket
        s3Client (boto3.client)       :  used for interacting with Amazon S3
        s3Transfer (boto3.S3Transfer) :  transfers to/from S3
        s3DefaultObjParams (dict)     :  other parameters for S3 objects

    """

    ###################################################################
    # Initialization and exception handling
    ###################################################################

    def __init__( self, isInteractive=False, s3DefaultObjParams=None ) :
        """ Create an S3 ftp client."""

        super().__init__( isInteractive )

        self.s3Bucket = None
        self.s3Client = None
        self.s3Transfer = None
        self.s3DefaultObjParams = None

        # Set default object parameters for S3Transfer from file
        if os.path.exists('.s3ftp.json' ) :
            f = '.s3ftp.json'
        elif os.path.exists( os.path.expanduser('~') + '/.s3ftp.json' ) :
            f = os.path.expanduser('~') + '/.s3ftp.json' 
        else :
            f = None
        if f != None :
            self.LoadS3DefaultObjParams( f, isRelative=False )

        # Set default object parameters from constructor argument
        if s3DefaultObjParams!=None :
            if self.S3ParamsAreValid(s3DefaultObjParams) :
                for key,value in s3DefaultObjParams.items() :
                    self.s3DefaultObjParams[key] = value
            else :
                raise s3e.S3FTPInvalidObjectParameter
                


    ###################################################################
    # Methods for interacting with Amazon S3
    ###################################################################


    @S3ExceptionWrapper
    def close(self) :
        """Closes connection to S3 bucket.
        
        No return value.

        """

        super().close()
        self.s3Bucket = None
        self.s3Client = None
        self.s3Transfer = None



    @S3ExceptionWrapper
    def AuxDeleteFromCloud( self, remotePath ) :
        """ Delete a file from Amazon S3 bucket.

        Arguments:
            remotePath (str):  path to file to be deleted
        
        No return value.

        """

        objs = self.s3Bucket.objects.filter( Prefix=remotePath )
        objs = [ obj for obj in objs if obj.size > 0 ]
        objs[0].delete()



    @S3ExceptionWrapper
    def AuxGetFromCloud( self, remotePath, localPath, extraArgs ) :
        """Downloads a file from an S3 bucket.

        Arguments:
            remotePath (str):  file to be gotten
            localPath (str) :  where to put it
            s3ObjArgs (dict):  args for corresponding S3 client operation

        No return value.

        """

        s3ObjArgs = None
        if self.s3DefaultObjParams :
            s3ObjArgs = { key:value for key,value in self.s3DefaultObjParams.items() if key in S3Transfer.ALLOWED_DOWNLOAD_ARGS}
            if extraArgs :
                s3ObjArgs.update( { key:value for key,value in extraArgs.items() if key in S3Transfer.ALLOWED_DOWNLOAD_ARGS } )
        elif extraArgs :
            s3ObjArgs = { key:value for key,value in extraArgs.items() if key in S3Transfer.ALLOWED_DOWNLOAD_ARGS } 
        self.s3Transfer.download_file( self.cloudStorageLocation, remotePath, localPath, extra_args=s3ObjArgs )
            

    @S3ExceptionWrapper
    def ls(self) :
        """Lists contents of current working folder in an S3 bucket.

        Returns a list.

        """

        try :
            objSummaryIter = self.s3Bucket.objects.filter( Prefix=self.remoteWorkingDir )
        except :
            raise bftp_ex.FTPError
        else :
            if self.remoteWorkingDir :
                objs = { obj.key.replace(self.remoteWorkingDir + '/','',1) for obj in objSummaryIter }
            else :
                objs = { obj.key.split('/')[0] for obj in objSummaryIter }
            if not objs :
                raise bftp_ex.FTPNoSuchDirError
            objs = [ obj.rstrip('/') for obj in (objs - {''}) ]
            objs.sort()
            return objs
        
        

    @S3ExceptionWrapper
    def AuxMkDirInCloud( self,remotePath ) :
        """Make a directory in an S3 bucket.

        Does nothing if folder or file of this name already exists in the
        remote working directory.

        Arguments:
           remotePath (str):  directory specifier

        No return value.

        """

        self.s3Bucket.put_object( Key = remotePath + '/' )


    @S3ExceptionWrapper
    def open(self,loc) :
        """Returns an S3 bucket object, stored in s3Bucket.

        Attributes:
            loc (str):  cloud location to connect with

        No return value.

        """

        bucketName = loc.split('/')[0]

        bucketFolder = "/".join( loc.split('/')[1:] ).rstrip('/')

        try :
            s3 = boto3.resource('s3')
            s3Client = boto3.client('s3')
            s3Transfer = S3Transfer(s3Client)
        except :
            raise base_ex.FTPError
        else :
            bucket = [ a for a in s3.buckets.all() if a.name==bucketName ]
            if not bucket:
                raise s3e.S3FTPNoSuchBucketError
            else :
                self.cloudStorageLocation = bucketName
                self.s3Bucket = bucket[0]
                self.s3Client = s3Client
                self.s3Transfer = s3Transfer
                self.remoteWorkingDir = bucketFolder


    @S3ExceptionWrapper
    def AuxPutInCloud( self, localPath, remotePath, extraArgs ) :
        """Uploads a file to an S3 bucket.

        This is an auxiliary method that encapsulates S3-specific
        functionality.  

        Arguments:
            localPath (str)  : file to be transferred to cloud
            remotePath (str) : where to put it
            extraArgs (dict) : for S3Transfer object
        
        No return value.

        """

        s3ObjArgs = None
        if self.s3DefaultObjParams :
            s3ObjArgs = { key:value for key,value in self.s3DefaultObjParams.items() if key in S3Transfer.ALLOWED_UPLOAD_ARGS}
            if extraArgs :
                s3ObjArgs.update( { key:value for key,value in extraArgs.items() if key in S3Transfer.ALLOWED_UPLOAD_ARGS } )
        elif extraArgs :
            s3ObjArgs = { key:value for key,value in extraArgs.items() if key in S3Transfer.ALLOWED_UPLOAD_ARGS } 
        self.s3Transfer.upload_file( localPath, self.cloudStorageLocation, remotePath, extra_args=s3ObjArgs )


    @S3ExceptionWrapper
    def AuxRmDirFromCloud( self,remotePath ) :
        """ Remove S3 folder.

        Arguments:
            remotePath (str) : directory to be removed
        
        No return value.

        """

        objs = self.s3Bucket.objects.filter( Prefix=remotePath ) 
        objs = [ obj for obj in objs ]
        objs[0].delete()


    @S3ExceptionWrapper
    def IsDir(self,loc) :
        """ Auxiliary method:  check of specified S3 object is a directory.

        An S3 directory is a location that ends with a / character.
        This method assumes loc is a valid S3 location identifier.
        Assumes loc is an absolute path, as returned by the
        AbsolutePath auxiliary function below.

        Returns a boolean.

        """

        objSummaryIter = self.s3Bucket.objects.filter( Prefix=loc )
        objs = [ obj.key for obj in objSummaryIter ]
        objs.sort()
        if loc=='' or (len(objs)==0 and loc=='/') : # bucket root dir
            return True
        elif len(objs)==0 : # no such location, thus not a dir
            return False
        elif (loc==objs[0] and objs[0][-1]=='/') or (loc+'/')==objs[0] : # is a dir
            return True
        else :  # not a dir
            return False


    @S3ExceptionWrapper
    def IsFile(self,loc) :
        """ Auxiliary method:  check if specified S3 file object is valid.

        Is a file if there is only one S3 object of this name with nonzero size.
        This method assumes loc is a valid S3 location identifier.
        Assumes loc is an absolute path, as returned by the
        AbsolutePath auxiliary function.

        Returns a boolean.

        """

        objSummaryIter = self.s3Bucket.objects.filter( Prefix=loc )
        objs = [ obj.size for obj in objSummaryIter ]
        if len(objs)==1 and objs[0]>0 :
            return True
        else :
            return False


    @S3ExceptionWrapper
    def DirEmpty(self,loc) :
        """ Auxiliary method:  check if specified directory is empty.

        This method assumes loc is a valid S3 location identifier.
        Assumes loc is an absolute path, as returned by the
        AbsolutePath auxiliary function.

        Returns a boolean.

        """

        if self.IsDir(loc) :
            objSummaryIter = self.s3Bucket.objects.filter( Prefix=loc )
            objs = [ obj.size for obj in objSummaryIter ]
            if len(objs)==1 and objs[0]==0 :
                return True
            else :
                return False
        else :
            raise base_ex.FTPNoSuchDirError


    @S3ExceptionWrapper
    def AbsolutePath(self,f) :
        """ Auxiliary method:  transform relative path to absolute path.

        This function normalizes paramter f to an absolute path
        specified relative to the root of a bucket location.  Note that
        result of this function has no trailing forward slash character.  
        Also, the root directory of a bucket is represented by setting
        the return parameter to the empty string.

        Parameter f is considered an absolute path if it begins with a /
        character.  Otherwise it is relative to the remote working directory.

        As distinguished from an absolute path, the absolute
        location of an S3 object is obtained by appending the bucket
        name to the absolute path.  

        Returns a string.

        """

        if f[0]!='/' and self.remoteWorkingDir :
            remotePath = self.remoteWorkingDir + '/' + f.rstrip('/')
        else :
            remotePath = f.lstrip('/').rstrip('/')
        remotePath = os.path.normpath(remotePath).replace('\\','/')  # posix
        if remotePath=='.' : # root folder in bucket
            remotePath = ''
        return remotePath



    ###################################################################
    # Methods for specifying/managing S3 object-related parameters
    ###################################################################


    @S3ExceptionWrapper
    def LoadS3DefaultObjParams( self, fileName, isRelative=True ) :
        """Loads default S3 object parameters from a JSON file.

        Loads default S3 object parameters from the specified
        file.  The file can contain a partial or complete set of
        S3 object-related parameters.  If it's a partial set, then
        only overwrite the corresponding object defaults, leaving
        the others unchanged.  The file is read from the local
        working directory, unless isRelative is set to False.

        Arguments:
            fileName (str)       :  file name to read S3 parameters
            isRelative (boolean) :  file is relative to local working dir

        Raises:
            OSError
            FTPInvalidObjectParameter

        No return value.

        """

        if isRelative==True :
            localFile = os.path.basename(fileName)
            fileName = self.localWorkingDir + '/' + localFile
        fp = open( fileName, 'r' )
        s3Params = json.load( fp )
        fp.close()
        if self.S3ParamsAreValid( s3Params ) :
            self.s3DefaultObjParams = s3Params
        else :
            raise s3e.S3FTPInvalidObjectParameter


    @S3ExceptionWrapper
    def SaveS3DefaultObjParams( self, fileName, isRelative=True ) :
        """Stores default S3 object parameters to a JSON file.

        Save the default S3 object parameters to the specified
        file in JSON format.  The file is saved to the current
        local working directory, unless isRelative is set to False.

        Arguments:
            fileName (str) :        file name to store S3 parameters
            isRelative (boolean) :  file is relative to local working dir

        Raises:
            OSError

        No return value.

        """

        if isRelative==True :
            localFile = os.path.basename(fileName)
            fileName = self.localWorkingDir + '/' + localFile
        fp = open( fileName, 'w' )
        json.dump( self.S3DefaultObjParams,fp )
        fp.close()


    @S3ExceptionWrapper
    def GetS3DefaultObjParams(self) :
        """Returns current default S3 parameters (a dictionary)."""
        
        return self.s3DefaultObjParams


    @S3ExceptionWrapper
    def SetS3DefaultObjParams( self, s3Params ) :
        """Sets current default S3 parameters to user-specified values.

        The s3Params argument is a dictionary that contains either a
        partial or complete set of S3 object-related parameters.  If it's
        a partial set, then only overwrite the corresponding object
        defaults, leaving the others unchanged.  If any of the parameters
        are invalid, do nothing.

        Arguments:
            s3Params (dict):  parameters for S3 objects

        Raises:
            S3FTPInvalidObjectParameter

        No return value.

        """

        if self.S3ParamsAreValid( s3Params ) :
            for key,value in s3Params.items() :
                self.s3DefaultObjParams[key] = value
        else :
            raise s3e.S3FTPInvalidObjectParameter


    @S3ExceptionWrapper
    def S3ParamsAreValid( self, s3Params ) :
        """Auxiliary method:  Check extraArgs for S3Transfer functions.

        Recall that S3Transfer's download and upload file functions
        each include an extraArgs parameter that allows specification
        of S3 object-related parameters.  In this function, the
        s3Params argument is a dictionary that contains either a
        partial or complete set of S3 object-related parameters.  This
        method checks that each key represents a valid S3 parameter.
        It does not check validity of the values.  We rely on Amazon
        to do that.  This is probably not the best answer for the
        long term.  Note that Amazon does not seem to expose a simple
        API to check the values.  If a key is invalid, we alert
        the user and indicate that it will be ignored.

        Arguments:
            s3Params (dict):  parameters for S3 objects

        Returns a boolean.

        """

        valid = True
        if s3Params!= None :
            for key in s3Params.keys():
                if key not in S3Transfer.ALLOWED_UPLOAD_ARGS and \
                   key not in S3Transfer.ALLOWED_DOWNLOAD_ARGS :
                    valid = False
        return valid







#########################################################
# Get/check arguments if calling from command line
#########################################################

if __name__ == '__main__' :
    s3ftp = S3FtpClient( isInteractive=True )
    if len(sys.argv) == 1 :
        s3ftp.CommandLine()
        
    elif len(sys.argv) == 2 :
        s3ftp.open(sys.argv.pop())
        s3ftp.CommandLine()
    else :
        print( "ERROR:  Incorrect number of arguments.")
        sys.exit(1)

