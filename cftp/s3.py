#!/usr/local/bin/python3
import sys
import boto3
import os.path, glob, fnmatch
from abc import ABCMeta, abstractmethod
from functools import wraps
from cftp.base import BaseFtpClient
import cftp.s3_exceptions as s3e
from boto3.s3.transfer import S3Transfer


# This code is protected under the GNU General Public License, Version 3.
# See https://www.gnu.org/copyleft/gpl.html.
# Author:  Dude Revolucion (dudrevolucion@gmail.com)


class S3FtpClient(BaseFtpClient) :
    """Emulates basic ftp client functionality to access Amazon S3 cloud storage.

    This class implements the CFtpClient abstract class in order to provide
    ftp client functionality to access Amazon's Simple Storage Service (S3).

    The ftp commands implemented by this class can be accessed programmatically
    and interactively (via the command line).  Instance methods in this class
    implement the commands. 

    Recall the superclass (BaseFtpClient) has three instance attributes.
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
    the current working directory on the local host.

    ISSUE TO CHECK:  See the open method.  What if the loc parameter
    points to a file rather than a folder?

    Attributes:
        s3Bucket (boto3.S3Bucket):  object representing Amazon S3 bucket
        s3Client (boto3.client):  used for interacting with Amazon S3
        s3Transfer (boto3.S3Transfer):  transfers to/from S3

    """

    ###################################################################
    # Initialization and exception handling
    ###################################################################

    def __init__(self,s3DefaultObjArgs=None) :
        """ Create an S3 ftp client."""

        super().__init__()
        self.s3Bucket = None
        self.s3Client = None
        self.s3Transfer = None
        self.s3DefaultObjArgs=s3DefaultObjArgs


    def ExceptionWrapper( func ) :
        """ Adds exception handling to instance methods.

        This avoids cluttering individual methods with try/except
        clauses.

        Raises: 
            OSError:  A standard python exception indicating an operating
                system-related exception.
            S3FTPNoSuchBucketError:  Attempt to access a non-existent S3 bucket.
            S3FTPNoSuchDirError:  Attempt to access an S3 bucket directory
                that does not exist.
            S3FTPNoSuchObjectError:  Attempt to access a non-existent object
                in an S3 bucket.
            S3FTPIsADirectoryError:  Attempting to access an object that
                is actually a directory.
            S3FTPNoSuchFileError:  Attempting to access a non-existent file
                in an S3 bucket.
            S3FTPObjectAlreadyExistsError:  Attempting to create an object
                (either file or directory) in an S3 bucket, but an object
                of the same name already exists.
            S3FTPDirNotEmptyError:  Expecting an emtpy directory, but the
                the directory is not actually empty.
            S3FTPError:  An unanticipated error has arisen.

        """

        @wraps(func)

        def wrapper( *args, **kwargs) :

            try :
                rVal = func( *args, **kwargs) 
                return rVal 

            except OSError as osErr :
                print( "OSError:  " + osErr.strerror + " " + osErr.filename )
                args[0].CommandLine()

            except s3e.S3FTPNoSuchBucketError as e:
                e.errorLog()
                args[0].CommandLine()

            except s3e.S3FTPNoSuchDirError as e:
                e.errorLog()
                args[0].CommandLine()

            except s3e.S3FTPNoSuchObjectError as e:
                e.errorLog()
                args[0].CommandLine()

            except s3e.S3FTPIsADirectoryError as e:
                e.errorLog()
                args[0].CommandLine()

            except s3e.S3FTPNoSuchFileError as e:
                e.errorLog()
                args[0].CommandLine()

            except s3e.S3FTPObjectAlreadyExistsError as e:
                e.errorLog()
                args[0].CommandLine()

            except s3e.S3FTPDirNotEmptyError as e:
                e.errorLog()
                args[0].CommandLine()

            except s3e.S3FTPError as e:
                e.errorLog()
                args[0].CommandLine()

            except :
                print( "S3Ftp:  Unanticipated Exception\n" )
                sys.exit(1)

        return wrapper



    ###################################################################
    # Methods for interacting with Amazon S3
    ###################################################################


    @ExceptionWrapper
    def cd( self,dirName ) :
        """ Change current directory in the Amazon S3 bucket.

        Arguments:
            dirName (str):  directory specifier

        """
    
        remotePath = self.AbsolutePath(dirName)
        if self.IsDir(remotePath) :
            self.remoteWorkingDir = remotePath
        else :
            raise s3e.S3FTPNoSuchDirError


    @ExceptionWrapper
    def close(self) :
        """Closes connection to S3 bucket."""

        self.cloudStorageLocation = None
        self.s3Bucket = None
        self.s3Client = None
        self.s3Transfer = None
        self.remoteWorkingDir = None


    @ExceptionWrapper
    def delete( self, fileName ) :
        """ Delete a file from Amazon S3 bucket.

        Arguments:
            fileName (str):  file to be deleted
        
        """

        remotePath = self.AbsolutePath(fileName) 

        if self.IsFile(remotePath) :
            objs = self.s3Bucket.objects.filter( Prefix=remotePath )
            objs = [ obj for obj in objs if obj.size > 0 ]
            objs[0].delete()
        elif self.IsDir(remotePath) : 
            raise s3e.S3FTPIsADirectoryError
        else :
            raise s3e.S3FTPNoSuchObjectError


    @ExceptionWrapper
    def get( self, fileName, s3ObjArgs=None ) :
        """Downloads a file from an S3 bucket.

        Downloads to current working directory.  Does nothing if
        file parameter is a directory.  Overwrites an existing file
        of the same name the current working directory.  Note
        that if parameter fileName includes directories, then the S3 object
        location is relative to the remote current working directory.
        Suppose in that case fileName is a/b.txt.  Then the file b.txt
        is downloaded directly to localWorkingDir/b.txt.

        Arguments:
            fileName (str):    file to be gotten
            s3ObjArgs (dict):  args for corresponding S3 client operation

        """

        localFile = os.path.basename(fileName)
        localPath = self.localWorkingDir + '/' + localFile
        remotePath = self.AbsolutePath(fileName) 

        if self.IsFile( remotePath ) :
            self.s3Transfer.download_file( self.cloudStorageLocation, remotePath, localPath, extra_args=s3ObjArgs )
        elif self.IsDir( remotePath ) :
            raise s3e.S3FTPIsADirectoryError
        else :
            raise s3e.S3FTPNoSuchFileError
            

    @ExceptionWrapper
    def ls(self) :
        """Lists contents of current working folder in an S3 bucket."""

        try :
            objSummaryIter = self.s3Bucket.objects.filter( Prefix=self.remoteWorkingDir )
        except :
            raise s3e.S3FTPError
        else :
            if self.remoteWorkingDir :
                objs = { obj.key.replace(self.remoteWorkingDir + '/','',1) for obj in objSummaryIter }
            else :
                objs = { obj.key.split('/')[0] for obj in objSummaryIter }
            if not objs :
                raise s3e.S3FTPNoSuchDirError
            objs = [ obj.rstrip('/') for obj in (objs - {''}) ]
            objs.sort()
            return objs
        
        
    @ExceptionWrapper
    def mdelete( self, args ) :
        """ Deletes multiple files from an S3 bucket.

        Repeatedly deletes files whose name match the pattern(s) specified
        in the function arguments.  Note this function only operates
        on files in the current remote working directory.  

        """

        remoteFileList = self.ls()
        for fpattern in args :
            for f in fnmatch.filter( remoteFileList, fpattern ) :
                self.delete(f)
    

    @ExceptionWrapper
    def mget( self, args, s3ObjArgs=None ) :
        """ Downloads multiple files from an S3 bucket.

        Repeatedly gets files whose name match the pattern(s) specified
        in the function arguments.  Note this function only operates
        on files in the current remote working directory.  Matchin
        files are downloaded to the local working directory.

        Arguments:
            args (list):       list of files to be gotten
            s3ObjArgs (dict):  args for corresponding S3 client operation

        """

        remoteFileList = self.ls()
        for fpattern in args :
            for f in fnmatch.filter( remoteFileList, fpattern ) :
                self.get(f,s3ObjArgs)


    @ExceptionWrapper
    @abstractmethod
    def mkdir( self,dirName ) :
        """Make a directory in an S3 bucket.

        Does nothing if folder or file of this name already exists in the
        remote working directory.

        Arguments:
           dirName (str):  directory specifier

        """

        remotePath = self.AbsolutePath(dirName)
        
        if not self.IsDir(remotePath) and not self.IsFile(remotePath) :
            self.s3Bucket.put_object( Key = remotePath + '/' )
        else :
            raise s3e.S3FTPObjectAlreadyExistsError 


    @ExceptionWrapper
    def mput( self, args, s3ObjArgs=None ) :
        """ Uploads multiple files to an S3 bucket.

        Invokes python's iglob function on the file pattern(s) specified
        and then repeatedly invokes the put method on the results.  

        Attributes:
            args (list):       files to be transferred
            s3ObjArgs (dict):  args for corresponding S3 client operation

        """

        for fpattern in args :
            for f in glob.iglob(fpattern) :
                self.put(f,s3ObjArgs)
        

    @ExceptionWrapper
    def open(self,loc) :
        """Returns an S3 bucket object, stored in s3Bucket.

        Attributes:
            loc (str):  cloud location to connect with

        """

        bucketName = loc.split('/')[0]

        bucketFolder = "/".join( loc.split('/')[1:] ).rstrip('/')

        try :
            s3 = boto3.resource('s3')
            s3Client = boto3.client('s3')
            s3Transfer = S3Transfer(s3Client)
        except :
            raise s3e.S3FTPError
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


    @ExceptionWrapper
    def put( self, fileName, s3ObjArgs=None ) :
        """Uploads a file to an S3 bucket.

        Overwrites an existing file of the same name in the bucket.
        Note that parameter f may include folders.  If so, the file
        is uploaded to the current S3 working directory.

        Attributes:
            fileName (str):    file to be placed into the cloud
            s3ObjArgs (dict):  args for corresponding S3 client operation

        """

        localPath = self.localWorkingDir + '/' + fileName
        (localDir,localFile) = os.path.split(localPath)
        remotePath = self.AbsolutePath(localFile) 
        self.s3Transfer.upload_file( localPath, self.cloudStorageLocation, remotePath, extra_args=s3ObjArgs )


    @ExceptionWrapper
    @abstractmethod
    def rmdir( self,dirName ) :
        """ Remove S3 folder.

        Attributes:  
            dirName (str):  specifies remote directory to be removed.

        """

        remotePath = self.AbsolutePath(dirName)
        if self.IsDir(remotePath) :
            if self.DirEmpty(remotePath) :
                objs = self.s3Bucket.objects.filter( Prefix=remotePath ) 
                objs = [ obj for obj in objs ]
                objs[0].delete()
            else :
                raise s3e.S3FTPDirNotEmptyError
        else :
            raise s3e.S3FTPNoSuchDirError


    @ExceptionWrapper
    def IsDir(self,loc) :
        """ Auxiliary method:  check of specified S3 object is a directory.

        An S3 directory is a location that ends with a / character.
        This method assumes loc is a valid S3 location identifier.
        Assumes loc is an absolute path, as returned by the
        AbsolutePath auxiliary function below.

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


    @ExceptionWrapper
    def IsFile(self,loc) :
        """ Auxiliary method:  check if specified S3 file object is valid.

        Is a file if there is only one S3 object of this name with nonzero size.
        This method assumes loc is a valid S3 location identifier.
        Assumes loc is an absolute path, as returned by the
        AbsolutePath auxiliary function.

        """

        objSummaryIter = self.s3Bucket.objects.filter( Prefix=loc )
        objs = [ obj.size for obj in objSummaryIter ]
        if len(objs)==1 and objs[0]>0 :
            return True
        else :
            return False


    @ExceptionWrapper
    def DirEmpty(self,loc) :
        """ Auxiliary method:  check if specified directory is empty.

        This method assumes loc is a valid S3 location identifier.
        Assumes loc is an absolute path, as returned by the
        AbsolutePath auxiliary function.

        """

        if self.IsDir(loc) :
            objSummaryIter = self.s3Bucket.objects.filter( Prefix=loc )
            objs = [ obj.size for obj in objSummaryIter ]
            if len(objs)==1 and objs[0]==0 :
                return True
            else :
                return False
        else :
            raise s3e.S3FTPNoSuchDirError


    @ExceptionWrapper
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

        """

        if f[0]!='/' and self.remoteWorkingDir :
            remotePath = self.remoteWorkingDir + '/' + f.rstrip('/')
        else :
            remotePath = f.lstrip('/').rstrip('/')
        remotePath = os.path.normpath(remotePath).replace('\\','/')  # posix
        if remotePath=='.' : # root folder in bucket
            remotePath = ''
        return remotePath



#########################################################
# Get/check arguments if calling from command line
#########################################################

if __name__ == '__main__' :
    s3ftp = S3FtpClient()
    if len(sys.argv) == 1 :
        s3ftp.CommandLine()
        
    elif len(sys.argv) == 2 :
        s3ftp.open(sys.argv.pop())
        s3ftp.CommandLine()
    else :
        print( "ERROR:  Incorrect number of arguments.")
        sys.exit(1)

