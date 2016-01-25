#!/usr/local/bin/python3
import sys


# This code is protected under the GNU General Public License, Version 3.
# See https://www.gnu.org/copyleft/gpl.html.
# Author:  Dude Revolucion (dudrevolucion@gmail.com)


#########################################################
# Exceptions raised by cftp
#########################################################


class S3FTPError(Exception) :
    """An unanticipated error has arisen."""

    def errorLog(self):
        sys.stderr.write( 'General s3ftp error\n' )

class S3FTPNoSuchBucketError(Exception) :
    """Attempt to access a non-existent S3 bucket."""

    def errorLog(self):
        sys.stderr.write( 'No such bucket\n' )

class S3FTPNoSuchDirError(Exception) :
    """Attempt to access an S3 bucket directory that does not exist."""

    def errorLog(self):
        sys.stderr.write( 'No such directory\n' )

class S3FTPNoSuchObjectError(Exception) :
    """Attempt to access a non-existent object in an S3 bucket."""

    def errorLog(self):
        sys.stderr.write( 'No such object\n' )

class S3FTPIsADirectoryError(Exception) :
    """Attempting to access an object that is actually a directory."""

    def errorLog(self):
        sys.stderr.write( 'Error:  This is a directory.\n' )

class S3FTPNoSuchFileError(Exception) :
    """Attempting to access a non-existent file in an S3 bucket."""

    def errorLog(self):
        sys.stderr.write( 'Error:  No such file.\n' )

class S3FTPObjectAlreadyExistsError(Exception) :
    """Attempting to create an object (either file or directory) in an S3 bucket,
    but an object of the same name already exists."""
    
    def errorLog(self):
        sys.stderr.write( 'Error:  A file or directory by this name already exists.\n' )

class S3FTPDirNotEmptyError(Exception) :
    """Expecting an emtpy directory, but the the directory is not actually empty."""

    def errorLog(self):
        sys.stderr.write( 'Error:  Directory is not empty.\n' )

