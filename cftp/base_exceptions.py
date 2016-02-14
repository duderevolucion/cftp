#!/usr/local/bin/python3
import sys


# This code is protected under the GNU General Public License, Version 3.
# See https://www.gnu.org/copyleft/gpl.html.
# Author:  Dude Revolucion (dudrevolucion@gmail.com)


#########################################################
# Exceptions raised by BaseFtpClient
#########################################################


class FTPError(Exception) :
    """Gracefully handle unanticipated errors."""

    def errorLog(self):
        sys.stderr.write( 'General BaseFTP error\n' )

class FTPInvalidCloudLocation(Exception) :
    """Attempt to access an invalid cloud location."""

    def errorLog(self):
        sys.stderr.write( 'Invalid location\n' )

class FTPInvalidCommand(Exception) :
    """User entered an invalid ftp command."""

    def errorLog(self):
        sys.stderr.write( 'Invalid Command\n' )

class FTPNoSuchObjectError(Exception) :
    """ Attempt to access a non-existent cloud object (directory or file)."""
    
    def errorLog(self):
        sys.stderr.write( 'No such object (directory or file)\n' )

class FTPNoSuchDirError(Exception) :
    """Attempt to access an cloud directory that does not exist."""

    def errorLog(self):
        sys.stderr.write( 'No such directory\n' )

class FTPNoSuchFileError(Exception) :
    """Attempt to access a non-existent cloud file."""

    def errorLog(self):
        sys.stderr.write( 'No such file\n' )

class FTPIsADirectoryError(Exception) :
    """Attempting to access a cloud file that is actually a directory."""

    def errorLog(self):
        sys.stderr.write( 'Error:  This is a directory.\n' )

class FTPObjectAlreadyExistsError(Exception) :
    """Attempting to create an object (either file or directory) in the cloud,
    but an object of the same name already exists."""
    
    def errorLog(self):
        sys.stderr.write( 'Error:  A file or directory by this name already exists.\n' )

class FTPDirNotEmptyError(Exception) :
    """Expecting an emtpy directory, but the the directory is not actually empty."""

    def errorLog(self):
        sys.stderr.write( 'Error:  Directory is not empty.\n' )

