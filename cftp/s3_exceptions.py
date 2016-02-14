#!/usr/local/bin/python3
import sys


# This code is protected under the GNU General Public License, Version 3.
# See https://www.gnu.org/copyleft/gpl.html.
# Author:  Dude Revolucion (dudrevolucion@gmail.com)


#########################################################
# Exceptions raised by s3
#########################################################


class S3FTPNoSuchBucketError(Exception) :
    """Attempt to access a non-existent S3 bucket."""

    def errorLog(self):
        sys.stderr.write( 'No such bucket\n' )

class S3FTPInvalidObjectParameter(Exception) :
    """Attempt to specify an invalid object parameter.

    This pertains to the extraArgs parameter of the S3Transfer
    functions for downloading and uploading files.  The object
    parameters include things like metadata, encryption algorithm,
    content-type, and so forth.

    """

    def errorLog(self):
        sys.stderr.write( 'One or more invalid S3 object parameters.\n' )
        sys.stderr.write( 'In interactive mode, they are ignored.\n' )
        sys.stderr.write( 'Otherwise, no parameters are changed until all are correct.\n' )

