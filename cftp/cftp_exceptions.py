#!/usr/local/bin/python3
import sys


# This code is protected under the GNU General Public License, Version 3.
# See https://www.gnu.org/copyleft/gpl.html.
# Author:  Dude Revolucion (dudrevolucion@gmail.com)


#########################################################
# Exceptions raised by cftp
#########################################################


class CFTPError(Exception) :
    """Gracefully handle unanticipated errors."""

    def errorLog(self):
        sys.stderr.write( 'General CFTP error\n' )

class CFTPInvalidCloudLocation(Exception) :
    """Attempt to access an invalid cloud location."""

    def errorLog(self):
        sys.stderr.write( 'Invalid location\n' )

class CFTPInvalidCommand(Exception) :
    """User entered an invalid ftp command."""

    def errorLog(self):
        sys.stderr.write( 'Invalid Command\n' )


