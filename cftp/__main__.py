import sys
import cftp.s3



# This code is protected under the GNU General Public License, Version 3.
# See https://www.gnu.org/copyleft/gpl.html.
# Author:  Dude Revolucion (dudrevolucion@gmail.com)


# This implements a console_script under setuptools.


def main(args=None) :
    """Exposes ftp-like command line interface to Amazon S3."""

    if args is None:
        args = sys.argv[1:]
    
    s3ftp = cftp.s3.S3FtpClient( isInteractive=True )

    if len(args) == 0 :
        s3ftp.CommandLine()
        
    elif len(args) == 1 :
        s3ftp.open(args.pop())
        s3ftp.CommandLine()
    else :
        print( "ERROR:  Incorrect number of arguments.")
        sys.exit(1)
    
    

#########################################################
# Get/check arguments if calling from command line
#########################################################

if __name__ == '__main__' :
    main()
