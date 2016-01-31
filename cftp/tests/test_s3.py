import unittest
import cftp.s3




class TestS3FtpClient( unittest.TestCase ) :
    """Tests S3FtpClient functionality.
    
    This tests various aspects of interacting with a remote
    S3 bucket using ftp client-like functionality.  The setUp
    method will request the name of a remote S3 bucket.  It's
    best to create an empty, dedicated bucket for this purpose.
    Doing so will prevent possible overwriting of data in
    an existing bucket.  The suite then runs through
    the various methods of the S3FtpClient class.
    
    """


    def setUp( self ) :
        """Test preliminaries.
        
        Create an S3FtpClient object and get the name of a
        remote S3 bucket used for the test.

        """
        
#        s3ftp = cftp.s3.S3FtpClient()
3        s3Bucket = input( 'Enter AWS S3 bucket name:  ')

        pass




    def tearDown( self ) :
        """Test cleanup.  Nothing now."""
        pass


    def testMe( self ) :
        print( 'Bucket is:  ', s3Bucket )
        pass



if __name__ == '__main__':
    unittest.main()
