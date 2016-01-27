================
CLOUD FTP CLIENT
================


Introduction
============

The cftp package provides an ftp-like interface to cloud-based
storage services.  It is basically a wrapper.  It abstracts
the user from the particulars of specific cloud storage services
by exposing a common interface.  Accessing the interface is
simple.  Instantiate a class corresponding to a desired
cloud storage service (eg, Amazon S3).  The class has methods
corresponding to common ftp client commands.  

The current version includes the four files
noted below.

1.  base.py - abstract base class for cloud ftp client
2.  base_exceptions.py - exceptions raised by base.py
3.  s3.py - ftp-like client interface to Amazon's S3 service
4.  s3_exceptions.py - exceptions raised by s3.py
5.  __main__.py - shim for s3ftp command line script (console script)

Over time, this package may be extended to include an
ftp-like client interface to the DropBox storage services.  That
functionality would be implemented as a sub-class of base.py,
just as s3.py is a sub-class.  The same is true for other
cloud-based storage services that offer an API.


Installation
============

Install using *pip* or *easy_install*.  The cftp package does
require Amazon's boto3 package, which exposes an API for controlling
AWS objects using python.  The boto3 package will be automatically
installed (via pip or easy_install), since it is listed as a
dependency in this package.

This package also includes a tool called *s3ftp* that provides 
command line, ftp-like access to the Amazon S3 service.



Configuration
=============

Before using this package, you will need to setup AWS authentication
credentials as required by boto3.  See the following URL:

https://boto3.readthedocs.org/en/latest/guide/quickstart.html#configuration


Using the *s3ftp* Command Line Utility
======================================

The following illustrates using the interactive *s3ftp* command line
utility.  It assumes the existence of an Amazon S3 bucket, in this
this case a notional bucket named *com.s3ftp.test*.

The following connects to a bucket, lists its contents,
changes the remote working directory (in the bucket),
changes the local working directory, creates a directory
in the bucket, uploads a file to it, multiple gets several files,
deletes a file from the bucket, closes the bucket, opens a
new bucket, lists its contents, and quits.  Below does not
show s3ftp output.

    s3ftp com.s3ftp.test

    ls

    cd dir1/dir2

    lcd ~

    mkdir dir3

    put file1

    mget f*.txt

    delete file2

    close

    open com.s3ftp2.test2

    ls

    close

    quit


Note
====

The Amazon S3 client above does not support creation or deletion
of S3 buckets.  It assumes the bucket already exists.  This is
consistent with behavior of a traditional ftp client in that it
is accessing existing storage (on an ftp server).

This software has been tested on Linux but not Windows or
Mac OS platforms.


Changes in this Release
=======================

Fixed some minor formatting issues in README.rst and
added a note indicating cftp has not been tested on
Windows and Mac platforms.


