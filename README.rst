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


Configuration
=============

Before using this package, you will need to setup AWS authentication
credentials as required by boto3.  See the following URL:

https://boto3.readthedocs.org/en/latest/guide/quickstart.html#configuration



