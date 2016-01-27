from setuptools import setup

def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='cftp',
      version='0.2.1a',
      description='FTP-like client for interacting with cloud storage',
      url='http://github.com/duderevolucion/cftp',
      author='Dude Revolucion',
      author_email='duderevolucion@gmail.com',
      license='GNU',
      packages=['cftp'],
      entry_points={
          'console_scripts': [
              's3ftp = cftp.__main__:main'
          ]
      },
      install_requires=[
          'boto3',
      ],
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
          'Programming Language :: Python :: 3.4',
          'Topic :: Utilities'
      ],
      keywords='cloud s3 amazon ftp',
      zip_safe=False)
