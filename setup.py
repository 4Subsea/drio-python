from setuptools import setup, find_packages


# read the contents of your README file
from os import path
with open(path.join(path.abspath(path.dirname(__file__)), 'README.rst')) as f:
    long_description = f.read()


setup(name='datareservoirio',
      version='0.0.1',
      license='MIT',
      description='Python client for DataReservoir.io',
      long_description=long_description,
      long_description_content_type='text/x-rst',
      keywords='drio datareservoir timeseries storage database saas',
      url='http://www.4subsea.com/python/datareservoirio',
      author='4Subsea',
      author_email='support@4subsea.com',
      packages=find_packages(exclude=['tests', 'tests_integration']),
      install_requires=[
          'azure-storage-blob>=1.1.0',
          'numpy',
          'oauthlib',
          'pandas>=0.23.0',
          'requests',
          'requests-oauthlib',
          'six',
          'futures; python_version == "2.7"',
          'future; python_version == "2.7"'
      ],
      include_package_data=True,
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'License :: OSI Approved :: MIT License',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.6'
      ],
      zip_safe=False)
