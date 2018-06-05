from setuptools import setup, find_packages


setup(name='datareservoirio',
      version='0.0.1',
      license='Proprietary',
      description='Python client for 4Subsea datareservoir.io',
      keywords='drio datareservoir timeseries saas',
      url='http://www.4subsea.com/python/datareservoirio',
      author='4Subsea',
      author_email='support@4subsea.com',
      packages=find_packages(exclude=['tests', 'integrationtests', 'packages']),
      install_requires=[
          'adal>=1.0.0',
          'azure-storage-blob>=1.1.0',
          'numpy',
          'pandas>=0.23.0',
          'requests',
          'six',
          'futures; python_version == "2.7"',
          'future; python_version == "2.7"'
      ],
      include_package_data=True,
      zip_safe=False)
