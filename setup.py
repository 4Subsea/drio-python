from setuptools import setup, find_packages


setup(name='datareservoirio',
      version='0.0.1',
      license='Proprietary',
      description='Python client for 4Subsea datareservoir.io',
      url='http://www.4subsea.com/python/datareservoirio',
      author='4Subsea',
      author_email='support@4subsea.com',
      packages=find_packages(exclude=['tests', 'integrationtests', 'packages']),
      install_requires=[
          'adal>=0.4.3',
          'azure-storage>=0.30.0',
          'numpy',
          'pandas>=0.20.2',
          'requests',
          'futures'
      ],
      include_package_data=True,
      zip_safe=False)
