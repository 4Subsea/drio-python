from setuptools import setup, find_packages


setup(name='timeseriesclient', 
      version='0.1.1',
      license='Proprietary',
      description='Python client for timeseries in 4Subsea data reservoir',
      url='http://www.4subsea.com/python/timeseriesclient',
      author='4Subsea',
      author_email='python@4subsea.com',
      packages=find_packages(exclude=['tests', 'integrationtests']),
        install_requires = [
            'adal>=0.4.3',
            'azure>=1.0.3',
            'azure-storage>=0.30.0',
            'numpy',
            'pandas'
        ],
    include_package_data = True,
    zip_safe=False)
