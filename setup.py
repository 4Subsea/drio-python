from setuptools import setup, find_packages

setup(name='timeseriesclient', 
      version='0.3',
      description='client to access timeseriesservice',
      url='',
      author='4Subsea',
      packages=find_packages(),
      zip_safe=False,
        install_requires = [
            "adal>=0.4.3",
            "azure>=1.0.3",
            "azure-storage>=0.30.0", 
        ] )

      
 #['orcaflextools', 'orcaflextools.tests'],echo %PATH#
