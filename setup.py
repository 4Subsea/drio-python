from setuptools import setup, find_packages
import json

def update_version():
    #read version file
    #update minor
    with open('version.json', 'r') as f:
        version = json.load(f)

    version['minor'] += 1

    with open('version.json', 'w') as f:
        json.dump(version, f)

    return "{}.{}".format(version['major'], version['minor'])


setup(name='timeseriesclient', 
      version=update_version(),
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
