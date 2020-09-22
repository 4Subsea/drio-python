# read the contents of your README file
from os import path

from setuptools import find_packages, setup

with open(path.join(path.abspath(path.dirname(__file__)), "README.rst")) as f:
    long_description = f.read()


setup(
    name="datareservoirio",
    version="0.0.1",
    license="MIT",
    description="DataReservoir.io Python API",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    keywords="drio datareservoir timeseries storage database saas",
    url="https://www.datareservoir.io",
    author="4Subsea",
    author_email="support@4subsea.com",
    packages=find_packages(exclude=["tests", "tests_integration"]),
    project_urls={"Documentation": "https://www.datareservoir.io/python/docs/latest/"},
    install_requires=[
        "azure-storage-blob>=12.5.0,<13.0.0",
        "numpy",
        "oauthlib",
        "pandas>=0.24.0",
        "pyarrow",
        "requests",
        "requests-oauthlib",
    ],
    include_package_data=True,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    zip_safe=False,
)
