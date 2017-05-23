timeseriesclient
================

.. attention:: DO NOT DISTRIBUTE THE CONTENT OF THIS PACKAGE OUTSIDE 4SUBSEA.

.. important:: Maintainer: ACE

Description
***********

Python client for accessing timeseries stored in 4Subsea Data Reservoir.

Features
''''''''
* Add/upload timeseries
* Append to existing timeseries
* Get/download timeseries
* Delete timeseries
* Retrieve information about timeseries

Getting Started
***************

To install:

.. code:: shell

    pip install timeseriesclient -f \\fil-ask-004\python\pypi

and to update:

.. code:: shell

    pip install timeseriesclient --upgrade -f \\fil-ask-004\python\pypi


Developers
**********


Pre-requisites
''''''''''''''
* python 2.7
* anaconda (https://www.continuum.io/downloads)

Dev environment
'''''''''''''''

In your working folder, do the following:
.. code:: pip install mock
.. code:: pip install -e .
    
If your editor is VS Code, we recommend installing the autopep8 linter extension

