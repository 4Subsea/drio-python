
from __future__ import absolute_import, division, print_function

RESOURCE_TEST = '7555e98b-963d-4ff4-9920-c7c11a3ef295'
RESOURCE_QA = 'afab01f7-ea88-4788-92d5-fade82b9027a'
RESOURCE_PROD = '383e1005-3497-4b05-b08a-10e6d4b49f7d'
RESOURCE_DEV = '99f4668d-2f20-4fbb-bd7e-36498904c835'

CLIENT_ID = 'a946277d-6f78-43ab-be34-be689c4dd3e0'

TENANT_ID = 'cdf4cf3d-de23-49cf-a9b0-abd2b675f253'
AUTHORITY = 'https://login.microsoftonline.com/{}'.format(TENANT_ID)


ENV_TEST = 'TEST'
ENV_QA = 'QA'
ENV_PROD = 'PROD'
ENV_DEV = 'DEV'


API_BASE_URL_TEST = 'https://reservoir-api-test.4subsea.net/api/'
API_BASE_URL_QA = 'https://reservoir-api-qa.4subsea.net/api/'
API_BASE_URL_PROD = 'https://reservoir-api.4subsea.net/api/'
API_BASE_URL_DEV = 'http://localhost:5824/api/'
