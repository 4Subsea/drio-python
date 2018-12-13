
from __future__ import absolute_import, division, print_function

# COMMON
ENV_DEV = 'DEV'
ENV_TEST = 'TEST'
ENV_QA = 'QA'
ENV_PROD = 'PROD'

API_BASE_URL_DEV = 'http://localhost:5824/api/'
API_BASE_URL_TEST = 'https://reservoir-api-test.4subsea.net/api/'
API_BASE_URL_QA = 'https://reservoir-api-qa.4subsea.net/api/'
API_BASE_URL_PROD = 'https://reservoir-api.4subsea.net/api/'

# B2C
CLIENT_ID_DEV = '9931d0a4-359d-47db-b17d-6fb0bd7679d0'
CLIENT_SECRET_DEV = 'w2[KzZi,3*pM6NA8]#JC_mP5'
CLIENT_ID_TEST = '98b5621f-10c5-470c-8935-7ac266885776'
CLIENT_SECRET_TEST = '3SF1IhJ;,oR533-#0FEYXO]('
CLIENT_ID_QA = 'dabdd9b6-7167-4631-b074-1f28dbae55e5'
CLIENT_SECRET_QA = 'Q7/5RU5%c;Q|vIfJl9r^Owb1'
CLIENT_ID_PROD = '6b879622-4c52-43a3-ba23-2e9595dd996b'
CLIENT_SECRET_PROD = '7gOrIf4b(8IH$13wea38$-x5'

REDIRECT_URI_DEV = 'http://localhost:5824'
REDIRECT_URI_TEST = 'https://reservoir-api-test.4subsea.net'
REDIRECT_URI_QA = 'https://reservoir-api-qa.4subsea.net'
REDIRECT_URI_PROD = 'https://reservoir-api-prod.4subsea.net'

AUTHORITY_URL_DEV = 'http://localhost:5824/account'
AUTHORITY_URL_TEST = 'https://reservoir-api-test.4subsea.net/account'
AUTHORITY_URL_QA = 'https://reservoir-api-qa.4subsea.net/account'
AUTHORITY_URL_PROD = 'https://reservoir-api-prod.4subsea.net/account'

TOKEN_URL_BASE = 'https://login.microsoftonline.com/4subseaid.onmicrosoft.com/' \
                 'oauth2/v2.0/token?p=B2C_1A_SignUpOrSignInWith4ss_'
TOKEN_URL_DEV = TOKEN_URL_TEST = TOKEN_URL_QA = '{}qa'.format(TOKEN_URL_BASE)
TOKEN_URL_PROD = '{}prod'.format(TOKEN_URL_BASE)

SCOPE_DEV = [
    'https://4subseaid.onmicrosoft.com/reservoir-dev/read',
    'https://4subseaid.onmicrosoft.com/reservoir-dev/write',
]
SCOPE_TEST = [
    'https://4subseaid.onmicrosoft.com/reservoir-test/read',
    'https://4subseaid.onmicrosoft.com/reservoir-test/write',
]
SCOPE_QA = [
    'https://4subseaid.onmicrosoft.com/reservoir-qa/read',
    'https://4subseaid.onmicrosoft.com/reservoir-qa/write',
]
SCOPE_PROD = [
    'https://4subseaid.onmicrosoft.com/reservoir-prod/read',
    'https://4subseaid.onmicrosoft.com/reservoir-prod/write',
]

# LEGACY
CLIENT_ID_LEGACY = 'a946277d-6f78-43ab-be34-be689c4dd3e0'

RESOURCE_DEV_LEGACY = 'https://firesubsea.onmicrosoft.com/reservoir-api-dev'
RESOURCE_TEST_LEGACY = 'https://firesubsea.onmicrosoft.com/reservoir-api-test'
RESOURCE_QA_LEGACY = 'https://firesubsea.onmicrosoft.com/reservoir-api-qa'
RESOURCE_PROD_LEGACY = '383e1005-3497-4b05-b08a-10e6d4b49f7d'

TENANT_ID = 'cdf4cf3d-de23-49cf-a9b0-abd2b675f253'
AUTHORITY_URL_LEGACY = 'https://login.microsoftonline.com/{}'.format(TENANT_ID)
TOKEN_URL_LEGACY = AUTHORITY_URL_LEGACY + '/oauth2/token'
