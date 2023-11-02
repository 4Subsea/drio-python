# COMMON
ENV_DEV = "DEV"
ENV_TEST = "TEST"
ENV_QA = "QA"
ENV_PROD = "PROD"

API_BASE_URL_DEV = "http://localhost:5824/api/"
API_BASE_URL_TEST = "https://reservoir-api-test.4subsea.net/api/"
API_BASE_URL_QA = "https://reservoir-api-qa.4subsea.net/api/"
API_BASE_URL_PROD = "https://reservoir-api.4subsea.net/api/"

# USER (B2C - NOT REALLY SECRETS...)
CLIENT_ID_DEV_USER = "9931d0a4-359d-47db-b17d-6fb0bd7679d0"
CLIENT_SECRET_DEV_USER = "eK1{Vn_K]zpGl7(4%t4b;k2S"
REDIRECT_URI_DEV_USER = "http://localhost:5824"
AUTHORITY_URL_DEV_USER = "http://localhost:5824/account"
APPLICATIONINSIGHTS_DEV_CONNECTIONSTRING = "InstrumentationKey=43eacbb6-eed8-4ef1-bc81-10d7959b91e2;IngestionEndpoint=https://westeurope-5.in.applicationinsights.azure.com/;LiveEndpoint=https://westeurope.livediagnostics.monitor.azure.com/"
SCOPE_DEV_USER = [
    "https://4subseaid.onmicrosoft.com/reservoir-dev/read",
    "https://4subseaid.onmicrosoft.com/reservoir-dev/write",
]


CLIENT_ID_TEST_USER = "98b5621f-10c5-470c-8935-7ac266885776"
CLIENT_SECRET_TEST_USER = "3SF1IhJ;,oR533-#0FEYXO]("
REDIRECT_URI_TEST_USER = "https://reservoir-api-test.4subsea.net"
AUTHORITY_URL_TEST_USER = "https://reservoir-api-test.4subsea.net/account"
APPLICATIONINSIGHTS_TEST_CONNECTIONSTRING = "InstrumentationKey=725af0e5-7530-4f2c-b055-36258831785e;IngestionEndpoint=https://westeurope-5.in.applicationinsights.azure.com/;LiveEndpoint=https://westeurope.livediagnostics.monitor.azure.com/"
SCOPE_TEST_USER = [
    "https://4subseaid.onmicrosoft.com/reservoir-test/read",
    "https://4subseaid.onmicrosoft.com/reservoir-test/write",
]


CLIENT_ID_QA_USER = "dabdd9b6-7167-4631-b074-1f28dbae55e5"
CLIENT_SECRET_QA_USER = "Q7/5RU5%c;Q|vIfJl9r^Owb1"
REDIRECT_URI_QA_USER = "https://reservoir-api-qa.4subsea.net"
AUTHORITY_URL_QA_USER = "https://reservoir-api-qa.4subsea.net/account"
APPLICATIONINSIGHTS_QA_CONNECTIONSTRING = "InstrumentationKey=aec779fc-7a4c-4580-b205-cd9f8ecdcf48;IngestionEndpoint=https://westeurope-5.in.applicationinsights.azure.com/;LiveEndpoint=https://westeurope.livediagnostics.monitor.azure.com/"
SCOPE_QA_USER = [
    "https://4subseaid.onmicrosoft.com/reservoir-qa/read",
    "https://4subseaid.onmicrosoft.com/reservoir-qa/write",
]


CLIENT_ID_PROD_USER = "6b879622-4c52-43a3-ba23-2e9595dd996b"
CLIENT_SECRET_PROD_USER = "7gOrIf4b(8IH$13wea38$-x5"
REDIRECT_URI_PROD_USER = "https://reservoir-api.4subsea.net"
AUTHORITY_URL_PROD_USER = "https://reservoir-api.4subsea.net/account"
APPLICATIONINSIGHTS_PROD_CONNECTIONSTRING = "InstrumentationKey=3dbde241-7407-4577-b670-f9d561f96ee4;IngestionEndpoint=https://westeurope-5.in.applicationinsights.azure.com/;LiveEndpoint=https://westeurope.livediagnostics.monitor.azure.com/"
SCOPE_PROD_USER = [
    "https://4subseaid.onmicrosoft.com/reservoir-prod/read",
    "https://4subseaid.onmicrosoft.com/reservoir-prod/write",
]


# CLIENT
TOKEN_URL_DEV_CLIENT = (
    "https://login.microsoftonline.com/4subseaid.onmicrosoft.com/oauth2/v2.0/token"
)
SCOPE_DEV_CLIENT = ["https://4subseaid.onmicrosoft.com/reservoir-dev/.default"]

TOKEN_URL_TEST_CLIENT = (
    "https://login.microsoftonline.com/4subseaid.onmicrosoft.com/oauth2/v2.0/token"
)
SCOPE_TEST_CLIENT = ["https://4subseaid.onmicrosoft.com/reservoir-test/.default"]

TOKEN_URL_QA_CLIENT = (
    "https://login.microsoftonline.com/4subseaid.onmicrosoft.com/oauth2/v2.0/token"
)
SCOPE_QA_CLIENT = ["https://4subseaid.onmicrosoft.com/reservoir-qa/.default"]

TOKEN_URL_PROD_CLIENT = (
    "https://login.microsoftonline.com/4subseaid.onmicrosoft.com/oauth2/v2.0/token"
)
SCOPE_PROD_CLIENT = ["https://4subseaid.onmicrosoft.com/reservoir-prod/.default"]

# APP INSIGHTS
ENV_VAR_ENABLE_APP_INSIGHTS = "DRIO_PYTHON_APPINSIGHTS"
ENV_VAR_ENGINE_ROOM_APP_ID = "ENGINE_ROOM_APP_ID"
