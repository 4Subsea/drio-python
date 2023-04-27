"""
RESPONSE_CASES defines attributes assigned to ``requests.Response`` object. The
following attributes can be used as needed:

    * ``_content``
    * ``status_code``
    * ``headers``
    * ``history``
    * ``encoding``
    * ``reason``
    * ``cookies``
    * ``elapsed``
    * ``request``

Note that ``url`` is defined as part of the key in RESPONSE_CASES.
See ``requests.Response`` source code for more details.
"""
from pathlib import Path
from requests import Request

TEST_PATH = Path(__file__).parent


RESPONSE_CASES_GENERAL = {
    # description: blob day file (numeric) from remote storage
    ("GET", "http://blob/dayfile/numeric"): {
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_CASES_GENERAL" / "dayfile_numeric.csv"
        ).read_bytes(),
        "status_code": 200,
        "reason": "OK",
    },
    # description: blob day file (string) from remote storage
    ("GET", "http://blob/dayfile/string"): {
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_CASES_GENERAL" / "dayfile_string.csv"
        ).read_bytes(),
        "status_code": 200,
        "reason": "OK",
    },
    # description: blob day file (malformatted string) from remote storage
    ("GET", "http://blob/dayfile/string/malformatted"): {
        "_content": (
            TEST_PATH
            / "testdata"
            / "RESPONSE_CASES_GENERAL"
            / "dayfile_string_malformatted.csv"
        ).read_bytes(),
        "status_code": 200,
        "reason": "OK",
    },
    # description: blob do not exist in remote storage
    ("GET", "http://example/no/exist"): {
        "status_code": 404,
        "reason": "Not Found",
    },
    # description: put data to blob
    ("PUT", "http://example/blob/url"): {
        "status_code": 201,
        "reason": "Created",
    },
    # description: TimeSeries API response (empty data)
    (
        "GET",
        "https://reservoir-api.4subsea.net/api/timeseries/e3d82cda-4737-4af9-8d17-d9dfda8703d0/data/days?start=-9214560000000000000&end=9214646399999999999",
    ): {
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_CASES_GENERAL" / "data_days_empty.json"
        ).read_bytes(),
        "status_code": 200,
        "reason": "OK",
    },
    # description: commit file for processing
    ("POST", "https://reservoir-api.4subsea.net/api/files/commit"): {
        "status_code": 204,
        "reason": "No Content",
    },
    # description: PUT raises status
    ("PUT", "http://example/put/raises"): {
        "status_code": 501,
        "reason": "Not Implemented",
    },
    # description: POST raises status
    ("POST", "http://example/post/raises"): {
        "status_code": 501,
        "reason": "Not Implemented",
    },
    # description: ping the DataReservoir.io
    ("GET", "https://reservoir-api.4subsea.net/api/ping"): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_CASES_GENERAL" / "ping.json"
        ).read_bytes(),
        "request": Request("GET", "https://reservoir-api.4subsea.net/api/ping"),
    },
}


RESPONSE_GROUP1 = {
    # =========================================================================
    # This 'group' of response cases represents the DataReservoir.io backend response
    # and Azure Blob Storage responses when requesting data for a timeries with:
    #
    #   * ID = "2fee7f8a-664a-41c9-9b71-25090517c275"
    #   * start = 1672358400000000000
    #   * end = 1672703939999999999
    #
    # I.e.,
    #   /api/timeseries/2fee7f8a-664a-41c9-9b71-25090517c275/data/days?start=1672358400000000000&end=1672703939999999999"
    #
    # Comments:
    #   * Numeric data
    #   * No overlap (i.e., only one 'File')
    #
    # =========================================================================
    #
    # A (TimeSeries API response)
    # ---------------------------
    (
        "GET",
        "https://reservoir-api.4subsea.net/api/timeseries/2fee7f8a-664a-41c9-9b71-25090517c275/data/days?start=1672358400000000000&end=1672703939999999999",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "data_days.json"
        ).read_bytes(),
    },
    # B (Azure Blob Storage response)
    # -------------------------------
    (
        "GET",
        "https://permanentprodu000p169.blob.core.windows.net/data/1b0d906b34ce40d69520e46f49a54545/2022/12/30/day/csv/19356.csv?versionid=2023-03-14T14:56:10.8583280Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-03-14T13%3A50%3A58Z&ske=2023-03-15T13%3A50%3A57Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-03-14T17%3A21%3A07Z&sr=b&sp=r&sig=QTYi%2FAeiMFg72EyxC8d%2BV0M0lmgbYek%2BfGXhAXvme1U%3D",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "19356.csv"
        ).read_bytes(),
    },
    # C (Azure Blob Storage response)
    # -------------------------------
    (
        "GET",
        "https://permanentprodu003p208.blob.core.windows.net/data/1b0d906b34ce40d69520e46f49a54545/2022/12/31/day/csv/19357.csv?versionid=2023-03-14T14:56:11.0377879Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-03-14T14%3A00%3A24Z&ske=2023-03-15T14%3A00%3A24Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-03-14T17%3A21%3A07Z&sr=b&sp=r&sig=MIchFxo2gfRsa82kqTgHtq1DRY7cQldsZ0jQi4ySPZE%3D",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "19357.csv"
        ).read_bytes(),
    },
    # D (Azure Blob Storage response)
    # -------------------------------
    (
        "GET",
        "https://permanentprodu003p153.blob.core.windows.net/data/1b0d906b34ce40d69520e46f49a54545/2023/01/01/day/csv/19358.csv?versionid=2023-03-14T14:56:11.1047474Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-03-14T13%3A44%3A11Z&ske=2023-03-15T13%3A43%3A49Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-03-14T17%3A21%3A07Z&sr=b&sp=r&sig=Uo5EqCiYRXcWDjifsiom9uGi7CNhFkGZQ4lBsGgEmC8%3D",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "19358.csv"
        ).read_bytes(),
    },
    # E (Azure Blob Storage response)
    # -------------------------------
    (
        "GET",
        "https://permanentprodu002p192.blob.core.windows.net/data/1b0d906b34ce40d69520e46f49a54545/2023/01/02/day/csv/19359.csv?versionid=2023-03-14T14:56:11.1906071Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-03-14T13%3A50%3A18Z&ske=2023-03-15T13%3A50%3A18Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-03-14T17%3A21%3A07Z&sr=b&sp=r&sig=bto08GDGsK7M%2FZLXOQ%2Bhm3sgYd%2B23g6rs5fI0nCq9AQ%3D",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "19359.csv"
        ).read_bytes(),
    },
    # F (TimeSeries API response)
    # ---------------------------
    (
        "GET",
        "https://reservoir-api.4subsea.net/api/timeseries/2fee7f8a-664a-41c9-9b71-25090517c275/data/days?start=-9214560000000000000&end=9214646399999999999",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "data_days.json"
        ).read_bytes(),
    },
}


RESPONSE_GROUP2 = {
    # =========================================================================
    # This 'group' of response cases represents the DataReservoir.io backend responses
    # and Azure Blob Storage responses when requesting data for a timeseries with:
    #
    #   * ID = "693cb0b2-3599-46d3-b263-ea913a648535"
    #   * start = 1672358400000000000 (i.e., 2022-12-30T00:00)
    #   * end = 1672617600000000000 (i.e., 2023-01-02T00:00)
    #
    # I.e.,
    #   /api/timeseries/693cb0b2-3599-46d3-b263-ea913a648535/data/days?start=1672358400000000000&end=1672617600000000000
    #
    # Comments:
    #   * Overlapping data (i.e., several 'Files')
    #
    # =========================================================================
    #
    # A (TimeSeries API response)
    # ---------------------------
    (
        "GET",
        "https://reservoir-api.4subsea.net/api/timeseries/693cb0b2-3599-46d3-b263-ea913a648535/data/days?start=1672358400000000000&end=1672617600000000000",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP2" / "data_days.json"
        ).read_bytes(),
    },
    # B (Azure Blob Storage response)
    # -------------------------------
    (
        "GET",
        "https: //permanentprodu000p106.blob.core.windows.net/data/03fc12505d3d41fea77df405b2563e49/2022/12/30/day/csv/19356.csv?versionid=2023-04-14T13:17:44.5067517Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T16%3A00%3A41Z&ske=2023-04-14T16%3A00%3A41Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=csFUPlbzexTJkgrLszdJrKTum5jUi%2BWv2PnIN9yM92Y%3D",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP2" / "19356.csv"
        ).read_bytes(),
    },
    # C (Azure Blob Storage response)
    # -------------------------------
    (
        "GET",
        "https: //permanentprodu001p067.blob.core.windows.net/data/03fc12505d3d41fea77df405b2563e49/2022/12/31/day/csv/19357.csv?versionid=2023-04-14T13:17:44.6722101Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T15%3A51%3A15Z&ske=2023-04-14T15%3A51%3A15Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=TMfeXQYlcAe%2BdZGSGy5Z1WTytf41uIUQlQKBlDOQ3b4%3D",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP2" / "19357.csv"
        ).read_bytes(),
    },
    # D (Azure Blob Storage response)
    # -------------------------------
    (
        "GET",
        "https: //permanentprodu002p193.blob.core.windows.net/data/629504a5fe3449049370049874b69fe0/2022/12/30/day/csv/19356.csv?versionid=2023-04-14T13:18:26.1211914Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T15%3A55%3A52Z&ske=2023-04-14T15%3A55%3A51Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=dwqY3aiVKRb6MEwQYw%2B34y4LJcp0VHLat1BBNl9sUX8%3D",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP2" / "19356_1.csv"
        ).read_bytes(),
    },
    # E (Azure Blob Storage response)
    # -------------------------------
    (
        "GET",
        "https: //permanentprodu001p232.blob.core.windows.net/data/629504a5fe3449049370049874b69fe0/2022/12/31/day/csv/19357.csv?versionid=2023-04-14T13:18:26.2782276Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T15%3A53%3A09Z&ske=2023-04-14T15%3A53%3A09Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=DmRXb%2F7p%2B%2BYp%2FcPvJV5jTUzLJGgsjfEyA6PL8Kv4LTo%3D",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP2" / "19357_1.csv"
        ).read_bytes(),
    },
    # F (Azure Blob Storage response)
    # -------------------------------
    (
        "GET",
        "https: //permanentprodu002p003.blob.core.windows.net/data/1d9d844990bc45d6b24432b33a324156/2022/12/31/day/csv/19357.csv?versionid=2023-04-14T13:19:41.2836525Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T15%3A57%3A15Z&ske=2023-04-14T15%3A57%3A15Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=7tMAHyWBldWmECe3fyb%2B9D8RcN9xKNk%2FIJva%2B5vkpW0%3D",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP2" / "19357_2.csv"
        ).read_bytes(),
    },
    # G (Azure Blob Storage response)
    # -------------------------------
    (
        "GET",
        "https: //permanentprodu002p058.blob.core.windows.net/data/1d9d844990bc45d6b24432b33a324156/2023/01/01/day/csv/19358.csv?versionid=2023-04-14T13:19:41.5175166Z&skoid=4b73ab81-cb6b-4de8-934e-cf62e1cc3aa2&sktid=cdf4cf3d-de23-49cf-a9b0-abd2b675f253&skt=2023-04-13T16%3A00%3A41Z&ske=2023-04-14T16%3A00%3A41Z&sks=b&skv=2021-10-04&sv=2021-10-04&spr=https&se=2023-04-14T15%3A27%3A42Z&sr=b&sp=r&sig=YRmhRwUe0Fw40bj2Jh2XMFFtsAKNE6E5FVqK4rbIGhg%3D",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP2" / "19358.csv"
        ).read_bytes(),
    },
}


RESPONSE_CASES = {**RESPONSE_CASES_GENERAL, **RESPONSE_GROUP1, **RESPONSE_GROUP2}
