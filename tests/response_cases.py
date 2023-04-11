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

TEST_PATH = Path(__file__).parent


RESPONSE_CASES_GENERAL = {
    # description: blob (numeric) from remote storage
    ("GET", "http://example/drio/blob/file"): {
        "_content": (
            TEST_PATH
            / "testdata"
            / "RESPONSE_CASES_GENERAL"
            / "example_drio_blob_file.csv"
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
}


RESPONSE_GROUP1 = {
    # =========================================================================
    # This 'group' of response cases represents the DataReservoir.io backend response
    # and Azure Blob Storage responses when requesting data for a timeries with:
    #   * ID = "2fee7f8a-664a-41c9-9b71-25090517c275"
    #   * start = 1672358400000000000
    #   * end = 1672703939999999999
    #
    # The responses are:
    #   * A: TimeSeries API response, containing a list of 'chunks'.
    #   * B: Azure Blob Storage response for the first chunk of data.
    #   * C: Azure Blob Storage response for the second chunk of data.
    #   * Etc.
    #
    # These response cases will mock the backend for the following `get` call with
    # the `datareservoirio` client:
    #
    #   client.get(
    #       "2fee7f8a-664a-41c9-9b71-25090517c275",
    #       start=1672358400000000000,
    #       end=1672703939999999999
    #   )
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
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "TimeSeries_API_output.json"
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
    # F (TimeSeries API response for day 1672358400000000000)
    # ---------------------------
    (
        "GET",
        "https://reservoir-api.4subsea.net/api/timeseries/2fee7f8a-664a-41c9-9b71-25090517c275/data/days?start=1672358400000000000&end=1672358400000000000",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "TimeSeries_API_output_1672358400000000000.json"
        ).read_bytes(),
    },
    # G (TimeSeries API response for day 1672444800000000000)
    # ---------------------------
    (
        "GET",
        "https://reservoir-api.4subsea.net/api/timeseries/2fee7f8a-664a-41c9-9b71-25090517c275/data/days?start=1672444800000000000&end=1672444800000000000",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "TimeSeries_API_output_1672444800000000000.json"
        ).read_bytes(),
    },
    # H (TimeSeries API response for day 1672531200000000000)
    # ---------------------------
    (
        "GET",
        "https://reservoir-api.4subsea.net/api/timeseries/2fee7f8a-664a-41c9-9b71-25090517c275/data/days?start=1672531200000000000&end=1672531200000000000",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "TimeSeries_API_output_1672531200000000000.json"
        ).read_bytes(),
    },
    # I (TimeSeries API response for day 1672617600000000000)
    # ---------------------------
    (
        "GET",
        "https://reservoir-api.4subsea.net/api/timeseries/2fee7f8a-664a-41c9-9b71-25090517c275/data/days?start=1672617600000000000&end=1672617600000000000",
    ): {
        "status_code": 200,
        "reason": "OK",
        "_content": (
            TEST_PATH / "testdata" / "RESPONSE_GROUP1" / "TimeSeries_API_output_1672617600000000000.json"
        ).read_bytes(),
    },
}


RESPONSE_CASES = {**RESPONSE_CASES_GENERAL, **RESPONSE_GROUP1}
