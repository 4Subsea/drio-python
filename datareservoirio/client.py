import logging
import os
import time
import warnings
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import cache, wraps
from operator import itemgetter
from urllib.parse import urlencode
from uuid import uuid4

import numpy as np
import pandas as pd
import requests
from azure.monitor.opentelemetry import configure_azure_monitor
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_chain,
    wait_fixed,
)
from tqdm.auto import tqdm

from datareservoirio._constants import ENV_VAR_ENABLE_APP_INSIGHTS

from ._logging import log_decorator
from ._utils import function_translation, period_translation
from .globalsettings import environment
from .storage import Storage

log = logging.getLogger(__name__)


@cache
def metric() -> logging.Logger:
    logger = logging.getLogger(__name__ + "_metric_appinsight")
    if os.getenv(ENV_VAR_ENABLE_APP_INSIGHTS) is not None:
        enable_app_insights = os.environ[ENV_VAR_ENABLE_APP_INSIGHTS].lower()
        if enable_app_insights == "true" or enable_app_insights == "1":
            logger.setLevel(logging.DEBUG)
            configure_azure_monitor(
                connection_string=environment._application_insight_connectionstring,
                logger_name=__name__ + "_metric_appinsight",
            )
    return logger


# Default values to push as start/end dates. (Limited by numpy.datetime64)
_END_DEFAULT = 9214646400000000000  # 2262-01-01
_START_DEFAULT = -9214560000000000000  # 1678-01-01

_TIMEOUT_DEAULT = 120

_DEFAULT_MAX_PAGE_SIZE = 30000


class Client:
    """
    DataReservoir.io client for user-friendly interaction.

    Parameters
    ---------
    auth : cls
        An authenticated session that is used in all API calls. Must supply a
        valid bearer token to all API calls.
    cache : bool
        Enable caching (default).
    cache_opt : dict, optional
        Configuration object for controlling the series cache.
        'max_size': max size of cache in megabytes. Default is 1024 MB.
        'cache_root': cache storage location. See documentation for platform
        specific defaults.

    """

    def __init__(self, auth, cache=True, cache_opt=None):
        self._auth_session = auth

        # TODO: Remove after 2023-08-15
        if cache:
            try:
                del cache_opt["format"]
            except (TypeError, KeyError):
                pass
            else:
                warnings.warn(
                    "Support for choosing cache format deprecated. 'format' will be ignored.",
                    FutureWarning,
                )

        self._storage = Storage(self._auth_session, cache=cache, cache_opt=cache_opt)

    def ping(self):
        """
        Test that you have a working connection to DataReservoir.io.

        """
        response = self._auth_session.get(
            environment.api_base_url + "ping", timeout=_TIMEOUT_DEAULT
        )
        response.raise_for_status()
        return response.json()

    @log_decorator("exception")
    def create(self, series=None, wait_on_verification=True):
        """
        Create a new series in DataReservoir.io from a pandas.Series. If no
        data is provided, an empty series is created.

        Parameters
        ----------
        series : pandas.Series, optional
            Series with index (as DatetimeIndex-like or integer array). Default
            is None. Needs to be sorted on index.
        wait_on_verification : bool (optional)
            All series are subjected to a server-side data validation before
            they are made available for consumption; failing validation will
            result in the series being ignored. If True, the method will wait
            for the data validation process to be completed and return the
            outcome, which may be time consuming. If False, the method will NOT
            wait for the outcome and the data will be available when/if the
            validation is successful. The latter is significantly faster, but
            is recommended when the data is "validated" in advance.
            Default is True.

        Returns
        -------
        dict
            The response from DataReservoir.io containing the unique id of the
            newly created series.
        """
        if series is None:
            response = self._auth_session.put(
                environment.api_base_url + f"timeseries/{str(uuid4())}",
                timeout=_TIMEOUT_DEAULT,
            )
            response.raise_for_status()
            return response.json()

        if not series.index.is_monotonic_increasing:
            raise ValueError(
                "Index not sorted. Please sort series on index before creating a timeseries."
            )

        df = self._verify_and_prepare_series(series)

        response_file = self._auth_session.post(
            environment.api_base_url + "files/upload", timeout=_TIMEOUT_DEAULT
        )
        response_file.raise_for_status()
        file_id, target_url = itemgetter("FileId", "Endpoint")(response_file.json())

        commit_request = (
            "POST",
            environment.api_base_url + "files/commit",
            {"json": {"FileId": file_id}, "timeout": _TIMEOUT_DEAULT},
        )
        self._storage.put(df, target_url, commit_request)

        if wait_on_verification:
            status = self._wait_until_file_ready(file_id)
            if status == "Failed":
                return status

        response = self._auth_session.post(
            environment.api_base_url + "timeseries/create",
            data={"FileId": file_id},
            timeout=_TIMEOUT_DEAULT,
        )
        response.raise_for_status()
        return response.json()

    @log_decorator("exception")
    def append(self, series, series_id, wait_on_verification=True):
        """
        Append data to an already existing series.

        Parameters
        ----------
        series : pandas.Series
            Series with index (as DatetimeIndex-like or integer array). Needs to be sorted on index.
        series_id : string
            The identifier of the existing series.
        wait_on_verification : bool (optional)
            All series are subjected to a server-side data validation before
            they are made available for consumption; failing validation will
            result in the series being ignored. If True, the method will wait
            for the data validation process to be completed and return the
            outcome, which may be time consuming. If False, the method will NOT
            wait for the outcome and the data will be available when/if the
            validation is successful. The latter is significantly faster, but
            is recommended when the data is "validated" in advance.
            Default is True.

        Returns
        -------
        dict
            The response from DataReservoir.io.
        """
        if not series.index.is_monotonic_increasing:
            raise ValueError(
                "Index not sorted. Please sort series on index before appending data."
            )
        df = self._verify_and_prepare_series(series)

        response_file = self._auth_session.post(
            environment.api_base_url + "files/upload", timeout=_TIMEOUT_DEAULT
        )
        response_file.raise_for_status()
        file_id, target_url = itemgetter("FileId", "Endpoint")(response_file.json())

        commit_request = (
            "POST",
            environment.api_base_url + "files/commit",
            {"json": {"FileId": file_id}, "timeout": _TIMEOUT_DEAULT},
        )

        self._storage.put(df, target_url, commit_request)

        if wait_on_verification:
            status = self._wait_until_file_ready(file_id)
            if status == "Failed":
                return status

        response = self._auth_session.post(
            environment.api_base_url + "timeseries/add",
            data={"TimeSeriesId": series_id, "FileId": file_id},
            timeout=_TIMEOUT_DEAULT,
        )
        response.raise_for_status()
        return response.json()

    def info(self, series_id):
        """
        Retrieve basic information about a series.

        Returns
        -------
        dict
            Available information about the series. None if series not found.
        """
        response = self._auth_session.get(
            environment.api_base_url + f"timeseries/{series_id}",
            timeout=_TIMEOUT_DEAULT,
        )
        response.raise_for_status()
        return response.json()

    def search(self, namespace, key=None, name=None, value=None):
        """
        Find available series having metadata with given
        namespace + key* (optional) + name (optional) + *value* (optional)
        combination. Note that the arguments are hierarchical, starting from
        the left. If an argument is None, the proceeding ones are also set to
        None. For example, (namespace = “hello”, key=None, name=”Rabbit”, value=”Hole”)
        will have the same effect as (namespace = “hello”, key=None, name=None,
        value=None)

        Parameters
        ----------
        namespace : str
            Full namespace to search for
        key : str, optional
            Key or partial (begins with) key to narrow search.
            Default (None) will include all.
        name: str, optional
            Full name to narrow search further.
            Default (None) will include all.
        value: str, optional
            Value or partial (begins or ends with or both) to narrow search further.
            Default (None) will include all.

        Returns
        -------
        dict or list
            Available information about the series. If ``value`` is passed,
            a plain list with ``TimeSeriesId`` is returned. Otherwise, a dict is
            returned -> ``{TimeSeriesId: metadata}``.

        """
        args = [namespace, key, name, value]
        if None in args:
            none_count = args.count(None)
            if args[-none_count:].count(None) < none_count:
                warnings.warn(
                    "Warning: You have provided argument(s) following a None argument, they are ignored by the search!"
                )
            args = args[: args.index(None)]

        response = self._auth_session.get(
            environment.api_base_url + f"timeseries/search/{'/'.join(args)}",
            timeout=_TIMEOUT_DEAULT,
        )
        response.raise_for_status()
        return response.json()

    @log_decorator("exception")
    def delete(self, series_id):
        """
        Delete a series from DataReservoir.io.

        Parameters
        ----------
        series_id : string
            The id of the series to delete.

        """
        return self._auth_session.delete(
            environment.api_base_url + f"timeseries/{series_id}",
            timeout=_TIMEOUT_DEAULT,
        )

    def _timer(func):
        """Decorator used to log latency of the ``get`` and ``get_samples_aggregate`` method"""

        @wraps(func)
        def wrapper(self, series_id, start=None, end=None, **kwargs):
            start_time = time.perf_counter()
            result = func(self, series_id, start=start, end=end, **kwargs)
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            start_date_as_str = None
            end_date_as_str = None
            if start:
                start_date_as_str = pd.to_datetime(
                    start, dayfirst=True, unit="ns", utc=True
                ).isoformat()
            if end:
                end_date_as_str = pd.to_datetime(
                    end, dayfirst=True, unit="ns", utc=True
                ).isoformat()
            number_of_samples = len(result)
            properties = {
                "series_id": series_id,
                "start": start_date_as_str,
                "end": end_date_as_str,
                "elapsed": elapsed_time,
                "number-of-samples": number_of_samples,
            }
            metric().info("Timer", extra=properties)
            return result

        return wrapper

    @log_decorator("exception")
    @_timer
    @retry(
        stop=stop_after_attempt(
            4
        ),  # Attempt!, not retry attempt. Attempt 2, is 1 retry
        retry=retry_if_exception_type(
            (
                ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.ReadTimeout,
                ConnectionRefusedError,
                requests.ConnectionError,
            )
        ),
        wait=wait_chain(*[wait_fixed(0.1), wait_fixed(0.5), wait_fixed(30)]),
    )
    @log_decorator("warning")
    def get(
        self,
        series_id,
        start=None,
        end=None,
        convert_date=True,
        raise_empty=False,
    ):
        """
        Retrieve a series from DataReservoir.io.

        Parameters
        ----------
        series_id : str
            Identifier of the series to download
        start : optional
            start time (inclusive) of the series given as anything
            pandas.to_datetime is able to parse.
        end : optional
            stop time (exclusive) of the series given as anything
            pandas.to_datetime is able to parse.
        convert_date : bool
            If True (default), the index is converted to DatetimeIndex.
            If False, index is returned as ascending integers.
        raise_empty : bool
            If True, raise ValueError if no data exist in the provided
            interval. Otherwise, return an empty pandas.Series (default).

        Returns
        -------
        pandas.Series
            Series data
        """
        if not start:
            start = _START_DEFAULT
        if not end:
            end = _END_DEFAULT

        start = pd.to_datetime(start, dayfirst=True, unit="ns", utc=True).value
        end = pd.to_datetime(end, dayfirst=True, unit="ns", utc=True).value - 1

        if start >= end:
            raise ValueError("start must be before end")

        response = self._auth_session.get(
            environment.api_base_url
            + f"timeseries/{series_id}/data/days?start={start}&end={end}",
            timeout=_TIMEOUT_DEAULT,
        )
        if response.status_code == 504:
            raise TimeoutError(
                "Gateway Timeout. Try downloading data in smaller batches, preferably with a daily interval. See documentation for guidance: https://docs.4insight.io/dataanalytics/reservoir/python/latest/user_guide/dos_donts.html."
            )
        response.raise_for_status()
        response_json = response.json()

        if response_json["Files"]:
            with ThreadPoolExecutor(max_workers=None) as e:
                futures = [
                    e.submit(self._storage.get, blob_sequence_i)
                    for _, blob_sequence_i in sorted(
                        _blob_sequence_days(response_json).items()
                    )
                ]
            df = pd.concat([future_i.result() for future_i in futures])
        else:
            df = pd.DataFrame(columns=("index", "values")).astype({"index": "int64"})

        try:
            series = (
                df.set_index("index").squeeze("columns").loc[start:end].copy(deep=True)
            )
        except KeyError as e:
            logging.warning(
                "The time series you requested is not properly ordered. The data will be sorted to attempt to resolve the issue. Please note that this operation may take some time."
            )
            series = (
                df.set_index("index")
                .sort_index()
                .squeeze("columns")
                .loc[start:end]
                .copy(deep=True)
            )
        series.index.name = None

        if series.empty and raise_empty:  # may become empty after slicing
            raise ValueError("can't find data in the given interval")

        if convert_date:
            series.index = pd.to_datetime(series.index, utc=True)

        return series

    @log_decorator("exception")
    @_timer
    @log_decorator("warning")
    def get_samples_aggregate(
        self,
        series_id,
        start=None,
        end=None,
        aggregation_period=None,
        aggregation_function=None,
        max_page_size=_DEFAULT_MAX_PAGE_SIZE,
    ):
        """
        Retrieve a series from DataReservoir.io using the samples/aggregate endpoint.

        Parameters
        ----------
        series_id : str
            Identifier of the series to download
        start: required
            Start time (inclusive) of the aggregated series given as anything
            pandas.to_datetime is able to parse. Date must be within the past 90 days.
        end:
            Stop time (exclusive) of the aggregated series given as anything
            pandas.to_datetime is able to parse. Date must be within the past 90 days.
        aggregation_function : str
            One of "mean", "min", "max", "std".
        aggregation_period : str
            Used in combination with aggregation function to specify the period for aggregation.
            Aggregation period is maximum 24 hours. Values can be in units of h, m, s, ms,
            microsecond or tick. Use 100 ms instead of 0.1s for 10Hz.
        max_page_size : optional
            Maximum number of samples to return per page. The method automatically follows links
            to next pages and returns the entire series. For advanced usage.
        Returns
        -------
        pandas.Series
            Series data
        """
        if not start:
            # Required parameter
            raise ValueError(
                "You must specify the start date in ISO 8601 format, for example 2023-12-01"
            )

        if not end:
            # Required parameter
            raise ValueError(
                "You must specify the end date in ISO 8601 format, for example 2023-12-31."
            )

        if not aggregation_period:
            # Required parameter
            raise ValueError(
                "Aggregation period must be specified using integers and one of these units: h, m, s, ms, microsecond or tick, or their Pandas equivalents"
            )

        if not aggregation_function:
            # Required parameter
            raise ValueError(
                "Aggregation function must be one of: Avg (mean), Min, Max, Stdev (std)"
            )

        # Translating some pandas terms to API terms
        # Note the API is case insensitive so both min and Min will work
        if aggregation_function in function_translation:
            aggregation_function = function_translation[aggregation_function]

        if not aggregation_period[0].isnumeric():
            aggregation_period = "1" + aggregation_period

        for period_unit in period_translation:
            if (
                aggregation_period.endswith(period_unit)
                and aggregation_period[-len(period_unit) - 1].isnumeric()
            ):
                aggregation_period = (
                    aggregation_period[: -len(period_unit)]
                    + period_translation[period_unit]
                )
                break

        start = pd.to_datetime(start, dayfirst=True, unit="ns", utc=True)
        end = pd.to_datetime(end, dayfirst=True, unit="ns", utc=True)

        if start.value >= end.value:
            raise ValueError("Start must be before end.")

        params = {}

        params["maxPageSize"] = max_page_size
        params["aggregationPeriod"] = aggregation_period
        params["aggregationFunction"] = aggregation_function
        params["start"] = start.isoformat()
        params["end"] = end.isoformat()

        next_page_link = f"{environment.api_base_url}reservoir/timeseries/{series_id}/samples/aggregate?{urlencode(params)}"

        df = (
            pd.DataFrame(columns=("index", "values"))
            .astype({"index": "int64"})
            .astype({"values": "float64"}, errors="ignore")
        )

        @retry(
            stop=stop_after_attempt(
                4
            ),  # Attempt!, not retry attempt. Attempt 2, is 1 retry
            retry=retry_if_exception_type(
                (
                    ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                    requests.ReadTimeout,
                    ConnectionRefusedError,
                    requests.ConnectionError,
                )
            ),
            wait=wait_chain(*[wait_fixed(0.1), wait_fixed(0.5), wait_fixed(30)]),
        )
        def get_samples_aggregate_page(url):
            return self._auth_session.get(
                url,
                timeout=_TIMEOUT_DEAULT,
            )

        if log.getEffectiveLevel() < logging.WARNING:
            progress_bar = tqdm(unit=" pages", desc="Downloading aggregate data")

        while next_page_link:
            response = get_samples_aggregate_page(next_page_link)
            if response.status_code == 504:
                raise TimeoutError(
                    "Gateway Timeout. Try downloading data in smaller batches, preferably with a daily interval. See documentation for guidance: https://docs.4insight.io/dataanalytics/reservoir/python/latest/user_guide/dos_donts.html."
                )
            response.raise_for_status()
            response_json = response.json()
            next_page_link = response_json.get("@odata.nextLink", None)

            content = [
                (
                    pd.to_datetime(sample["Timestamp"], unit="ns", utc=True),
                    sample["Value"],
                )
                for sample in response_json["value"]
            ]

            # update the progress bar
            if content and log.getEffectiveLevel() < logging.WARNING:
                progress_bar.update(1)

            new_df = pd.DataFrame(
                content, columns=("index", "values"), copy=False
            ).astype({"values": "float64"}, errors="ignore")

            df = pd.concat([df, new_df])
        if log.getEffectiveLevel() < logging.WARNING:
            progress_bar.close()
        series = (
            df.infer_objects().set_index("index").squeeze("columns").copy(deep=True)
        )

        return series

    def set_metadata(
        self,
        series_id,
        metadata_id=None,
        namespace=None,
        key=None,
        overwrite=False,
        **namevalues,
    ):
        """
        Set metadata entries on a series. Metadata can be set from existing
        values or new metadata can be created.

        Parameters
        ----------
        series_id : str
            The identifier of the existing series
        metadata_id : str, optional
            The identifier of the existing metadata entries. If passed, other
            metadata related arguments are ignored.
        namespace : str, optional
            Metadata namespace.
        key : str, mandatory if namespace is passed.
            Metadata key.
        overwrite: bool, optional
            If true, and namespace+key corresponds to existing metadata, the
            value of the metadata will be overwritten. If false, a ValueError
            will be raised if the metadata already exist.
        namevalues : keyword arguments
            Metadata name-value pairs

        Return
        ------
        dict
            response.json()
        """
        if not metadata_id and not namespace:
            raise ValueError("one of metadata_id or namespace is mandatory")
        elif not metadata_id and namespace:
            if not key:
                raise ValueError("key is mandatory when namespace is passed")
            try:
                response_create = self._auth_session.put(
                    environment.api_base_url
                    + f"metadata/{namespace}/{key}?overwrite={'true' if overwrite else 'false'}",
                    json={"Value": namevalues},
                    timeout=_TIMEOUT_DEAULT,
                )
                response_create.raise_for_status()
                metadata_id = response_create.json()["Id"]
            except requests.exceptions.HTTPError as err:
                if err.response.status_code == 409:
                    raise ValueError(
                        "Metadata already exist. Specify overwrite=True to"
                        "confirm overwriting the metadata."
                    )
                else:
                    raise

        response = self._auth_session.put(
            environment.api_base_url + f"timeseries/{series_id}/metadata",
            json=[metadata_id],
            timeout=_TIMEOUT_DEAULT,
        )
        response.raise_for_status()
        return response.json()

    def remove_metadata(self, series_id, metadata_id):
        """
        Remove a metadata entry from a series. Note that metadata entries are
        not deleted, but the link between series and metadata is broken.

        Parameters
        ----------
        series_id : str
            The identifier of the existing series
        metadata_id : str
            The identifier of the existing metadata entry.

        Return
        ------
        dict
            response.json()

        """
        response = self._auth_session.delete(
            environment.api_base_url + f"timeseries/{series_id}/metadata",
            json=[metadata_id],
            timeout=_TIMEOUT_DEAULT,
        )
        response.raise_for_status()
        return response.json()

    def metadata_set(self, namespace, key, **namevalues):
        """
        Create or update a metadata entry. If the namespace/key combination
        does not already exist, a new entry will be created. If the combination
        already exist, the entry will be updated with the specified namevalues.

        Parameters
        ----------
        namespace : str
            Metadata namespace
        key : str
            Metadata key
        namevalues : keyword arguments
            Metadata name-value pairs

        Returns
        -------
        dict
            The response from DataReservoir.io containing the unique id of the
            new or updated metadata.
        """
        response = self._auth_session.put(
            environment.api_base_url + f"metadata/{namespace}/{key}?overwrite=true",
            json={"Value": namevalues},
            timeout=_TIMEOUT_DEAULT,
        )
        response.raise_for_status()
        return response.json()

    def metadata_get(self, metadata_id=None, namespace=None, key=None):
        """
        Retrieve a metadata entry. Required input is either metatdata_id, or
        namespace + key, i.e. metadata_get(my_metadata_id) or metadata_get(my_namespace, my_key)

        Parameters
        ----------
        metadata_id : str
            The identifier of existing metadata
        namespace : str
            Metadata namespace. Ignored if metadata_id is set.
        key : str
            Metadata key. Ignored if metadata_id is set.

        Returns
        -------
        dict
            Metadata entry.
        """
        if metadata_id:
            uri_postfix = f"metadata/{metadata_id}"
        elif namespace and key:
            uri_postfix = f"metadata/{namespace}/{key}"
        else:
            raise ValueError(
                "Missing required input: either (metadata_id) or (namespace, key)"
            )

        response = self._auth_session.get(
            environment.api_base_url + uri_postfix, timeout=_TIMEOUT_DEAULT
        )
        response.raise_for_status()
        return response.json()

    def metadata_browse(self, namespace=None):
        """
        List available metadata namespaces and keys. If namespace is None, a list
        of all available namespaces is returned. If namespace is specified,
        a list of all available keys for that namespace is returned.

        Parameters
        ----------
        namespace : string
            The namespace to search in (exact match)

        Returns
        -------
        list
            The namespaces or keys found.
        """

        if not namespace:
            uri_postfix = "metadata/"
        else:
            uri_postfix = f"metadata/{namespace}"

        response = self._auth_session.get(
            environment.api_base_url + uri_postfix, timeout=_TIMEOUT_DEAULT
        )
        response.raise_for_status()
        return sorted(response.json())

    def metadata_search(self, namespace, key):
        """
        Find metadata entries given namespace/key combination.

        namespace : string
            The namespace to search in
        key : string
            The key to narrow search. Supports "begins with" specification,
            i.e. will look for matches with "key + wildcard"

        Returns
        -------
        list
            Metadata entries that matches the search.

        """
        response = self._auth_session.post(
            environment.api_base_url + "metadata/search",
            json={"Namespace": namespace, "Key": key, "Value": {}},
            timeout=_TIMEOUT_DEAULT,
        )
        response.raise_for_status()
        return response.json()

    def metadata_delete(self, metadata_id):
        """
        Delete an existing metadata entry.

        Parameters
        ----------
        metadata_id : str
            id of metadata
        """
        response = self._auth_session.delete(
            environment.api_base_url + f"metadata/{metadata_id}",
            timeout=_TIMEOUT_DEAULT,
        )
        response.raise_for_status()
        return

    def _verify_and_prepare_series(self, series):
        if not isinstance(series, pd.Series):
            raise ValueError("series must be a pandas Series")

        if not (
            pd.api.types.is_datetime64_ns_dtype(series.index)
            or series.index.dtype == np.int64
        ):
            raise ValueError("allowed dtypes are datetime64[ns] and int64")

        if not series.index.is_unique:
            raise ValueError("index values must be unique timestamps")

        df = series.to_frame(name=1).reset_index(names=0)
        df[0] = df[0].astype("int64")

        return df

    def _wait_until_file_ready(self, file_id):
        # wait for server side processing
        while True:
            status = self._get_file_status(file_id)
            log.debug(f"status is {status}")

            if status == "Ready":
                return "Ready"
            elif status == "Failed":
                return "Failed"

            time.sleep(5)

    def _get_file_status(self, file_id):
        response = self._auth_session.get(
            environment.api_base_url + f"files/{file_id}/status",
            timeout=_TIMEOUT_DEAULT,
        )
        response.raise_for_status()
        return response.json()["State"]


def _blob_sequence_days(response_json):
    """
    Returns blob sequences grouped by days and sorted by 'Files'.

    Parameters
    ----------
    response_json : dict
        TimeSeries API JSON response.
    """

    blob_sequences = defaultdict(list)
    for file_i in response_json["Files"]:
        for chunk_i in file_i["Chunks"]:
            blob_sequences[chunk_i["DaysSinceEpoch"]].append(
                {
                    "Path": chunk_i["Path"],
                    "Endpoint": chunk_i["Endpoint"],
                    "ContentMd5": chunk_i["ContentMd5"],
                }
            )

    return dict(blob_sequences)
