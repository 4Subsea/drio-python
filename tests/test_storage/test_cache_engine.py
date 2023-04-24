import shutil
from collections.abc import MutableMapping
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from datareservoirio.storage.cache_engine import CacheIO, _CacheIndex
from datareservoirio.storage.storage import _encode_for_path_safety

TEST_PATH = Path(__file__).parent


class Test_CacheIO:
    @pytest.mark.parametrize("data", ("data_float", "data_string"))
    def test__write(self, request, data, tmp_path):
        data = request.getfixturevalue(data)

        filepath = tmp_path / "foobar.parquet"

        assert not filepath.exists()

        df = data.as_dataframe()
        CacheIO._write(df, str(filepath))

        assert filepath.exists()
        df_from_file = pd.read_parquet(filepath)
        pd.testing.assert_frame_equal(df_from_file, df)

    @pytest.mark.parametrize(
        "filename, data",
        [("data_float.parquet", "data_float"), ("data_string.parquet", "data_string")],
    )
    def test__read(self, request, filename, data):
        data = request.getfixturevalue(data)

        path = TEST_PATH.parent / "testdata" / filename
        df_out = CacheIO._read(path)

        df_expect = data.as_dataframe()
        pd.testing.assert_frame_equal(df_out, df_expect)

    @pytest.mark.parametrize("filename", ("data_float.parquet", "data_string.parquet"))
    def terst__delete(self, filename, tmp_path):
        # Copy file to temporary folder (so that we can test deleting it)
        src_path = TEST_PATH.parent / "testdata" / filename
        dst_path = tmp_path / filename
        shutil.copyfile(src_path, dst_path)

        filepath = dst_path
        assert filepath.exists()
        CacheIO._delete(filepath)
        assert not filepath.exists()


class Test__CacheIndex:
    @pytest.fixture
    def cache_index(self):
        CACHE_ROOT = TEST_PATH.parent / "testdata" / "RESPONSE_GROUP2" / "cache"
        CACHE_PATH = CACHE_ROOT / "v3"
        max_size_mb = 1025
        max_size_b = max_size_mb * 1024 * 1024  # MB to B
        return _CacheIndex(CACHE_PATH, max_size_b)

    def test__init__(self):
        CACHE_ROOT = TEST_PATH.parent / "testdata" / "RESPONSE_GROUP2" / "cache"
        CACHE_PATH = CACHE_ROOT / "v3"

        cache_index = _CacheIndex(CACHE_PATH, 1024)

        assert isinstance(cache_index, MutableMapping)
        assert cache_index._current_size == 690851

        key = "parquet03fc12505d3d41fea77df405b2563e4920221231daycsv19357csv_d1haRlV6akM2U0lzMDlPcWt0dFpXUT09"
        assert set(cache_index[key].keys()) == {"id", "md5", "size", "time"}
        assert cache_index[key]["md5"] == "d1haRlV6akM2U0lzMDlPcWt0dFpXUT09"
        assert cache_index[key]["size"] == 81265
        assert (
            cache_index[key]["id"]
            == "parquet03fc12505d3d41fea77df405b2563e4920221231daycsv19357csv"
        )

    @pytest.mark.parametrize(
        "path, md5",
        [
            (
                "03fc12505d3d41fea77df405b2563e49/2022/12/30/day/csv/19356.csv",
                "fJ85MDJqsTW6zDJbd+Fa4A==",
            ),
            (
                "03fc12505d3d41fea77df405b2563e49/2022/12/31/day/csv/19357.csv",
                "wXZFUzjC6SIs09OqkttZWQ==",
            ),
            (
                "629504a5fe3449049370049874b69fe0/2022/12/30/day/csv/19356.csv",
                "JQAxeHMZ69WSsEuanKMJHA==",
            ),
            (
                "629504a5fe3449049370049874b69fe0/2022/12/31/day/csv/19357.csv",
                "n5FdtLw0noj575zc0gilog==",
            ),
            (
                "1d9d844990bc45d6b24432b33a324156/2022/12/31/day/csv/19357.csv",
                "c4cRzdbJCUkYa1JkprsUWw==",
            ),
            (
                "1d9d844990bc45d6b24432b33a324156/2023/01/01/day/csv/19358.csv",
                "6BmmWa7uXis3+xZdR2tVwg==",
            ),
        ],
    )
    def test_exists(self, cache_index, path, md5):
        id_ = "parquet" + path.replace("/", "").replace(".", "")
        md5 = _encode_for_path_safety(md5)
        assert cache_index.exists(id_, md5) is True
        assert len(cache_index) == 6

    def test_no_exists(self, cache_index):
        path = "foo/bar/baz.csv"
        md5 = "6BmmWa7uXis3+xZdR2tVwg=="
        id_ = "parquet" + path.replace("/", "").replace(".", "")
        md5 = _encode_for_path_safety(md5)
        assert cache_index.exists(id_, md5) is False

    @pytest.mark.parametrize(
        "path, md5",
        [
            (
                "03fc12505d3d41fea77df405b2563e49/2022/12/30/day/csv/19356.csv",
                "fJ85MDJqsTW6zDJbd+Fa4A==",
            ),
            (
                "03fc12505d3d41fea77df405b2563e49/2022/12/31/day/csv/19357.csv",
                "wXZFUzjC6SIs09OqkttZWQ==",
            ),
            (
                "629504a5fe3449049370049874b69fe0/2022/12/30/day/csv/19356.csv",
                "JQAxeHMZ69WSsEuanKMJHA==",
            ),
            (
                "629504a5fe3449049370049874b69fe0/2022/12/31/day/csv/19357.csv",
                "n5FdtLw0noj575zc0gilog==",
            ),
            (
                "1d9d844990bc45d6b24432b33a324156/2022/12/31/day/csv/19357.csv",
                "c4cRzdbJCUkYa1JkprsUWw==",
            ),
            (
                "1d9d844990bc45d6b24432b33a324156/2023/01/01/day/csv/19358.csv",
                "6BmmWa7uXis3+xZdR2tVwg==",
            ),
        ],
    )
    def test_exists2(self, path, md5, tmp_path):
        SRC_CACHE_PATH = (
            TEST_PATH.parent / "testdata" / "RESPONSE_GROUP2" / "cache" / "v3"
        )

        CACHE_PATH = tmp_path
        cache_index = _CacheIndex(CACHE_PATH, 1024)

        id_ = "parquet" + path.replace("/", "").replace(".", "")
        md5 = _encode_for_path_safety(md5)
        filename = id_ + "_" + md5

        assert cache_index.exists(id_, md5) is False
        assert len(cache_index) == 0

        shutil.copyfile(SRC_CACHE_PATH / filename, CACHE_PATH / filename)
        assert cache_index.exists(id_, md5) is True
        assert len(cache_index) == 1

    def test_touch(self, cache_index):
        keys_list_before_touch = list(cache_index.keys())

        # Mark the 'last used' entry as 'recently used'
        idx_last_used = 0
        key = keys_list_before_touch[idx_last_used]
        id_, md5 = key.split("_")
        cache_index.touch(id_, md5)

        keys_list_after_touch = list(cache_index.keys())

        assert keys_list_after_touch[-1] == keys_list_before_touch[0]
        assert keys_list_after_touch[0] == keys_list_before_touch[1]

    def test_less_than_max(self, cache_index):
        assert cache_index.size_less_than_max is True

        # Lower the `max_size`, and check that size becomes greater than max
        cache_index._max_size = 1024
        assert cache_index.size_less_than_max is False

    def test_size(self, cache_index):
        assert cache_index.size == 690851

    def test__update_size(self, cache_index):
        sum_ = 0
        for _, val_i in cache_index.items():
            size_update_i = np.random.randint(0, 100)
            val_i["size"] = size_update_i  # update size with random values
            sum_ += size_update_i

        assert cache_index.size == 690851  # old size
        cache_index._update_size()
        assert cache_index.size == sum_  # updated size

    def test_popitem(self, cache_index):
        id_out, item_out = cache_index.popitem()

        id_expect = "parquet03fc12505d3d41fea77df405b2563e4920221230daycsv19356csv"
        item_expect = {
            "id": "parquet03fc12505d3d41fea77df405b2563e4920221230daycsv19356csv",
            "md5": "Zko4NU1ESnFzVFc2ekRKYmQrRmE0QT09",
            "size": 162347,
        }

        assert id_out == id_expect
        item_out.items() >= item_expect.items()  # check is subset since 'time' is not known

    def test__key(self, cache_index):
        id_ = "parquet03fc12505d3d41fea77df405b2563e4920221230daycsv19356csv"
        md5 = "Zko4NU1ESnFzVFc2ekRKYmQrRmE0QT09"
        key_out = cache_index._key(id_, md5)

        key_expect = "parquet03fc12505d3d41fea77df405b2563e4920221230daycsv19356csv_Zko4NU1ESnFzVFc2ekRKYmQrRmE0QT09"
        assert key_out == key_expect
