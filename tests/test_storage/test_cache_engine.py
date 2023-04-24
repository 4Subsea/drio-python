import re
import shutil
from collections.abc import MutableMapping
from pathlib import Path

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
        return _CacheIndex(CACHE_PATH, 1024)

    def test__init__(self):
        CACHE_ROOT = TEST_PATH.parent / "testdata" / "RESPONSE_GROUP2" / "cache"
        CACHE_PATH = CACHE_ROOT / "v3"

        cache_index = _CacheIndex(CACHE_PATH, 1024)

        assert isinstance(cache_index, MutableMapping)

    # def test_exists(self, cache_index):
    #     id_ = "parquet03fc12505d3d41fea77df405b2563e4920221230daycsv19356csv"
    #     md5 = "fJ85MDJqsTW6zDJbd+Fa4A=="
    #     assert cache_index.exists(id_, md5) is True

    def test_exists(self, cache_index):
        path = "03fc12505d3d41fea77df405b2563e49/2022/12/30/day/csv/19356.csv"
        md5 = "fJ85MDJqsTW6zDJbd+Fa4A=="

        id_ = "parquet" + re.sub(r"-|_|/|\.", "", path)
        md5 = _encode_for_path_safety(md5)

        assert cache_index.exists(id_, md5) is True
