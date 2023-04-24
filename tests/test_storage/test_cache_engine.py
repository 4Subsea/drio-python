import shutil
from collections.abc import MutableMapping
from pathlib import Path

import pandas as pd
import pytest

from datareservoirio.storage.cache_engine import CacheIO, _CacheIndex

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
    def test__init__(self):
        CACHE_ROOT = TEST_PATH.parent / "testdata" / "RESPONSE_GROUP2" / "cache"
        CACHE_PATH = CACHE_ROOT / "v3"

        cache_index = _CacheIndex(CACHE_PATH, 1024)

        assert isinstance(cache_index, MutableMapping)
