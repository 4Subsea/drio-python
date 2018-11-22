import unittest
import pandas as pd

from datareservoirio.storage import CachedDownloadStrategy, SimpleFileCache
from datareservoirio.storage.downloadstrategy import BaseDownloadStrategy

try:
    from unittest.mock import patch, Mock
except ImportError:
    from mock import patch, Mock


class Test_CachedDownloadStrategy(unittest.TestCase):
    def setUp(self):
        self._blob_to_series_patch = patch(
            'datareservoirio.storage.CachedDownloadStrategy._blob_to_series')
        self._blob_to_series = self._blob_to_series_patch.start()
        self.addCleanup(self._unpatch)
        self._cache = Mock()

        self._df = pd.DataFrame(
            data=[{'1': 42}, {'2': 32}], columns=['index', 'values'])
        self._files = {
            'Files': [
                {
                    'Index': 0,
                    'FileId': '42',
                    'Chunks': [
                        {
                            'Account': 'acc',
                            'SasKey': 'sas',
                            'Container': 'cnt',
                            'Path': 'pth',
                            'Endpoint': 'ep',
                            'ContentMd5': 'md5/+hash=='
                        }
                    ]
                }
            ]
        }

        self._files_empty = {'Files': []}

        self._blob_to_series.return_value = self._df
        self._cache.get.return_value = self._df

        self._dl = CachedDownloadStrategy(cache=self._cache)

    def _unpatch(self):
        self._blob_to_series_patch.stop()

    def test_init_with_invalid_format_raises_exception(self):
        with self.assertRaises(ValueError):
            CachedDownloadStrategy(self._cache, format='bogusformat')

    def test_init_with_cache(self):
        dl = CachedDownloadStrategy(cache=self._cache)
        self.assertIsInstance(dl._cache, Mock)

    def test_init_default_serialization_is_msgpack(self):
        dl = CachedDownloadStrategy(cache=self._cache)

        dl.get(self._files)

        calls = self._cache.get.call_args[0]
        self.assertIn('bWQ1LytoYXNoPT0=', calls)
        self.assertIn('mp', calls)

    def test_init_msgpack_serialization_md5file_have_mp_postfix(self):
        dl = CachedDownloadStrategy(cache=self._cache, format='msgpack')

        dl.get(self._files)

        calls = self._cache.get.call_args[0]
        self.assertIn('bWQ1LytoYXNoPT0=', calls)
        self.assertIn('mp', calls)

    def test_init_csv_serialization_md5file_have_csv_postfix(self):
        dl = CachedDownloadStrategy(cache=self._cache, format='csv')

        dl.get(self._files)

        calls = self._cache.get.call_args[0]
        self.assertIn('bWQ1LytoYXNoPT0=', calls)
        self.assertIn('csv', calls)

    def test_get(self):
        sr = self._dl.get(self._files)

        self.assertTrue(sr.equals(self._df))

    def test_get_empty(self):
        sr = self._dl.get(self._files_empty)

        self.assertTrue(sr.equals(pd.Series()))


class FakedDownloadStrategy(BaseDownloadStrategy):
    def _download_chunk(self, chunk):
        return 'dummy'


class Test_BaseDownloadStrategy(unittest.TestCase):

    def test__combine_first_no_overlap(self):
        df1 = pd.Series([0., 1., 2., 3.], index=[0, 1, 2, 3])
        df2 = pd.Series([10., 11., 12., 13.], index=[6, 7, 8, 9])
        df_expected = df1.combine_first(df2)
        df_out = BaseDownloadStrategy._combine_first(df1, df2)
        pd.testing.assert_series_equal(df_expected, df_out)

    def test__combine_first_no_overlap_reversed_order(self):
        df2 = pd.Series([0., 1., 2., 3.], index=[0, 1, 2, 3])
        df1 = pd.Series([10., 11., 12., 13.], index=[6, 7, 8, 9])
        df_expected = df1.combine_first(df2)
        df_out = BaseDownloadStrategy._combine_first(df1, df2)
        pd.testing.assert_series_equal(df_expected, df_out)

    def test__combine_first_exact_overlap(self):
        df1 = pd.Series([0., 1., 2., 3.], index=[0, 1, 2, 3])
        df2 = pd.Series([10., 11., 12., 13.], index=[0, 1, 2, 3])
        df_expected = df1.combine_first(df2)
        df_out = BaseDownloadStrategy._combine_first(df1, df2)
        pd.testing.assert_series_equal(df_expected, df_out)

    def test__combine_first_partial_overlap(self):
        df1 = pd.Series([0., 1., 2., 3.], index=[0, 1, 2, 3])
        df2 = pd.Series([10., 11., 12., 13.], index=[2, 3, 4, 5])
        df_expected = df1.combine_first(df2)
        df_out = BaseDownloadStrategy._combine_first(df1, df2)
        pd.testing.assert_series_equal(df_expected, df_out)

    def test__get_chunks_hash(self):
        response = {'Files': [
            {'Chunks': [
                {'ContentMd5': 'abc123'},
                {'ContentMd5': 'def456'}]},
            {'Chunks': [
                {'ContentMd5': 'ghi789'}]}
            ]}
        hash_out = BaseDownloadStrategy._get_chunks_hash(response)
        hash_expected = hash('abc123def456ghi789')
        self.assertEqual(hash_out, hash_expected)

    def test__download_verified_chunk(self):
        chunk = {}
        df = pd.Series([0., 1., 2., 3.1, 3.2, 3.3, 4.], index=[0, 1, 2, 3, 3, 3, 4])
        df_expected = pd.Series([0., 1., 2., 3.3, 4.], index=[0, 1, 2, 3, 4])

        ds = FakedDownloadStrategy()
        ds._download_chunk = Mock(name='_download_chunk')
        ds._download_chunk.return_value = df

        df_out = ds._download_verified_chunk(chunk)

        pd.testing.assert_series_equal(df_expected, df_out)


if __name__ == '__main__':
    unittest.main()
