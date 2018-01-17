import unittest
import logging
import os
import sys

from datareservoirio.storage import SimpleFileCache

try:
    from unittest.mock import patch, Mock, MagicMock
except:
    from mock import patch, Mock, MagicMock


class Test_SimpleFileCache(unittest.TestCase):
    def setUp(self):
        self._io_open_patch = patch('datareservoirio.storage.simplefilecache.io.open')
        self._gzip_open_patch = patch('datareservoirio.storage.simplefilecache.gzip.open')
        self._path_exists_patch = patch('datareservoirio.storage.simplefilecache.os.path.exists')
        self._makedirs_patch = patch('datareservoirio.storage.simplefilecache.os.makedirs')
        self._rename_patch = patch('datareservoirio.storage.simplefilecache.os.rename')
        self._environ_patch = patch.dict('datareservoirio.storage.simplefilecache.os.environ', {'LOCALAPPDATA': 'app'})
        self._walk_patch = patch('datareservoirio.storage.simplefilecache.os.walk')
        self._path_getsize_patch = patch('datareservoirio.storage.simplefilecache.os.path.getsize')
        self._path_getmtime_patch = patch('datareservoirio.storage.simplefilecache.os.path.getmtime')
        self._remove_patch = patch('datareservoirio.storage.simplefilecache.os.remove')
        self._rmtree_patch = patch('datareservoirio.storage.simplefilecache.shutil.rmtree')

        self._io_open = self._io_open_patch.start()
        self._gzip_open = self._gzip_open_patch.start()
        self._path_exists = self._path_exists_patch.start()
        self._makedirs = self._makedirs_patch.start()
        self._rename = self._rename_patch.start()
        self._environ = self._environ_patch.start()
        self._walk = self._walk_patch.start()
        self._path_getsize = self._path_getsize_patch.start()
        self._path_getmtime = self._path_getmtime_patch.start()
        self._remove = self._remove_patch.start()
        self._rmtree = self._rmtree_patch.start()
        self.addCleanup(self._unpatch)

        self._cache = SimpleFileCache(cache_folder='root', compressionOn=False)

    def _unpatch(self):
        self._io_open_patch.stop()
        self._gzip_open_patch.stop()
        self._path_exists_patch.stop()
        self._makedirs_patch.stop()
        self._rename_patch.stop()
        self._environ_patch.stop()
        self._walk_patch.stop()
        self._path_getsize_patch.stop()
        self._path_getmtime_patch.stop()
        self._remove_patch.stop()
        self._rmtree_patch.stop()

    def test_init_with_cache_root_initializes_root_folder(self):
        self._path_exists.return_value = False
        cache = SimpleFileCache(cache_root = 'root\\folder', compressionOn=False)
        self._makedirs.assert_called_once_with('root\\folder')

    def test_init_with_cache_root_ignores_cache_folder_parameter(self):
        self._path_exists.return_value = False
        cache = SimpleFileCache(cache_root = 'root', cache_folder='whatevver', compressionOn=False)
        self._makedirs.assert_called_once_with('root')

    def test_reset_cache(self):
        self._cache.reset_cache()
        self._rmtree.assert_called_once_with('app\\root')

    def test_get_without_tokenparameters_throws(self):
        with self.assertRaises(Exception):
            self._cache.get(lambda: u'some data', lambda data,
                            stream: '', lambda stream: '')

    def test_get_without_data_in_cache_calls_datagetter(self):
        self._path_exists.return_value = False
        message = u'Hello!'

        cached_message = self._cache.get(lambda: message, lambda data, stream: '',
                                         lambda stream: '', 'folder', 'file.csv')

        self.assertEquals(cached_message, message)

    def test_get_uncompressed_without_cached_data_writes_to_cache(self):
        self._path_exists.return_value = False
        expected_tmp_file = 'app\\root\\v1raw\\a\\b\\file.csv.uncommitted'
        expected_file = 'app\\root\\v1raw\\a\\b\\file.csv'

        self._cache.get(lambda: 'Hello!', lambda data, stream: '', 
                        lambda stream: '', 'a', 'b', 'file.csv')

        self._io_open.assert_called_with(expected_tmp_file, 'wb')
        self._rename.assert_called_with(expected_tmp_file, expected_file)

    def test_get_compressed_without_cached_data_deflates_to_cache(self):
        self._path_exists.return_value = False
        self._cache._compressionOn = True
        expected_tmp_file = 'app\\root\\v1gz\\a\\b\\file.csv.uncommitted'
        expected_file = 'app\\root\\v1gz\\a\\b\\file.csv'

        self._cache.get(lambda: 'Hello!', lambda data, stream: '', 
                        lambda stream: '', 'a', 'b', 'file.csv')

        self._gzip_open.assert_called_with(expected_tmp_file, 'wb')
        self._rename.assert_called_with(expected_tmp_file, expected_file)

    def test_get_uncompressed_with_data_in_cache_calls_deserializer(self):
        self._path_exists.return_value = True
        self._path_getsize.return_value = 10
        expected_file = 'app\\root\\v1raw\\a\\b\\file.csv'

        cached_message = self._cache.get(
            lambda: 'Hello!',
            lambda data, stream: '',
            lambda stream: 'Hello from cache!', 'a', 'b', 'file.csv')

        self._io_open.assert_called_with(expected_file, 'rb')
        self.assertEquals(cached_message, u'Hello from cache!')

    def test_get_compressed_with_data_in_cache_inflates_from_cache(self):
        self._path_exists.return_value = True
        self._path_getsize.return_value = 10
        self._cache._compressionOn = True

        cached_message = self._cache.get(
            lambda: 'Hello!',
            lambda data, stream: '',
            lambda stream: 'Hello from cache!', 'a', 'b', 'file.csv')

        self._gzip_open.assert_called_with('app\\root\\v1gz\\a\\b\\file.csv', 'rb')
        self.assertEquals(cached_message, u'Hello from cache!')

    def test_get_with_maxsize_evicts_old_files(self):
        self._walk.return_value = [
            ('rt', (), ('file.1', 'file.2', 'file.3')),
        ]
        files = {
            'app\\root': {'exists': True},
            'app\\root\\v1raw': {'exists': True},
            'app\\root\\v1raw\\file.0': {'exists': False},
            'rt\\file.1': {'size': 1000, 'time': 1000, 'exists': True}, 
            'rt\\file.2': {'size': 2 * 1024 * 1024 * 1024, 'time': 20, 'exists': True},
            'rt\\file.3': {'size': 3000, 'time': 50, 'exists': True}
        }
        self._path_getsize.side_effect = lambda p: 0 if p not in files else files[p]['size']
        self._path_getmtime.side_effect = lambda p: 0 if p not in files else files[p]['time']
        self._path_exists.side_effect = lambda p: files[p]['exists']
        cache = SimpleFileCache(cache_folder='root', compressionOn=False)

        cached_message = cache.get(lambda: '', lambda data, stream: '', lambda stream: '', 'file.0')

        self._remove.assert_called_once_with('rt\\file.2')

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger("datareservoirio.storage").setLevel(logging.DEBUG)
    unittest.main()