import unittest
import logging
import os
import sys
import warnings

from datareservoirio.storage.simplefilecache import (
    EvictBySizeAndAge,
    SimpleFileCache)

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch


class Test_SimpleFileCache(unittest.TestCase):
    def setUp(self):
        self._io_open_patch = patch('datareservoirio.storage.simplefilecache.io.open')
        self._path_exists_patch = patch('datareservoirio.storage.simplefilecache.os.path.exists')
        self._makedirs_patch = patch('datareservoirio.storage.simplefilecache.os.makedirs')
        self._rename_patch = patch('datareservoirio.storage.simplefilecache.os.rename')
        self._user_cache_dir_patch = patch('datareservoirio.storage.simplefilecache.user_cache_dir')
        self._walk_patch = patch('datareservoirio.storage.simplefilecache.os.walk')
        self._path_getsize_patch = patch('datareservoirio.storage.simplefilecache.os.path.getsize')
        self._path_getmtime_patch = patch('datareservoirio.storage.simplefilecache.os.path.getmtime')
        self._remove_patch = patch('datareservoirio.storage.simplefilecache.os.remove')
        self._rmdir_patch = patch('datareservoirio.storage.simplefilecache.os.rmdir')
        self._rmtree_patch = patch('datareservoirio.storage.simplefilecache.shutil.rmtree')

        self._io_open = self._io_open_patch.start()
        self._path_exists = self._path_exists_patch.start()
        self._makedirs = self._makedirs_patch.start()
        self._rename = self._rename_patch.start()
        self._user_cache_dir = self._user_cache_dir_patch.start()
        self._walk = self._walk_patch.start()
        self._path_getsize = self._path_getsize_patch.start()
        self._path_getmtime = self._path_getmtime_patch.start()
        self._remove = self._remove_patch.start()
        self._rmdir = self._rmdir_patch.start()
        self._rmtree = self._rmtree_patch.start()
        self.addCleanup(self._unpatch)

        self._user_cache_dir.return_value = 'app\\root'
        self._cache = SimpleFileCache(cache_folder='root')

    def _unpatch(self):
        self._io_open_patch.stop()
        self._path_exists_patch.stop()
        self._makedirs_patch.stop()
        self._rename_patch.stop()
        self._user_cache_dir_patch.stop()
        self._walk_patch.stop()
        self._path_getsize_patch.stop()
        self._path_getmtime_patch.stop()
        self._remove_patch.stop()
        self._rmdir_patch.stop()
        self._rmtree_patch.stop()

    def test_init_with_cache_root_initializes_root_folder(self):
        self._path_exists.return_value = False
        SimpleFileCache(cache_root='root\\folder')
        self._makedirs.assert_called_once_with(os.path.abspath('root\\folder'))

    def test_init_with_cache_root_ignores_cache_folder_parameter(self):
        self._path_exists.return_value = False
        SimpleFileCache(cache_root='root', cache_folder='whatevver')
        self._makedirs.assert_called_once_with(os.path.abspath('root'))

    def test_reset_cache(self):
        self._cache.reset_cache()
        self._user_cache_dir.assert_called_once_with('root')
        self._rmtree.assert_called_once_with(os.path.abspath('app\\root'))

    def test_get_without_tokenparameters_throws(self):
        with self.assertRaises(Exception):
            self._cache.get(lambda: u'some data', lambda data,
                            stream: '', lambda stream: '')

    def test_get_without_data_in_cache_calls_datagetter(self):
        self._path_exists.return_value = False
        message = u'Hello!'

        cached_message = self._cache.get(lambda: message, lambda data, stream: '',
                                         lambda stream: '', 'folder', 'file.csv')

        self.assertEqual(cached_message, message)

    def test_get_without_cached_data_writes_to_cache(self):
        self._path_exists.return_value = False
        expected_tmp_file = os.path.abspath('app\\root\\v1\\a\\b\\file.csv.uncommitted')
        expected_file = os.path.abspath('app\\root\\v1\\a\\b\\file.csv')

        self._cache.get(lambda: range(60*24 + 1), lambda data, stream: '',
                        lambda stream: '', 'a', 'b', 'file.csv')

        self._io_open.assert_called_with(expected_tmp_file, 'wb')
        self._rename.assert_called_with(expected_tmp_file, expected_file)

    def test_get_without_cached_data_too_short_for_cache(self):
        self._path_exists.return_value = False
        self._cache.get(lambda: range(60*24), lambda data, stream: '',
                        lambda stream: '', 'a', 'b', 'file.csv')

        self._io_open.assert_not_called()
        self._rename.assert_not_called()

    def test_get_with_data_in_cache_calls_deserializer(self):
        self._path_exists.return_value = True
        self._path_getsize.return_value = 10
        expected_file = os.path.abspath('app\\root\\v1\\a\\b\\file.csv')

        cached_message = self._cache.get(
            lambda: 'Hello!',
            lambda data, stream: '',
            lambda stream: 'Hello from cache!', 'a', 'b', 'file.csv')

        self._io_open.assert_called_with(expected_file, 'rb')
        self.assertEqual(cached_message, u'Hello from cache!')

    def test_get_with_sizeoverage_evicts_old_files(self):
        self._walk.return_value = [
            ('rt', (), ('file.1', 'file.2', 'file.3')),
        ]
        files = {
            os.path.abspath('app\\root'): {'exists': True},
            os.path.abspath('app\\root\\v1'): {'exists': True},
            os.path.abspath('app\\root\\v1\\file.0'): {'exists': False},
            'rt\\file.1': {'size': 1000, 'time': 10.0, 'exists': True},
            'rt\\file.2': {'size': 2 * 1024 * 1024 * 1024, 'time': 20.0, 'exists': True},
            'rt\\file.3': {'size': 3000, 'time': 50.0, 'exists': True}
        }

        self._path_getsize.side_effect = lambda p: 0 if p not in files else files[p]['size']
        self._path_getmtime.side_effect = lambda p: 0 if p not in files else files[p]['time']
        self._path_exists.side_effect = lambda p: files[p]['exists']
        cache = SimpleFileCache(cache_folder='root')

        cache.get(lambda: '', lambda data, stream: '', lambda stream: '', 'file.0')

        self._remove.assert_called_once_with('rt\\file.2')
        self._rmdir.assert_called_once_with('rt')

    def test_get_when_evict_old_file_fail_triggers_userwarning(self):
        self._walk.return_value = [
            ('rt', (), ('file.1', 'file.2', 'file.3')),
        ]
        files = {
            os.path.abspath('app\\root'): {'exists': True},
            os.path.abspath('app\\root\\v1'): {'exists': True},
            os.path.abspath('app\\root\\v1\\file.0'): {'exists': False},
            'rt\\file.1': {'size': 1000, 'time': 10.0, 'exists': True},
            'rt\\file.2': {'size': 2 * 1024 * 1024 * 1024, 'time': 20.0, 'exists': True},
            'rt\\file.3': {'size': 3000, 'time': 50.0, 'exists': True}
        }
        self._path_getsize.side_effect = lambda p: 0 if p not in files else files[p]['size']
        self._path_getmtime.side_effect = lambda p: 0 if p not in files else files[p]['time']
        self._path_exists.side_effect = lambda p: files[p]['exists']
        self._remove.side_effect = Exception('someone is locking the file!')

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            cache = SimpleFileCache(cache_folder='root')

            cache.get(lambda: '', lambda data, stream: '', lambda stream: '', 'file.0')

            self._remove.assert_called_once_with('rt\\file.2')
            self.assertIs(w[0].category, UserWarning)


class Test_EvictBySizeAndAge(unittest.TestCase):
    def setUp(self):
        self._walk_patch = patch('datareservoirio.storage.simplefilecache.os.walk')
        self._rmdir_patch = patch('datareservoirio.storage.simplefilecache.os.rmdir')

        self._walk = self._walk_patch.start()
        self._rmdir = self._rmdir_patch.start()
        self.addCleanup(self._unpatch)

    def _unpatch(self):
        self._walk_patch.stop()
        self._rmdir_patch.stop()

    def test_init(self):
        EvictBySizeAndAge()

    def test_evicts_empty_folder(self):
        self._walk.return_value = [
            ('rt_empty', (), ()),
        ]

        evicter = EvictBySizeAndAge()
        evicter.CLEANUP = True
        evicter.evict('', 1024)

        self._rmdir.assert_called_once_with('rt_empty')


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger("datareservoirio.storage").setLevel(logging.DEBUG)
    unittest.main()
