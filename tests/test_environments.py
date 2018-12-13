import unittest
import logging
import sys

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import datareservoirio._constants as consts
import datareservoirio.globalsettings as gs


class TestConfiguration(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(logging.StreamHandler(sys.stdout))

    def tearDown(self):
        gs.environment.set_qa()

    def verify_environment(self, expected):
        env = gs.environment.get()

        self.assertEqual(env, expected)

    def test_default_is_production(self):
        gs.environment.set_default()

        self.verify_environment(consts.ENV_PROD)

    def test_set_qa_environment(self):
        gs.environment.set_qa()

        self.verify_environment(consts.ENV_QA)

    def test_set_test_environment(self):
        gs.environment.set_test()

        self.verify_environment(consts.ENV_TEST)

    def test_set_dev_environment(self):
        gs.environment.set_dev()

        self.verify_environment(consts.ENV_DEV)

    def test_different_imports_refer_to_same_environment_instance(self):
        gs.environment.set_test()

        env1 = gs.environment.get()
        env2 = gs.environment.get()

        self.assertEqual(env1, env2)

        gs.environment.set_qa()

        env1 = gs.environment.get()
        env2 = gs.environment.get()

        self.assertEqual(env1, env2)

    @patch('datareservoirio.globalsettings.environment._logger.info')
    def test_change_of_environment_is_logged(self, mock):
        gs.environment.set_qa()
        mock.assert_any_call('Setting environment to: QA')


class TestApiBaseURL(unittest.TestCase):

    def test_(self):
        gs.environment.set_dev()
        base_url = gs.environment.api_base_url
        self.assertEqual(base_url, consts.API_BASE_URL_DEV)

        gs.environment.set_test()
        base_url = gs.environment.api_base_url
        self.assertEqual(base_url, consts.API_BASE_URL_TEST)

        gs.environment.set_qa()
        base_url = gs.environment.api_base_url
        self.assertEqual(base_url, consts.API_BASE_URL_QA)

        gs.environment.set_production()
        base_url = gs.environment.api_base_url
        self.assertEqual(base_url, consts.API_BASE_URL_PROD)

    @patch('datareservoirio.globalsettings.environment._logger.info')
    def test_change_of_base_url_is_logged(self, mock):
        gs.environment.set_qa()
        mock.assert_any_call(
            'Setting baseurl to: {}'.format(consts.API_BASE_URL_QA))


if __name__ == '__main__':
    unittest.main()
