import unittest

import sys

sys.path.append('../../')
import timeseriesclient
import timeseriesclient.constants as consts
import timeseriesclient.adal_parameters as apm

class TestADALParameters(unittest.TestCase):

    def test_test_environment(self):
        params = apm.ADALParameters(consts.ENV_TEST)

        self.assertEqual(params.resource, consts.RESOURCE_TEST)
        self.verify_shared_parameters(params)

    def test_qa_environment(self):
        params = apm.ADALParameters(consts.ENV_QA)

        self.assertEqual(params.resource, consts.RESOURCE_QA)
        self.verify_shared_parameters(params)

    def test_prod_environment(self):
        params = apm.ADALParameters(consts.ENV_PROD)

        self.assertEqual(params.resource, consts.RESOURCE_PROD)
        self.verify_shared_parameters(params)

    def verify_shared_parameters(self, params):
        self.assertEqual(params.client_id, consts.CLIENT_ID)
        self.assertEqual(params.authority, consts.AUTHORITY)
