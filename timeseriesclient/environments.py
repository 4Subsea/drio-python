from . import constants
from .adalparameters import ADALParameters
import logging


class Environment(object):

    def __init__(self):
        self._logger = logging.getLogger(__name__)

        self.set_default()

    def get(self):
        return self.current_environment

    def set_default(self):
        self.set_production()

    def set_production(self):
        self._set_environment(constants.ENV_PROD)
        self._set_base_url(constants.API_BASE_URL_PROD)

    def set_test(self):
        self._set_environment(constants.ENV_TEST)
        self._set_base_url(constants.API_BASE_URL_TEST)

    def set_qa(self):
        self._set_environment(constants.ENV_QA)
        self._set_base_url(constants.API_BASE_URL_QA)
        
    def set_dev(self):
        self._set_environment(constants.ENV_DEV)
        self._set_base_url(constants.API_BASE_URL_DEV)

    def _set_environment(self, environment):
        self._logger.info('Setting environment to: {}'.format(environment))
        self.current_environment = environment

    def _set_base_url(self, base_url):
        self._logger.info('Setting baseurl to: {}'.format(base_url))
        self._api_base_url = base_url

    # this method is misplaced here, refactor! //JWE
    def get_adal_parameters(self):
        return ADALParameters(self.current_environment) 

    @property
    def api_base_url(self):
        return self._api_base_url
