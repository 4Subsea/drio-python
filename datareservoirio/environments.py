import logging

from . import _constants


class Environment(object):
    def __init__(self):
        self._logger = logging.getLogger(__name__)

        self.set_default()

    def get(self):
        return self.current_environment

    def set_default(self):
        self.set_production()

    def set_production(self):
        self._set_environment(_constants.ENV_PROD)
        self._set_base_url(_constants.API_BASE_URL_PROD)
        self._set_application_insight_connectionstring(
            _constants.APPLICATIONINSIGHTS_PROD_CONNECTIONSTRING
        )

    def set_qa(self):
        self._set_environment(_constants.ENV_QA)
        self._set_base_url(_constants.API_BASE_URL_QA)
        self._set_application_insight_connectionstring(
            _constants.APPLICATIONINSIGHTS_QA_CONNECTIONSTRING
        )

    def set_test(self):
        self._set_environment(_constants.ENV_TEST)
        self._set_base_url(_constants.API_BASE_URL_TEST)
        self._set_application_insight_connectionstring(
            _constants.APPLICATIONINSIGHTS_TEST_CONNECTIONSTRING
        )

    def set_dev(self):
        self._set_environment(_constants.ENV_DEV)
        self._set_base_url(_constants.API_BASE_URL_DEV)
        self._set_application_insight_connectionstring(
            _constants.APPLICATIONINSIGHTS_DEV_CONNECTIONSTRING
        )

    def _set_environment(self, environment):
        self._logger.info(f"Setting environment to: {environment}")
        self.current_environment = environment

    def _set_base_url(self, base_url):
        self._logger.info(f"Setting baseurl to: {base_url}")
        self._api_base_url = base_url

    def _set_application_insight_connectionstring(self, app_insights_connetionstring):
        self._logger.info(
            f"Setting application insights connectionstring to: {app_insights_connetionstring}"
        )
        self._application_insight_connectionstring = app_insights_connetionstring

    @property
    def api_base_url(self):
        return self._api_base_url
