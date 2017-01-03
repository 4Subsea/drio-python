from . import constants

class ADALParameters(object):

    def __init__(self, environment):
        if environment == constants.ENV_TEST:
            self.set_test()
        elif environment == constants.ENV_QA:
            self.set_qa()
        elif environment == constants.ENV_PROD:
            self.set_prod()
        elif environment == constants.ENV_DEV:
            self.set_dev()

    @property
    def resource(self):
        return self._resource

    @property
    def client_id(self):
        return self._client_id

    @property
    def authority(self):
        return self._authority

    def set_test(self):
        self._resource = constants.RESOURCE_TEST
        self.set_shared()

    def set_qa(self):
        self._resource = constants.RESOURCE_QA
        self.set_shared()

    def set_prod(self):
        self._resource = constants.RESOURCE_PROD
        self.set_shared()
        
    def set_dev(self):
        self._resource = constants.RESOURCE_DEV
        self.set_shared()

    def set_shared(self):
        self._client_id = constants.CLIENT_ID
        self._authority = constants.AUTHORITY

