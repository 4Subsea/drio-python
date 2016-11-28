from azure.storage.blob import BlockBlobService


def get_blobservice(upload_params):
    return BlockBlobService(upload_params['Account'], 
                            sas_token=upload_params['SasKey'])
     
