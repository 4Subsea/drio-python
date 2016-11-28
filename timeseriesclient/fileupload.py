import base64
import sys

if sys.version_info.major == 3:
    from io import BytesIO, StringIO
elif sys.version_info.major == 2:
    from StringIO import StringIO
 
from azure.storage.blob import BlobBlock

class DataFrameUploader(object):

    def __init__(self, block_blob_service):
        self.n_lines = int(1e6)
        self.blocksize = 4*1024*1024 # 4MB
        self.block_blob_service = block_blob_service

        self._blocks = [] # refactor out, introduces state into class instances


    def upload(self, dataframe, upload_params):
        leftover = ''
        block_id = 0

        for i, chunk in enumerate(self._gen_line_chunks(dataframe, self.n_lines)):
            buf = StringIO()
            chunk.to_csv(buf)
            buf.seek(0)

            n_blocks = 0
            while True:
                block_data = leftover + buf.read(self.blocksize-len(leftover))
                leftover = ''
                n_blocks += 1

                if len(block_data) < self.blocksize:
                    leftover = block_data
                    break

                block = self._make_block(block_id)
                self._append_block(block)
                block_id += 1

                self._put_block(block_data, block.id, upload_params)

        if leftover:
            block = self._make_block(block_id)
            self._append_block(block)
            
            self._put_block(block_data, block.id, upload_params)

        self._commit_blocks(self._blocks, upload_params)


    def _put_block(self, block_data, block_id, upload_params):
        container = upload_params['Container']
        blobname = upload_params['Path']

        self.block_blob_service.put_block(container, 
                                          blobname, 
                                          block=block_data.encode('ascii'),
                                          block_id=block_id)

    def _commit_blocks(self, blocks, upload_params):
        container = upload_params['Container']
        blobname = upload_params['Path']

        self.block_blob_service.put_block_list(container, blobname, blocks)

    def _make_block(self, block_id):
        base64_block_id = self._b64encode(block_id)
        return BlobBlock(id=base64_block_id)

    def _b64encode(self, i):
        b_ascii= str(i).encode('ascii')
        b_b64  = base64.b64encode(b_ascii)
        return b_b64.decode('ascii')

    def _append_block(self, block):
        self._blocks.append(block)

    def _gen_line_chunks(self, dataframe, n):
        a = 0
        b = n

        while a<len(dataframe):
            yield dataframe.iloc[a:b]
            a += n
            b += n

 
