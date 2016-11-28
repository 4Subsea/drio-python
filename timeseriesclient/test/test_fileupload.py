import unittest
import sys
import pandas as pd
import numpy as np
import base64

try:
    from unittest.mock import Mock
except:
    from mock import Mock

from azure.storage.blob import BlobBlock

try:
    from .. import fileupload
except:
    sys.path.append('../../')
    import timeseriesclient.fileupload as fileupload

class Test_DataFrameUploader(unittest.TestCase):

    def setUp(self):
        self.upload_params = { 'Container':'blobcontainer', 'Path':'blob_xy' }

    def test_constructor(self):
        uploader = fileupload.DataFrameUploader(block_blob_service=None)

        expected_attributes = ['n_lines', 
                               'blocksize', 
                               'block_blob_service',
                               '_blocks']

        for attribute in expected_attributes:
            if not hasattr(uploader, attribute):
                self.fail('Expected uploader to have attribute {}'.format(attribute))


    def test_upload(self):
        mock_blobservice = Mock()
        mock_putblock = Mock()
        mock_commit = Mock()
        uploader = fileupload.DataFrameUploader(block_blob_service=mock_blobservice)
        uploader._put_block = mock_putblock
        uploader._commit_blocks = mock_commit()

        df = pd.DataFrame({'a':np.arange(1001)})
        uploader.upload(df, self.upload_params)

        self.assertEqual(mock_putblock.call_count, 1)
        mock_putblock.assert_called_with(df.to_csv(), 
                        base64.b64encode('0'.encode('ascii')).decode('ascii'), 
                        self.upload_params)

        self.assertEqual(mock_commit.call_count, 1)


    def test_upload_long(self):
        mock_blobservice = Mock()
        mock_putblock = Mock()
        uploader = fileupload.DataFrameUploader(block_blob_service=mock_blobservice)
        uploader._put_block = mock_putblock

        df = pd.DataFrame({'a':np.arange(1.001e6)})
        uploader.upload(df, self.upload_params)

        # 4 was just found empirically 
        self.assertEqual(mock_putblock.call_count, 4)
        self.assertEqual(len(uploader._blocks), 4)

    def test_commit(self):
        mock_blobservice = Mock()
        mock_putblocklist = Mock()
        mock_blobservice.put_block_list = mock_putblocklist
        uploader = fileupload.DataFrameUploader(block_blob_service=mock_blobservice)

        
        blocklist = [BlobBlock(id=base64.b64encode(x.encode('ascii'))) for x in ['1','2','3']]
        uploader._commit_blocks(blocklist , self.upload_params)

        mock_putblocklist.assert_called_with('blobcontainer', 
                                             'blob_xy', 
                                             blocklist) 

        

    def test_upload_block(self):
        mock_blobservice = Mock()
        mock_putblock = Mock()
        uploader = fileupload.DataFrameUploader(block_blob_service=mock_blobservice)

        uploader._put_block(block_data='blockdata', 
                            block_id='id', 
                            upload_params={'Container' : 'container',
                                           'Path' : 'blob_xy'})

        
        

    def test_make_block(self):
        uploader = fileupload.DataFrameUploader(block_blob_service=None)

        block = uploader._make_block(0)

        self.assertIsInstance(block, BlobBlock)
        self.assertEqual(block.id, 'MA==')

    def test_b64encode(self):
        uploader = fileupload.DataFrameUploader(block_blob_service=None)

        b64_result = uploader._b64encode(0)
        b64_expected = 'MA=='
        self.assertEqual(b64_result, b64_expected)
        
        b64_result = uploader._b64encode(1)
        b64_expected = 'MQ=='
        self.assertEqual(b64_result, b64_expected)
        
        b64_result = uploader._b64encode('1')
        b64_expected = 'MQ=='
        self.assertEqual(b64_result, b64_expected)

        b64_result = uploader._b64encode(int(1e6))
        b64_expected = 'MTAwMDAwMA=='
        self.assertEqual(b64_result, b64_expected)

    def test_append_block(self):
        uploader = fileupload.DataFrameUploader(block_blob_service=None)
        self.assertEqual(uploader._blocks, [])

        uploader._append_block('dummy block')
        self.assertEqual(uploader._blocks, ['dummy block'])

    def test_gen_line_chunks(self):
        uploader = fileupload.DataFrameUploader(block_blob_service=None)
        
        df = pd.DataFrame({'a':np.arange(999)})

        for i,chunk in enumerate(uploader._gen_line_chunks(df, 100)):
            if i<9:
                self.assertEqual(len(chunk), 100)
            elif i==9:
                self.assertEqual(len(chunk), 99)
            else:
                self.fail("Too many iterations")

        self.assertEqual(i+1, 10)


        
        



        


