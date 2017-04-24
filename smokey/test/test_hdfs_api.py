import unittest
from unittest.mock import Mock, patch

import hdfs.hdfs_api as api


class TestHdfsApi(unittest.TestCase):
    def setUp(self):
        self.hdfs = api.HdfsApi(request_timeout=2)
        self.correct_output = """
"FSCK started by tester (auth:KERBEROS_SSL) from /192.168.0.489 for path /user/tester/subdir/20161221/host.hortonworks.com.log at Wed Feb 22 10:19:52 CET 2017
/user/tester/subdir/20161221/host.hortonworks.com.log 1066336015 bytes, 8 block(s):  OK
0. BP-1605498153-192.168.0.241-1426773903491:blk_1084465072_10763065 len=134217728 repl=3 [/Group1/DC1-X/RACK1/192.168.2.32:1019, /Group1/DC1-X/RACK1/192.168.2.29:1019, /Group1/DC1-X/RACK1/192.168.2.14:1019]
1. BP-1605498153-192.168.0.241-1426773903491:blk_1084465073_10763066 len=134217728 repl=3 [/Group1/DC2-X/RACK2/192.168.3.9:1019, /Group1/DC1-X/RACK1/192.168.2.9:1019, /Group1/DC1-X/RACK1/192.168.2.14:1019]
2. BP-1605498153-192.168.0.241-1426773903491:blk_1084465074_10763067 len=134217728 repl=3 [/Group1/DC1-X/RACK1/192.168.2.10:1019, /Group1/DC1-X/RACK1/192.168.2.12:1019, /Group1/DC2-X/RACK2/192.168.3.9:1019]
3. BP-1605498153-192.168.0.241-1426773903491:blk_1084465075_10763068 len=134217728 repl=3 [/Group1/DC2-X/RACK2/192.168.3.11:1019, /Group1/DC1-X/RACK1/192.168.2.38:1019, /Group1/DC1-X/RACK1/192.168.2.33:1019]
4. BP-1605498153-192.168.0.241-1426773903491:blk_1084465076_10763069 len=134217728 repl=3 [/Group1/DC2-X/RACK2/192.168.3.11:1019, /Group1/DC2-X/RACK2/192.168.3.16:1019, /Group1/DC1-X/RACK1/192.168.2.33:1019]
5. BP-1605498153-192.168.0.241-1426773903491:blk_1084465077_10763070 len=134217728 repl=3 [/Group1/DC1-X/RACK1/192.168.2.10:1019, /Group1/DC1-X/RACK1/192.168.2.12:1019, /Group1/DC2-X/RACK2/192.168.3.10:1019]
6. BP-1605498153-192.168.0.241-1426773903491:blk_1084465078_10763071 len=134217728 repl=3 [/Group1/DC1-X/RACK1/192.168.2.10:1019, /Group1/DC2-X/RACK2/192.168.3.8:1019, /Group1/DC1-X/RACK1/192.168.2.7:1019]
7. BP-1605498153-192.168.0.241-1426773903491:blk_1084465085_10763078 len=126811919 repl=3 [/Group1/DC1-X/RACK1/192.168.2.17:1019, /Group1/DC2-X/RACK2/192.168.3.7:1019, /Group1/DC2-X/RACK2/192.168.3.16:1019]

Status: HEALTHY
 Total size:\t1066336015 B
 Total dirs:\t0
 Total files:\t1
 Total symlinks:\t\t0
 Total blocks (validated):\t8 (avg. block size 133292001 B)
 Minimally replicated blocks:\t8 (100.0 %)
 Over-replicated blocks:\t0 (0.0 %)
 Under-replicated blocks:\t0 (0.0 %)
 Mis-replicated blocks:\t\t0 (0.0 %)
 Default replication factor:\t3
 Average block replication:\t3.0
 Corrupt blocks:\t\t0
 Missing replicas:\t\t0 (0.0 %)
 Number of data-nodes:\t\t27
 Number of racks:\t\t3
FSCK ended at Wed Feb 22 10:19:52 CET 2017 in 2 milliseconds



The filesystem under path '/user/tester/subdir/20161221/host.hortonworks.com.log' is HEALTHY"
"""
        self.empty_output = """
"FSCK started by tester (auth:KERBEROS_SSL) from /192.168.1.19 for path /user/tester/subdir/20161221/host.hortonworks.com.log at Wed Feb 22 10:29:05 CET 2017
FSCK ended at Wed Feb 22 10:29:05 CET 2017 in 1 milliseconds
Operation category READ is not supported in state standby


Fsck on path '/user/tester/subdir/20161221/host.hortonworks.com.log' FAILED"
"""

    @patch('requests.request')
    def test_mocking_request(self, mock_request):
        mock_request.return_value = Mock(ok=True, status_code=200, text=self.correct_output)

        hdfs_response = self.hdfs.get_block_info_for_file("/user/tester/subdir/20161221/host.hortonworks.com.log")
        self.assertEqual(self.correct_output, hdfs_response.text)

    @patch('requests.request')
    def test_check_response_status(self, mock_request):
        mock_request.return_value = Mock(ok=True, status_code=200, text=self.correct_output)
        hdfs_response = self.hdfs.get_block_info_for_file("/user/tester/subdir/20161221/host.hortonworks.com.log")
        self.hdfs._check_response_status(hdfs_response)

    @patch('requests.request')
    def test_check_response_status_wrong_response_status_code(self, mock_request):
        mock_request.return_value = Mock(ok=True, status_code=403, text=self.empty_output)

        with self.assertRaises(api.HdfsRequestError):
            hdfs_response = self.hdfs.get_block_info_for_file("/user/tester/subdir/20161221/host.hortonworks.com.log")
            self.hdfs._check_response_status(hdfs_response)

    def test_get_first_block_info(self):
        expected = "0. BP-1605498153-192.168.0.241-1426773903491:blk_1084465072_10763065 len=134217728 repl=3 [/Group1/DC1-X/RACK1/192.168.2.32:1019, /Group1/DC1-X/RACK1/192.168.2.29:1019, /Group1/DC1-X/RACK1/192.168.2.14:1019]"
        blockinfo = self.hdfs.get_first_block_info("/user/tester/subdir/20161221/host.hortonworks.com.log", self.correct_output)
        self.assertEqual(expected, blockinfo)

    def test_get_first_block_info_from_empty_response(self):
        with self.assertRaises(api.HdfsRequestError):
            self.hdfs.get_first_block_info("/user/tester/subdir/20161221/host.hortonworks.com.log", self.empty_output)

    def test_get_location_of_first_block(self):
        first_block_info = "0. BP-1605498153-192.168.0.241-1426773903491:blk_1084465072_10763065 len=134217728 repl=3 [/Group1/DC1-X/RACK1/192.168.2.32:1019, /Group1/DC1-X/RACK1/192.168.2.29:1019, /Group1/DC1-X/RACK1/192.168.2.14:1019]"
        result = self.hdfs.get_location_of_first_block(first_block_info)
        self.assertEqual("192.168.2.32", result)

    def test_get_location_of_first_block_in_unknown_response(self):
        first_block_info = "0. BP-1605498153-192.168.0.241-1426773903491:blk_1084465072_10763065 len=134217728 repl=3"
        with self.assertRaises(api.HdfsRequestError):
            self.hdfs.get_location_of_first_block(first_block_info)
