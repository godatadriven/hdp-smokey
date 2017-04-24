import unittest
import logging
from unittest.mock import MagicMock
import subprocess

import mapreduce.mr_runner as mr


class TestMrRunner(unittest.TestCase):
    def setUp(self):
        self.mr = mr.MrPiRunner(logging)

    def test_correct_output(self):
        output = str.encode("""	File Input Format Counters
        Bytes Read=944
    File Output Format Counters
        Bytes Written=97
Job Finished in 47.907 seconds
Estimated value of Pi is 3.148656850000000000000""")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=0, stdout=output, stderr=b'')
        self.mr._check_output(process.stdout, process.stderr)

    def test_error_on_wrong_output(self):
        output = str.encode("""	File Input Format Counters
        Bytes Read=944
        File Output Format Counters
        Bytes Written=97
        Job Finished in 47.907 seconds
        Estimated value of Pi is 3.13500000000000000000""")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=21, stdout=output, stderr=b'')
        with self.assertRaises(mr.MrRequestError):
            self.mr._check_output(process.stdout, process.stderr)

    def test_error_on_wrong_output_with_small_pi(self):
        output = str.encode("""	File Input Format Counters
        Bytes Read=944
        File Output Format Counters
        Bytes Written=97
        Job Finished in 47.907 seconds
        Estimated value of Pi is 3.14""")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=21, stdout=output, stderr=b'')
        with self.assertRaises(mr.MrRequestError):
            self.mr._check_output(process.stdout, process.stderr)

    def test_error_on_wrong_output_without_pi(self):
        output = str.encode("""	File Input Format Counters
        Bytes Read=944
        File Output Format Counters
        Bytes Written=97
        Job Finished in 47.907 seconds""")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=21, stdout=output, stderr=b'')
        with self.assertRaises(mr.MrRequestError):
            self.mr._check_output(process.stdout, process.stderr)
