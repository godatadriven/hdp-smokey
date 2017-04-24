import unittest
import logging
from unittest.mock import MagicMock
import subprocess

import spark.spark_runner as spark


class TestSparkRunner(unittest.TestCase):
    def setUp(self):
        self.spark = spark.SparkPiRunner(logging)

    def test_correct_output(self):
        output = str.encode("spark.yarn.driver.memoryOverhead is set but does not apply in client mode." +
                            "\nPi is roughly 3.1415471415471417")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=0, stdout=output, stderr=b'')
        self.spark._check_output(process.stdout, process.stderr)

    def test_error_on_wrong_output(self):
        output = str.encode("spark.yarn.driver.memoryOverhead is set but does not apply in client mode." +
                            "\nPi is roughly 2.1415471415471417")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=21, stdout=output, stderr=b'')
        with self.assertRaises(spark.SparkRequestError):
            self.spark._check_output(process.stdout, process.stderr)

    def test_error_on_wrong_output_with_small_pi(self):
        output = str.encode("spark.yarn.driver.memoryOverhead is set but does not apply in client mode." +
                            "\nPi is roughly 3.14")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=21, stdout=output, stderr=b'')
        with self.assertRaises(spark.SparkRequestError):
            self.spark._check_output(process.stdout, process.stderr)

    def test_error_on_wrong_output_without_pi(self):
        output = str.encode("spark.yarn.driver.memoryOverhead is set but does not apply in client mode.")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=21, stdout=output, stderr=b'')
        with self.assertRaises(spark.SparkRequestError):
            self.spark._check_output(process.stdout, process.stderr)
