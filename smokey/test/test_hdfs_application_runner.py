import unittest
import logging
from unittest.mock import MagicMock
import subprocess

import hdfs.hdfs_application_runner as runner


class TestHdfsApplicationRunner(unittest.TestCase):
    def setUp(self):
        self.spark = runner.SparkHdfsTestRunner(logging)
        self.mrgen = runner.MrTeragenRunner(logging)
        self.mrsort = runner.MrTerasortRunner(logging)

    def test_correct_output_spark(self):
        output = str.encode("spark.yarn.driver.memoryOverhead is set but does not apply in client mode.\n" +
                            "Iteration 1 took 3692 ms\nIteration 2 took 87 ms\nIteration 3 took 65 ms\n" +
                            "Iteration 4 took 64 ms\nIteration 5 took 56 ms\nIteration 6 took 74 ms\n" +
                            "Iteration 7 took 60 ms\nIteration 8 took 60 ms\nIteration 9 took 51 ms\n" +
                            "Iteration 10 took 52 ms\n")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=0, stdout=output, stderr=b'')
        self.spark._check_output(process.stdout, process.stderr)

    def test_error_on_wrong_output_spark(self):
        output = str.encode("spark.yarn.driver.memoryOverhead is set but does not apply in client mode." +
                            "\nPi is roughly 2.1415471415471417")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=21, stdout=output, stderr=b'')
        with self.assertRaises(runner.SparkRequestError):
            self.spark._check_output(process.stdout, process.stderr)

    def test_error_on_wrong_output_spark_with_not_enough_iterations(self):
        output = str.encode("spark.yarn.driver.memoryOverhead is set but does not apply in client mode.\n" +
                            "Iteration 1 took 3692 ms\n" +
                            "Iteration 4 took 64 ms\nIteration 5 took 56 ms\nIteration 6 took 74 ms\n" +
                            "Iteration 7 took 60 ms\nIteration 8 took 60 ms\nIteration 9 took 51 ms\n" +
                            "Iteration 10 took 52 ms\n")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=21, stdout=output, stderr=b'')
        with self.assertRaises(runner.SparkRequestError):
            self.spark._check_output(process.stdout, process.stderr)

    def test_correct_output_mr(self):
        output = str.encode("""
17/02/23 11:24:07 INFO client.ConfiguredRMFailoverProxyProvider: Failing over to rm2
17/02/23 11:24:08 INFO mapreduce.JobSubmitter: number of splits:2
17/02/23 11:24:08 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1768772602026_0001
17/02/23 11:24:08 INFO mapreduce.JobSubmitter: Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:sndbx, Ident: (HDFS_DELEGATION_TOKEN token 1086442 for smoketest)
17/02/23 11:24:09 INFO impl.YarnClientImpl: Submitted application application_1768772602026_0001
17/02/23 11:24:09 INFO mapreduce.Job: The url to track the job: http://sandbox.hortonworks.com:8088/proxy/application_1768772602026_0001/
17/02/23 11:24:09 INFO mapreduce.Job: Running job: job_1768772602026_0001
17/02/23 11:24:24 INFO mapreduce.Job: Job job_1768772602026_0001 running in uber mode : false
17/02/23 11:24:24 INFO mapreduce.Job:  map 0% reduce 0%
17/02/23 11:24:40 INFO mapreduce.Job:  map 50% reduce 0%
17/02/23 11:24:50 INFO mapreduce.Job:  map 100% reduce 0%
17/02/23 11:24:55 INFO mapreduce.Job:  map 100% reduce 100%
17/02/23 11:24:55 INFO mapreduce.Job: Job job_1768772602026_0001 completed successfully
17/02/23 11:24:55 INFO mapreduce.Job: Counters: 50
\tFile System Counters
\t\tFILE: Number of bytes read=52000006
\t\tFILE: Number of bytes written=104468578
\t\tFILE: Number of read operations=0
\t\tHDFS: Number of write operations=2
""")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=0, stdout=b'', stderr=output)
        self.mrgen._check_output(process.stdout, process.stderr)
        self.mrsort._check_output(process.stdout, process.stderr)

    def test_error_on_wrong_output_mr(self):
        output = str.encode("""
17/02/23 11:24:07 INFO client.ConfiguredRMFailoverProxyProvider: Failing over to rm2
17/02/23 11:24:08 INFO mapreduce.JobSubmitter: number of splits:2
17/02/23 11:24:08 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1768772602026_0001
17/02/23 11:24:08 INFO mapreduce.JobSubmitter: Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:sndbx, Ident: (HDFS_DELEGATION_TOKEN token 1086442 for smoketest)
17/02/23 11:24:09 INFO impl.YarnClientImpl: Submitted application application_1768772602026_0001
17/02/23 11:24:09 INFO mapreduce.Job: The url to track the job: http://sandbox.hortonworks.com:8088/proxy/application_1768772602026_0001/
17/02/23 11:24:09 INFO mapreduce.Job: Running job: job_1768772602026_0001
17/02/23 11:24:24 INFO mapreduce.Job: Job job_1768772602026_0001 running in uber mode : false
17/02/23 11:24:24 INFO mapreduce.Job:  map 0% reduce 0%
17/02/23 11:24:40 INFO mapreduce.Job:  map 50% reduce 0%
17/02/23 11:24:50 INFO mapreduce.Job:  map 100% reduce 0%
17/02/23 11:24:55 INFO mapreduce.Job:  map 100% reduce 100%
17/02/23 11:24:55 INFO mapreduce.Job: Job job_1768772602026_0001 FAILED
17/02/23 11:24:55 INFO mapreduce.Job: Counters: 49
""")
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=21, stdout=b'', stderr=output)
        with self.assertRaises(runner.MrRequestError):
            self.mrgen._check_output(process.stdout, process.stderr)
            self.mrsort._check_output(process.stdout, process.stderr)

