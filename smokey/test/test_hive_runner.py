# flake8: noqa
import unittest
import logging
from unittest.mock import MagicMock
import subprocess
import hive.hive_runner as hive


class TestHiveRunner(unittest.TestCase):
    def setUp(self):
        self.hive = hive.HiveRunner(logging)

    def test_correct_output(self):
        stderr = str.encode("""
            Connecting to jdbc:hive2://sandbox.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
            Connected to: Apache Hive (version 1.2.1000.2.5.0.0-1245)
            Driver: Hive JDBC (version 1.2.1000.2.5.0.0-1245)
            Transaction isolation: TRANSACTION_REPEATABLE_READ
            No rows affected (0.12 seconds)
            10 rows selected (2.913 seconds)
            Beeline version 1.2.1000.2.5.0.0-1245 by Apache Hive
            Closing: 0: jdbc:hive2://sandbox.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")

        """)

        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=0, stdout=b'', stderr=stderr)
        self.hive._check_output(process.stdout, process.stderr)

    def test_error_on_partial_wrong_output(self):
        stderr = str.encode("""
            Connecting to jdbc:hive2://sandbox.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
            No current connection
            Connected to: Apache Hive (version 1.2.1000.2.5.0.0-1245)
            Driver: Hive JDBC (version 1.2.1000.2.5.0.0-1245)
            Transaction isolation: TRANSACTION_REPEATABLE_READ
            No rows affected (0.12 seconds)
            10 rows selected (2.913 seconds)
            Beeline version 1.2.1000.2.5.0.0-1245 by Apache Hive
            Closing: 0: jdbc:hive2://sandbox.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")

        """)

        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=0, stdout=b'', stderr=stderr)
        with self.assertRaises(hive.HiveRequestError):
            self.hive._check_output(process.stdout, process.stderr)

    def test_error_unable_to_connect_output(self):
        stderr = str.encode("""
            Connecting to jdbc:hive2://sandbox.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
            No current connection
            No current connection
            Driver: Hive JDBC (version 1.2.1000.2.5.0.0-1245)
            Transaction isolation: TRANSACTION_REPEATABLE_READ
            No rows affected (0.12 seconds)
            10 rows selected (2.913 seconds)
            Beeline version 1.2.1000.2.5.0.0-1245 by Apache Hive
            Closing: 0: jdbc:hive2://sandbox.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2")

        """)

        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=0, stdout=b'', stderr=stderr)
        with self.assertRaises(hive.HiveRequestError):
            self.hive._check_output(process.stdout, process.stderr)

    def test_error_on_wrong_output(self):
        stderr = str.encode("""
        Command [('beeline', '-u', 'jdbc:hive2://sandbox.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2', '-e', 'use foodmart', '-e', 'select * from customer limit 10;')] returned non zero exit code [2].
        Output: b''; Stderr: b'Connecting to jdbc:hive2://sandbox.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2
        17/03/01 15:20:51 [main]: ERROR jdbc.HiveConnection: Error opening session
        org.apache.thrift.transport.TTransportException: org.apache.http.client.ClientProtocolException
            at org.apache.thrift.transport.THttpClient.flushUsingHttpClient(THttpClient.java:297)
            at org.apache.thrift.transport.THttpClient.flush(THttpClient.java:313)
            at org.apache.thrift.TServiceClient.sendBase(TServiceClient.java:73)
            at org.apache.thrift.TServiceClient.sendBase(TServiceClient.java:62)
            at org.apache.hive.component.cli.thrift.TCLIService$Client.send_OpenSession(TCLIService.java:154)
            at org.apache.hive.component.cli.thrift.TCLIService$Client.OpenSession(TCLIService.java:146)
            at org.apache.hive.jdbc.HiveConnection.openSession(HiveConnection.java:552)
            at org.apache.hive.jdbc.HiveConnection.<init>(HiveConnection.java:170)
            at org.apache.hive.jdbc.HiveDriver.connect(HiveDriver.java:105)
            at java.sql.DriverManager.getConnection(DriverManager.java:664)
            at java.sql.DriverManager.getConnection(DriverManager.java:208)
            at org.apache.hive.beeline.DatabaseConnection.connect(DatabaseConnection.java:146)
            at org.apache.hive.beeline.DatabaseConnection.getConnection(DatabaseConnection.java:211)
            at org.apache.hive.beeline.Commands.connect(Commands.java:1190)
            at org.apache.hive.beeline.Commands.connect(Commands.java:1086)
            at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
            at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
            at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
            at java.lang.reflect.Method.invoke(Method.java:498)
            at org.apache.hive.beeline.ReflectiveCommandHandler.execute(ReflectiveCommandHandler.java:52)
            at org.apache.hive.beeline.BeeLine.dispatch(BeeLine.java:990)
            at org.apache.hive.beeline.BeeLine.initArgs(BeeLine.java:715)
            at org.apache.hive.beeline.BeeLine.begin(BeeLine.java:777)
            at org.apache.hive.beeline.BeeLine.mainWithInputRedirection(BeeLine.java:491)
            at org.apache.hive.beeline.BeeLine.main(BeeLine.java:474)
            ........
            Error: Could not establish connection to jdbc:hive2://sandbox.hortonworks.com:10001/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2: org.apache.http.client.ClientProtocolException (state=08S01,code=0)
        """)
        process = MagicMock(spec=subprocess.CompletedProcess, args=["test"], returncode=2, stdout=b'', stderr=stderr)
        with self.assertRaises(hive.HiveRequestError):
            self.hive._check_output(process.stdout, process.stderr)
