import argparse
import hdfs.zkfailover_smoketest

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='HDFS Zookeeper failover controller smoketest')
    parser.add_argument('-t', '--type', required=False, default='AMBARI', choices=['AMBARI', 'KILL', 'NOOP'],
                        help='How to stop the component.')
    args = parser.parse_args()

    tester = hdfs.zkfailover_smoketest.FailoverControllerSmokeTest(cmd_line_type=args.type)
    tester.run()
