import argparse
import hdfs.datanode_smoketest

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='HDFS Datanode smoketest')
    parser.add_argument('-t', '--type', required=False, default='AMBARI', choices=['AMBARI', 'KILL', 'NOOP'],
                        help='How to stop the component.')
    args = parser.parse_args()

    tester = hdfs.datanode_smoketest.DatanodeSmokeTest(cmd_line_type=args.type)
    tester.run()
