import argparse
import hdfs.namenode_smoketest

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='HDFS Namenode smoketest')
    parser.add_argument('-t', '--type', required=False, default='AMBARI', choices=['AMBARI', 'KILL', 'NOOP'],
                        help='How to stop the component.')
    args = parser.parse_args()

    tester = hdfs.namenode_smoketest.NamenodeSmokeTest(cmd_line_type=args.type)
    tester.run()
