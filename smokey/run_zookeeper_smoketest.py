import argparse
import zookeeper.zookeeper_smoketest

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Zookeeper smoketest')
    parser.add_argument('-t', '--type', required=False, default='AMBARI', choices=['AMBARI', 'KILL', 'NOOP'],
                        help='How to stop the component.')
    args = parser.parse_args()

    tester = zookeeper.zookeeper_smoketest.ZookeeperSmokeTest(cmd_line_type=args.type)
    tester.run()
