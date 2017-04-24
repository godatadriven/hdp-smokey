import argparse
import yarn.yarn_resourcemanager_smoketest

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='YARN ResourceManager smoketest')
    parser.add_argument('-t', '--type', required=False, default='AMBARI', choices=['AMBARI', 'KILL', 'NOOP'], help='How to stop the component.')
    args = parser.parse_args()

    tester = yarn.yarn_resourcemanager_smoketest.YarnResourceManagerSmokeTest(cmd_line_type=args.type)
    tester.run()

