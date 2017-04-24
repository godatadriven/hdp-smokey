import argparse
import hive.hive_smoketest_hiveserver2

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Hive Hiveserver2 smoketest')
    parser.add_argument('-t', '--type', required=False, default='AMBARI', choices=['AMBARI', 'KILL', 'NOOP'], help='How to stop the component.')
    args = parser.parse_args()

    tester = hive.hive_smoketest_hiveserver2.HiveServerSmokeTest(cmd_line_type=args.type)
    tester.run()
