import argparse
import mapreduce.mr_smoketest

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Mapreduce Historyserver smoketest')
    parser.add_argument('-t', '--type', required=False, default='AMBARI', choices=['AMBARI', 'KILL', 'NOOP'],
                        help='How to stop the component.')
    args = parser.parse_args()

    tester = mapreduce.mr_smoketest.MrSmokeTest(cmd_line_type=args.type)
    tester.run()
