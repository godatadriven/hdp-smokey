import argparse
import spark.spark_smoketest

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Spark Historyserver smoketest')
    parser.add_argument('-t', '--type', required=False, default='AMBARI', choices=['AMBARI', 'KILL', 'NOOP'], help='How to stop the component.')
    args = parser.parse_args()

    tester = spark.spark_smoketest.Spark2SmokeTest(cmd_line_type=args.type)
    tester.run()

