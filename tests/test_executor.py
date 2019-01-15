# coding=UTF-8

import unittest
import sys
import os
import logging
import logging.config
from utils.cmd import CommandResult
from utils.cmd import CommandExecutor

sys.path.append("..")
from spark.executor import SparkSqlExecutor
from spark.exceptions import SparkUnfoundError
from spark.exceptions import SparkSqlCommandExecuteError
import test_data

#CONF_LOG = "../conf/logging.conf"
# logging.config.fileConfig(CONF_LOG)

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S')


class TestSparkSqlExecutorMethods(unittest.TestCase):

    def setUp(self):
        status = os.system('which spark-sql')

        if status == 0:
            self.spark_enable = True
        else:
            self.spark_enable = False

        self.assertRaises(ValueError, SparkSqlExecutor, None)
        if not self.spark_enable:
            self.assertRaises(SparkUnfoundError, SparkSqlExecutor, "spark-sql")
            self.executor = SparkSqlExecutor("ls")
        else:
            self.executor = SparkSqlExecutor("spark-sql")

    def test_execute_system_command(self):
        rc = CommandExecutor.system("ls ")
        self.assertTrue(isinstance(rc, CommandResult),
                        "return value type error,not a CommandResult instance.")

    def test_execute_sparksql(self):
        rc = self.executor.execute(sql=test_data.TEST_SQL)
        print rc.status
        #print rc.stderr_text
        print rc.stdout_text      

    def tearDown(self):
        self.executor = None


if __name__ == '__main__':
    unittest.main()
