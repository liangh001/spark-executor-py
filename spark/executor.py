#####################################
# Author: Hong Liang
# Date: 2017-09-29
# Desc:
# This module includes Three classes below.
# An instance of the `CommandResult` class
# may be looked as a DTO(Data Transfer Object).Some of methods in the `SparkExecutor` and 'SparkSqlExecutor'
# class return result that is an instance of the `CommandResult` class.
# You can use straight the object of `SparkExecutor` class to operate spark.
######################################

import os
import sys
import subprocess
import re
import logging
from collections import OrderedDict
from utils.calendar import Calendar
from utils.cmd import CommandExecutor
from utils.cmd import CommandResult

from spark.exceptions import (SparkUnfoundError,
                             SparkCommandExecuteError,
                             SparkSqlUnfoundError,
                             SparkSqlCommandExecuteError)


_logger = logging.getLogger(__name__)


class SparkSqlExecutor(object):
    """SparkSqlExecutor may be looked as a wrapper of SparkSqlCLI.
    Attributes
    ----------
    cmd_path : str
        The path of sparksql client.
    enable_verbose_mode : Optional[bool]
        If True, the class will use -v parameter when executing spark client.
    Parameters
    ----------
    cmd_path : Optional[str]
        The path of sparksql client. Default is 'spark-sql'.
    init_settings : Optional[sequence]
        The settings of sparksql client. Default is [].An empty sequence.
    verbose : Optional[bool]
        Default is False.

    Examples
    --------
    >>> from spark import SparkSqlExecutor
    >>> client=SparkSqlExecutor("spark-sql")
    >>> loader=client.load_data()

    >>> init_settings=[]
    >>> init_settings.append("set spark.sql.shuffle.partitions = 500")
    >>> init_settings.append("set spark.sql.autoBroadcastJoinThreshold = 304857600")
    >>> client=SparkSqlExecutor(cmd_path="spark-sql",init_settings=init_settings)
    >>> result=client.execute(sql=sql)

    """

    def __init__(self, cmd_path="spark-sql", variable_substitution={},init_settings=[], verbose=False):
        if cmd_path is None or len(cmd_path) == 0:
            raise ValueError(
                "When you passed the argument of spark_cmd_path,it should have a value.")

        cmd = "which %s" % (cmd_path)
        result = CommandExecutor.system(cmd)

        if result.status != 0:
            raise SparkSqlUnfoundError(
                "the spark command path:%s is not exists." % (cmd_path))

        self.cmd_path = cmd_path
        self.enable_verbose_mode = verbose
        if init_settings:
            self.init_settings = init_settings
        else:
            self.init_settings=['set spark.sql.shuffle.partitions = 500','set spark.sql.autoBroadcastJoinThreshold = 304857600']
        if variable_substitution:
            self.variable_substitution = variable_substitution
        else:
            self.variable_substitution = {
                "spark.app.name":"sparksql_default",
                "spark.driver.memory":"4G",
                "spark.executor.memory":"2G",
                "spark.executor.instances":"20",
                "spark.executor.cores":"3",
                "spark.yarn.queue":"q_guanggao.q_adlog",
                "spark.master":"yarn",
                "spark.submit.deployMode":"client",
                "spark.ui.port":"4060",
                "spark.yarn.executor.memoryOverhead":"8192",
                "spark.kryoserializer.buffer.max":"128m",
                "spark.kryoserializer.buffer":"64k",
                "spark.speculation":"true",
            }
        self.__default_command = self.cmd_path + " "
        self.config = {
            "spark.app.name":"--name",
            "spark.driver.memory":"--driver-memory",
            "spark.executor.memory":"--executor-memory",
            "spark.executor.instances":"--num-executors",
            "spark.executor.cores":"--executor-cores",
            "spark.yarn.queue":"--queue",
            "spark.master":"--master",
            "spark.submit.deployMode":"--deploy-mode"
        }

    def _result_to_sequence(self, text):
        tables = []
        if text:
            lines = text.strip().split("\n")
            for line in lines:
                if len(line) > 0:
                    if re.match('(SET |.*spark).*',str(line)):
                        continue
                    tables.append(line)

        return tables

    def load_data(self, inpath, db, table_name, partitions=None, local=True, overwrite=True):
        """
        substitute for `load data [local] inpath '' 
        [overwrite] into TABLE table_name 
        partition (dt='20160501',hour='12')`
        """
        spark_sql = ""
        if local:
            spark_sql = "load data local inpath '%s'" % (inpath)
        else:
            spark_sql = "load data inpath '%s'" % (inpath)

        if overwrite:
            spark_sql = "%s overwrite into table %s.%s" % (
                spark_sql, db, table_name)
        else:
            spark_sql = "%s into table %s.%s" % (spark_sql, db, table_name)

        if partitions:
            partition_seq = []
            for key, value in partitions.items():
                partition_seq.append("%s='%s'" % (key, value))
            spark_sql = "%s partition (%s);" % (
                spark_sql, ",".join(partition_seq))
        else:
            spark_sql = "%s;" % (spark_sql)

        _logger.debug("executed spark sql:%s" % (spark_sql))
        cr = self.execute(sql=spark_sql)

        if cr.status == 0:
            return True
        else:
            _logger.error(cr.stderr_text)
            raise SparkSqlCommandExecuteError(
                "the spark command:%s error! error info:%s" % (spark_sql, cr.stderr_text))
        return False

    def execute(self,init_sql_file=None, sql_file=None, 
        sql=None, output_file=None, log_file=None, print_stderr=None):
        """this method can be used to execute sparksql cliet commands.
        Parameters
        ----------
        variable_substitution : Optional[str]
            The parameter is a dict type variant.
        init_sql_file : Optional[str]
            The path of the initialization sql file
        sql_file : Optional[str]
            The path of the spark sql
        sql : Optional[str]
            The spark sql,if this parameter is required,the parameter of the sql_file will be disable.
        output_file : Optional[str]
            When passed the parameter 'sql',this parameter can be used.
        """
        # validate the parameters
        if sql_file is None and sql is None:
            raise ValueError(
                "the function execute_spark_sql:At least one argument of sql_file and sql passed.")

        if init_sql_file and not os.path.exists(init_sql_file):
            raise ValueError(
                "the function execute_spark_sql:The initialization sql file:%s doesn't exists." % (init_sql_file))

        if sql_file and not os.path.exists(sql_file):
            raise ValueError(
                "The spark sql file:%s doesn't exists." % (sql_file))

        if output_file and sql is None:
            raise ValueError(
                "the function execute_spark_sql:Just when the sql parameter passed,the output file parameter can be used.")

        spark_vars = ""
        if self.variable_substitution:
            for key,val in self.variable_substitution.items():
                if key in self.config:
                    spark_vars = "".join(
                        [spark_vars, " ", self.config[key], " ", val])
                else:
                    spark_vars = "".join(
                        [spark_vars, " --conf ", key, "=", val])

        spark_init_sql_file = ""
        if init_sql_file:
            spark_init_sql_file = " -i %s" % (init_sql_file)

        spark_sql_file = ""
        spark_sql = ""
        if sql:
            spark_sql = sql
            if len(self.init_settings)>0:
                spark_sql = " -e \"%s;%s\"" % (";".join(self.init_settings), spark_sql)
            else:
                spark_sql = " -e \"%s\"" % (spark_sql)

            if output_file:
                spark_sql = " ".join([spark_sql, "1>",output_file])
            elif log_file:
                spark_sql = " ".join([spark_sql, "2>",log_file])
        else:
            if sql_file:
                spark_sql_file = " -f %s" % (sql_file)
                if output_file:
                    spark_sql_file = " ".join([spark_sql_file, "1>",output_file])
                elif log_file:
                    spark_sql_file = " ".join([spark_sql_file, "2>",log_file])


        spark_verbose = ""
        if self.enable_verbose_mode:
            spark_verbose = " -v "

        execute_cmd = "".join([self.__default_command, spark_verbose, spark_init_sql_file,
                               spark_vars, spark_sql_file, spark_sql])

        _logger.info("the function execute:%s" % (execute_cmd))
        if print_stderr:
            cr = CommandExecutor.systemWithMess(execute_cmd)
        else:
            cr = CommandExecutor.system(execute_cmd)
        #cr contain (status,stdout_text,stderr_text)
        return cr
        
class SparkExecutor(object):
    """SparkExecutor may be looked as a wrapper of SparkCLI.
    Attributes
    ----------
    cmd_path : str
        The path of Spark client.
    enable_verbose_mode : Optional[bool]
        If True, the class will use -v parameter when executing spark client.
    Parameters
    ----------
    cmd_path : Optional[str]
        The path of Spark client. Default is 'spark-sql'.
    init_settings : Optional[sequence]
        The settings of Spark client. Default is [].An empty sequence.
    verbose : Optional[bool]
        Default is False.

    Examples
    --------
    >>> from spark import SparkExecutor
    >>> client=SparkExecutor("spark-submit")
    >>> loader=client.load_data()

    >>> client=SparkExecutor(spark_cmd_path="spark-submit")
    >>> result=client.execute()

    """

    def __init__(self, cmd_path="spark-submit", variable_substitution={}):
        if cmd_path is None or len(cmd_path) == 0:
            raise ValueError(
                "When you passed the argument of spark_cmd_path,it should have a value.")

        cmd = "which %s" % (cmd_path)
        result = CommandExecutor.system(cmd)

        if result.status != 0:
            raise SparkUnfoundError(
                "the spark command path:%s is not exists." % (cmd_path))

        self.cmd_path = cmd_path
        if variable_substitution:
            self.variable_substitution = variable_substitution
        else:
            self.variable_substitution = {
                "spark.app.name":"spark_default",
                "spark.driver.memory":"4G",
                "spark.executor.memory":"2G",
                "spark.executor.instances":"20",
                "spark.executor.cores":"3",
                "spark.yarn.queue":"q_guanggao.q_adlog",
                "spark.master":"yarn",
                "spark.submit.deployMode":"client",
                "spark.ui.port":"4060",
                "spark.yarn.executor.memoryOverhead":"8192",
                "spark.kryoserializer.buffer.max":"128m",
                "spark.kryoserializer.buffer":"64k",
                "spark.speculation":"true",
            }
        self.__default_command = self.cmd_path + " "
        self.config = {
            "spark.app.name":"--name",
            "spark.driver.memory":"--driver-memory",
            "spark.executor.memory":"--executor-memory",
            "spark.executor.instances":"--num-executors",
            "spark.executor.cores":"--executor-cores",
            "spark.yarn.queue":"--queue",
            "spark.master":"--master",
            "spark.submit.deployMode":"--deploy-mode"
        }

    def execute(self, init_sql_file=None, sql_file=None, 
        sql=None, output_file=None, log_file=None, print_stderr=None):
        """this method can be used to execute Spark cliet commands.
        Parameters
        ----------
        variable_substitution : Optional[str]
            The parameter is a dict type variant.
        init_sql_file : Optional[str]
            The path of the initialization sql file
        sql_file : Optional[str]
            The path of the spark sql
        sql : Optional[str]
            The spark sql,if this parameter is required,the parameter of the sql_file will be disable.
        output_file : Optional[str]
            When passed the parameter 'sql',this parameter can be used.
        """
        # validate the parameters
        if sql_file is None and sql is None:
            raise ValueError(
                "the function execute_spark_sql:At least one argument of sql_file and sql passed.")

        if init_sql_file and not os.path.exists(init_sql_file):
            raise ValueError(
                "the function execute_spark_sql:The initialization sql file:%s doesn't exists." % (init_sql_file))

        if sql_file and not os.path.exists(sql_file):
            raise ValueError(
                "The spark sql file:%s doesn't exists." % (sql_file))

        if output_file and sql is None:
            raise ValueError(
                "the function execute_spark_sql:Just when the sql parameter passed,the output file parameter can be used.")

        spark_vars = ""
        if self.variable_substitution:
            for key,val in self.variable_substitution.items():
                if key in self.config:
                    spark_vars = "".join(
                        [spark_vars, " ", self.config[key], " ", val])
                else:
                    spark_vars = "".join(
                        [spark_vars, " --conf ", key, "=", val])

        spark_init_sql_file = ""
        if init_sql_file:
            spark_init_sql_file = " -i %s" % (init_sql_file)

        spark_sql_file = ""
        spark_sql = ""
        if sql:
            spark_sql = sql
            spark_sql = " -e \"%s\"" % (spark_sql)

            if output_file:
                spark_sql = " ".join([spark_sql, "1>",output_file])
            elif log_file:
                spark_sql = " ".join([spark_sql, "2>",log_file])
        else:
            if sql_file:
                spark_sql_file = " -f %s" % (sql_file)
                if output_file:
                    spark_sql_file = " ".join([spark_sql_file, "1>",output_file])
                elif log_file:
                    spark_sql_file = " ".join([spark_sql_file, "2>",log_file])


        spark_verbose = ""
        if self.enable_verbose_mode:
            spark_verbose = " -v "

        execute_cmd = "".join([self.__default_command, spark_verbose, spark_init_sql_file,
                               spark_vars, spark_sql_file, spark_sql])

        _logger.info("the function execute:%s" % (execute_cmd))

        if print_stderr:
            cr = CommandExecutor.systemWithMess(execute_cmd)
        else:
            cr = CommandExecutor.system(execute_cmd)

        return cr
