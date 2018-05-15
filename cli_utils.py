import argparse
import re
import json
import logging

SPARK_APP_NAME = "appName"
SPARK_MASTER = "master"
SPARK_CONFIG = "config"
DEFAULT_SPARK_CONFIGURATIONS = {\
                                "master" : "local",\
                                "appName" : "WikiCrunching",\
                                "config" : {"spark.executor.memory" : "10g",\
	                                    "spark.executor.instances" : "4",\
	                                    "spark.executor.cores" : "1",\
	                                    "spark.jars.packages" : "org.mongodb.spark:mongo-spark-connector_2.11:2.2.1",\
	                                    "spark.mongodb.input.uri" : "mongodb://127.0.0.1/test.coll",\
	                                    "spark.mongodb.output.uri" : "mongodb://127.0.0.1/test.coll"}}

WIKI_VERSION = "wiki-version"
MEMORY = "process-in-memory"
MAX_DUMP = "max-dump"
DEFAULT_ONLINE_CONFIGURATIONS = {\
                                 WIKI_VERSION : "20180201",\
                                 MEMORY : False,\
                                 MAX_DUMP : 3}

def spark_builder(builder, spark_config):
    """Build the Spark Session given the
    session builder and the configurations hash.
    
    Returns:
      A SparkSession with the configurations defined in the
      given Python hash.
    """
    try:
        builder.master(spark_config[SPARK_MASTER])\
               .appName(spark_config[SPARK_APP_NAME])
        for param_name, param_value in spark_config[SPARK_CONFIG].items():
            builder.config(param_name, param_value)
        return builder.getOrCreate()
    except KeyError as ke:
        logging.error("SPARK-CONF-FILE-ERROR: missing "\
                      + "required object: {}".format(ke))
        raise ke

def get_online_params(online_config):
    """Return the parameters used when downloading
    wikipedia dumps online"""
    print(str(online_config))
    return (online_config[WIKI_VERSION],\
            online_config[MEMORY],\
            online_config[MAX_DUMP])

def remove_comments(file_stream):
    """Remove all lines starting with the
    # char.

    Returns:
       An array of uncommented lines.
    """
    uncommented_lines = []
    comment_reg = re.compile("^[^'\"]*?(('[^']*'|\"[^\"]*\")[^#'\"]*)*#")
    for line in file_stream:
        temp_line = line.strip()
        comment_found = comment_reg.search(temp_line)
        uncommented_line = temp_line
        if comment_found:
            uncommented_line = comment_found.group()[:-1]
        uncommented_lines.append(uncommented_line)
    return str.join("\n", uncommented_lines)

def get_config(conf_file):
    """Given a path to a json file remove the comments
    present and return the equivalent python hash."""
    try:
        config_json_string = remove_comments(conf_file)
        return json.loads(config_json_string)
    except json.decoder.JSONDecodeError as decode_e:
        logging.error("SPARK-CONF-FILE-ERROR: The given json "\
                      + "configuration file "\
                      + "is not properly formatted.")
        logging.error(decode_e)
        raise decode_e

def define_argparse():
    """Define the argument parser for the main program."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--dump-dir",\
                        help="The directory containing the dumps",\
                        type=str)
    parser.add_argument("-c", "--spark-conffile",\
                        help="The file describing the configurations "\
                        + "used to build the Spark session", \
                        type=str)
    parser.add_argument("-o", "--online-conffile",\
                        help="The file describing the configurations "\
                        + "used when retrieving dumps online. This only "\
                        + "states the conf file, to use online dumps just "\
                        + "do not set the dumps folder")
    return parser
