import argparse
import re
import json
import logging

DEFAULT_SPARK_CONFIGURATIONS = {\
                                "master" : "local",\
                                "appName" : "WikiCrunching",\
                                "config" : [\
	                                    {"spark.executor.memory" : "10g"},\
	                                    {"spark.executor.instances" : "4"},\
	                                    {"spark.executor.cores" : "1"},\
	                                    {"spark.jars.packages" : "org.mongodb.spark:mongo-spark-connector_2.11:2.2.1"},\
	                                    {"spark.mongodb.input.uri" : "mongodb://127.0.0.1/test.coll"},\
	                                    {"spark.mongodb.output.uri" : "mongodb://127.0.0.1/test.coll"}\
	                        ]}

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

def get_spark_config(conf_file):
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
                        help="The file describing the configuration "\
                        + "used to build the Spark session", \
                        type=str)
    return parser
