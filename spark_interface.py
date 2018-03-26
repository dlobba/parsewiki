import sys
import json
import hashlib
import logging

import requests
import parsewiki.utils as pwu

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

class MD5IntegrityException(Exception): pass

logging.getLogger().setLevel(logging.INFO)

# TODO: search for what those True values are meant for
schema = StructType([\
                     StructField("title", StringType(), True),\
                     StructField("link", StringType(), True)])

def get_wikipedia_chunk(bzip2_source, max_numpage=20, max_iteration=None):
    """Return a bzip2 file containing wikipedia pages in chunks
    of a given number of pages.

    It's possibile to limit the parsing by indicating the number
    of iteration for the process to be repeated. If this is `None`
    then all the bzip2 file is processed.

    Params:
      bzip2_source: source to the bzip2 file, either a filename or
        a bytearray
      max_numpage (int): length of each chunk
      max_iteration (int): number of iterations of the process
    """
    num_iteration = 0
    num_page = 0
    pages = []
    # the method used to iterate over pages
    # ALERT: pointer to function here!
    page_iterator = pwu.bzip2_memory_page_iter
    # if a filename is given then process as a bz2 file
    if type(bzip2_source) == str:
        page_iterator = pwu.bzip2_page_iter
    for wikipage in page_iterator(bzip2_source):
        for revision in pwu.iter_revisions(wikipage):
            num_page += 1
            pages.append(revision)
            # remember that revision is a tuple
            # (title, timestamp, plain_wikitext)
            if num_page >= max_numpage:
                yield pages
                pages = []
                num_page = 0
                num_iteration += 1
        if max_iteration and num_iteration >= max_iteration:
            break
    if len(pages) > 0:
        yield pages

def jsonify(wiki_tuple):
    title, timestamp, page = wiki_tuple
    return pwu.wikipage_to_json(page, title, timestamp)

def collect_links(json_page):
    """Return a tuple (page, link)
    for each link found in page."""
    results = []
    page = json.loads(json_page)
    title = page['title']
    # structured_part: [name, text, link]
    for sp in page['structured_part']:
        results.append((title, sp[2]))
    # unstructured_part: [key, link, position]
    for un_sp in page['unstructured_part']:
        results.append((title, un_sp[1]))
    return [result for result in results if result[1] is not None]

def get_online_dump(wiki_version, max_dump=None, memory=False):
    """Download a specific wikipedia version dumps online,
    returing each dump.

    Parameters:
      wiki_version (str): date string of the wiki version to
        work on
      max_dump (int): the number of dump to take into account
        (None if all dumps shuld be considered)
      memory (bool): True if the dumps must be processed in memory.
        If False then each dump is first saved temporarly into
        the system and then processed as a file.

    Notes:
      Keep in mind that by working with dumps in memory, in case
      the dump is 1GB then at least 1GB of ram is needed and then
      a much higher requirement is needed whene decompression
      is performed.
    """
    endpoint = "http://wikimedia.bytemark.co.uk"
    status_url = endpoint + "/wikidatawiki/" + wiki_version + "/dumpstatus.json"
    status_json = requests.get(status_url).json()
    # testing dataset (no revision)
    dumps = status_json['jobs']['articlesdump']
    # full dataset (70TB)
    #dumps = status_json['jobs']['metahistory7zdump']
    if dumps['status'] != "done":
        print("The dump is incomplete, stopping the execution...\n")
        exit(-1)
    files = []
    for key in dumps['files'].keys():
        files.append({'url': dumps['files'][key]['url'],\
                      'md5': dumps['files'][key]['md5'],\
                      'size': dumps['files'][key]['size']})
    # sort in order to download smaller files first
    files.sort(key=lambda file: file['size'])
    # if not set, consider all files
    if max_dump is None:
        max_dump = len(files)

    for dump in files[:max_dump]:
        if not memory:
            # Try to open the file matching with the md5sum of the resource.
            # If it does not exist download it and write in the file as it's md5sum
            filename_r = "/tmp/wikidump_" + dump['md5']
            try:
                with open(filename_r, "rt") as bz2_fh:
                        # If the file exists we are sure that it is not corrupted.
                        logging.info("Dump already downloaded, skipping download and reloading it.")
                        yield filename_r
            except FileNotFoundError:
                url = endpoint + dump['url']
                logging.info("Downloading dump: {}".format(url))
                bz2_dump = requests.get(url).content
                md5sum = hashlib.md5(bz2_dump).hexdigest()
                if md5sum != dump['md5']:
                    raise MD5IntegrityException("The dump downloaded has a wrong MD5 value.")
                filename_w = "/tmp/wikidump_" + md5sum
                with open(filename_w, "wb") as bz2_fh:
                    bz2_fh.write(bz2_dump)
                yield filename_w

        else:
            url = endpoint + dump['url']
            logging.info("Downloading dump: {}".format(url))
            bz2_dump = requests.get(url).content
            yield bz2_dump


if __name__ == "__main__":
    """Divide the wikipedia source into chunks of several
    pages, each chunk is processed within an RDD in order
    to parallelize the process of conversion into json files.
    """
    spark = SparkSession.builder\
                        .master("local")\
                        .appName("WikiCrunching")\
                        .config("spark.executor.memory", "10g")\
                        .config("spark.executor.instances", "4")\
                        .config("spark.executor.cores", "1")\
                        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.2.1")\
                        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.coll") \
                        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.coll") \
                        .getOrCreate()
                        #.config("spark.executor.memory", "8g")\
                        #.config("spark.executor.instances", "8")\
                        #.config("spark.executor.cores", "1")\
    sc = spark.sparkContext
    pairs_rdd = spark.createDataFrame(sc.emptyRDD(), schema).rdd
    json_text_rdd = spark.createDataFrame(sc.emptyRDD(), schema).rdd
    pair_rdd = spark.createDataFrame(sc.emptyRDD(), schema).rdd

    wiki_version = "20180201"
    # in this way each dump is first stored into
    # a temporary file, set True to process it in memory
    for dump in get_online_dump(wiki_version, 3, False):
        print("HERE: dump downloaded, currently processing....")
        for chunk in get_wikipedia_chunk(dump, max_numpage=200, max_iteration=0):

            rdd = sc.parallelize(chunk, numSlices=20)
            json_text_rdd = rdd.map(jsonify)
            #json_text_rdd = json_text_rdd.union(rdd.map(jsonify))
            # for i in json_text_rdd.collect():
            #     print(i)

            # try to read each line individually
            pair_rdd = json_text_rdd.flatMap(collect_links)
            try:
                links = spark.createDataFrame(pair_rdd, ["page", "link"])
                links.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
            except ValueError:
                logging.warning("Empty RDD.")
