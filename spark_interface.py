import os
import sys
import json
import hashlib
import logging

import requests
import parsewiki.utils as pwu

from collections import OrderedDict
from copy import deepcopy
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

class MD5IntegrityException(Exception): pass

logging.getLogger().setLevel(logging.INFO)
 
def arg_sort(iterable):
    """Return an array with the indexes that
    would make the given array sorted following
    the ascending order."""
    return sorted(range(len(iterable)), key=lambda x: iterable[x])

# If True then it's nullable
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
    """Return a tuple (page, timestamp, link)
    for each link found in page."""
    results = []
    page = json.loads(json_page)
    title = page['title']
    timestamp = page['timestamp']
    # structured_part: [name, text, link]
    for sp in page['structured_part']:
        if sp[2] is not None:
            results.append((title, timestamp, sp[2]))
    # unstructured_part: [key, link, position]
    for un_sp in page['unstructured_part']:
        if un_sp[1] is not None:
            results.append((title, timestamp, un_sp[1]))
    #return [result for result in results if result[1] is not None]
    return results

def collect_words(json_page):
    """Return a tuple (page, timestamp, word)
    for each word found in the unstructured part
    of the page."""
    results = []
    words = set()
    page = json.loads(json_page)
    title = page['title']
    timestamp = page['timestamp']
    # unstructured_part: [word, link, position]
    for un_sp in page['unstructured_part']:
        words.add(un_sp[0])
    if not len(words):
        return [(title, timestamp, None)]
    for word in words:
        results.append((title, timestamp, word))
    return results

def time_diff(iterable):
    """Given an iterable set containing timestamps
    for the same page, order them according to the timestamp,
    pick the timestamp two by two in sequence and compute
    the diff over the set of items they contain.

    Return:
      A tuple <timestamp, items_added, items_removed>
      for each pair of timestamps considered.
    """
    list_iter = list(iterable)
    timestamps = [value[0] for value in list_iter]

    sorted_ts = arg_sort(timestamps)
    diff_pairs = [(sorted_ts[t], sorted_ts[t+1]) \
                  for t in range(len(sorted_ts) - 1)]
    results = []
    for pair in diff_pairs:
        current_index, successive_index = pair
        current = list_iter[current_index]
        successive = list_iter[successive_index]
        current_items = current[1]
        successive_items = successive[1]
        diff_title = current[0] + "->" + successive[0]
        items_removed = list(set(current_items) - set(successive_items))
        items_added = list(set(successive_items) - set(current_items))
        results.append((diff_title, items_added, items_removed))
    return results

def encode_timestamp(entry):
    """Given an entry ((page, link), (changes_timestamps, page_timestamps))
    return an entry with same key (page, link) with a
    binary string which encodes the presence of the link in the
    page timestamps followed by the ordered set of timestamps for
    that page.

    Example:
      Suppose a link is present in 2017-24-1 until 2018-23-2
      and the page has the timestamps from 2016-31-1 up to
      2018-04-1 with 6 entries in between (2017-24-1 and
      2018-23-2 are in between by definition)
      then the link binary string is:
      0-1-1-1-1-0

      In this way it's possible to infer that the link wasn't
      there in the first timestamp, then appeared for a while
      and the disappeared.
    """
    change_ts, page_ts = entry[1]
    # avoid side effects on the rdd
    page_ts = deepcopy(page_ts)

    # it would be waaay better for the sort process
    # to be made by rdd operations...
    page_ts.sort()
    out = OrderedDict({key: 0 for key in page_ts})
    for change in change_ts:
        out[change] = 1
    return (entry[0], (page_ts, list(out.values())))
       

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

def get_offline_dump(dump_folder):
    dump_files = [dump for dump in os.listdir(dump_folder) \
                  if dump[-4:] == ".bz2"]
    for dump in dump_files:
        yield dump_folder + os.sep + dump

def to_csv(row):
    page, ts, link = row
    return str.join("\t", [page, ts, link])
    
            
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
                        .config("spark.executor.memory", "1g")\
                        .config("spark.executor.instances", "1")\
                        .config("spark.executor.cores", "1")\
                        .getOrCreate()
                        #.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.2.1")\
                        #.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.coll") \
                        #.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.coll") \
                       
    sc = spark.sparkContext
    pairs_rdd = spark.createDataFrame(sc.emptyRDD(), schema).rdd
    json_text_rdd = spark.createDataFrame(sc.emptyRDD(), schema).rdd
    pair_rdd = spark.createDataFrame(sc.emptyRDD(), schema).rdd

    wiki_version = "20180201"
    # in this way each dump is first stored into
    # a temporary file, set True to process it in memory
    # get_online_dump(wiki_version, 3, False)
    get_dump = lambda: get_offline_dump("/home/spark/Programming/test")
    q = 0
    for dump in get_dump():
        for chunk in get_wikipedia_chunk(dump, max_numpage=200, max_iteration=0):

            rdd = sc.parallelize(chunk, numSlices=20)
            json_text_rdd = rdd.map(jsonify)
            """
            # TASK 1: entity diff - method1
            # rearrange row values in order to have
            # a composite key made of (page, link) entries.
            # In addition remove double entries (with distinct).
            page_link_rdd = link_rdd.map(lambda entry:\
                                         ((entry[0], entry[2]), entry[1]))\
                                         .distinct()
            # group entries having same composite key
            # and flat the values obtained.
            # We now have all timestamps where a particular
            # link in a page has been changed.
            versions_rdd = page_link_rdd.groupByKey()\
                                        .mapValues(lambda entry_vals: list(entry_vals))

            # create a paird rdd <page, (link, tss)> which
            # will be used to join with the timestamp rdd.
            page_link_tss_rdd = versions_rdd.map(lambda entry: (entry[0][0], (entry[0][1], entry[1])))

            # We now want to have all timestamps a page has.
            # First, we collect all the timestamps associated to a page
            ts_list_rdd = link_rdd.map(lambda entry: (entry[0], entry[1])).distinct()
            page_ts_rdd = ts_list_rdd.groupByKey().mapValues(lambda entry_vals: list(entry_vals))

            # We want to expand the page_link_tss rdd with all timestamps
            # a page has, so that we can then further be able to
            # track the appearance and disappearance of a link on the timeline.
            #
            # We then rearrange the rdd
            joined_rdd = page_link_tss_rdd.\
                         join(page_ts_rdd)\
                         .map(lambda entry: (\
                                             (entry[0], entry[1][0][0]),\
                                             (entry[1][0][1], entry[1][1])))
            # finally we map the joined rdd so that
            # we make a row in the timestamp matrix.
            entity_encoding_rdd = joined_rdd.map(encode_timestamp)
            """
            # TASK1
            # entity diff - method 2
            links_rdd = json_text_rdd.flatMap(collect_links)
            pair_links_rdd = links_rdd.map(lambda entry: ((entry[0], entry[1]), entry[2]))

            # collect all links within a page in a single rdd
            links_page_rdd = pair_links_rdd.groupByKey()\
                             .mapValues(lambda entry_values: list(entry_values))

            # relax the structure and change the
            # pair rdd key-value values
            relaxed_rdd = links_page_rdd.map(lambda entry: (entry[0][0], entry[0][1], entry[1]))

            # the new pair rdd will have the following structure:
            # <page, <timestamp, words>>
            page_versions_rdd = relaxed_rdd.map(lambda entry: (entry[0], (entry[1], entry[2])))
            links_diff_rdd = page_versions_rdd.groupByKey().flatMapValues(time_diff)

            with open("/tmp/entities_out_{}.txt".format(q), "w") as fh:
                for i in links_diff_rdd.collect():
                    fh.write(str(i) + "\n")
                        
            # TASK2
            # words diff
            # select all the words within a page and
            # build a large tuple set
            words_rdd = json_text_rdd.flatMap(collect_words)

            # TODO: do stopwords removal here
            
            pair_words_rdd = words_rdd.map(lambda entry: ((entry[0], entry[1]), entry[2]))

            # collect all words within a page in a single rdd
            words_page_rdd = pair_words_rdd.groupByKey()\
                             .mapValues(lambda entry_values: list(entry_values))

            # relax the structure and change the
            # pair rdd key-value values
            relaxed_rdd = words_page_rdd.map(lambda entry: (entry[0][0], entry[0][1], entry[1]))

            # the new pair rdd will have the following structure:
            # <page, <timestamp, words>>
            page_versions_rdd = relaxed_rdd.map(lambda entry: (entry[0], (entry[1], entry[2])))
            words_diff_rdd = page_versions_rdd.groupByKey().flatMapValues(time_diff)

            # q is used here for debug.
            # write the rdd to several files in /tmp/*
            with open("/tmp/words_out_{}.txt".format(q), "w") as fh:
                for i in words_diff_rdd.collect():
                    fh.write(str(i) + "\n")
            q += 1

            # rdd union
            # json_text_rdd = json_text_rdd.union(rdd.map(jsonify))

            #try:
            #    links = spark.createDataFrame(pair_rdd, ["page", "link"])
            #    links.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
            #except ValueError:
            #    logging.warning("Empty RDD.")
