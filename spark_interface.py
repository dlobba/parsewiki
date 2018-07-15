import os
import json
import csv
import logging
import re
import math

#import parsewiki.lcs as lcsdiff
import parsewiki.diff as tokendiff
import cli_utils as cu
import parsewiki.utils as pwu
import dumpmanager as dm

from nltk.corpus import stopwords
from collections import OrderedDict
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, ArrayType, IntegerType

logging.getLogger().setLevel(logging.INFO)

## functions for task1 ---------------------------

def jsonify(wiki_tuple):
    title, timestamp, contributor, page = wiki_tuple
    return pwu.wikipage_to_json(page, title, timestamp, contributor)

def collect_heading(json_page):
    """Return a tuple (page_title, timestamp, json_page)
    for the given page."""
    results = []
    page = json.loads(json_page)
    title = page['title']
    timestamp = page['timestamp']
    return title, timestamp, json_page

def dump_to_json(spark_context, dump_iter, json_dir):
    """Given a **function** to retrieve dumps (either
    online or offline) and a folder where to store
    results, transform dumps into json and store
    them into the given folder.

    Args:
      spark_context: the context related to the task.
      dump_iter(function): the function used to retrieve dumps.
      json_dir(str): the folder used to store final .json files.
    """
    # TASK1: get the dump, max_numpage pages at a time.
    # Transform to JSON the pages and create a .json
    # file for each page, containing JSON objects representing
    # page revisions.
    for dump in dump_iter():
        for chunk in pwu.get_wikipedia_chunk(dump,\
                                             max_numpage=5,\
                                             max_iteration=4):
            chunk_rdd = spark_context.parallelize(chunk, numSlices=20)
            json_text_rdd = chunk_rdd.map(jsonify)
            json_heading_rdd = json_text_rdd\
                               .map(collect_heading)

            # now we want to store all revision of each
            # page into a separate json file
            # in json_dir
            #
            # first of all, obtain all pages so we can create
            # a filename
            page_titles = json_heading_rdd\
                          .map(lambda entry: entry[0])\
                          .distinct()\
                          .collect()

            json_pages = json_heading_rdd\
                         .map(lambda entry: ((entry[0], entry[1]), entry[2]))\
                         .sortByKey()\
                         .collect()

            # create (flushing) a new file for each page
            filename_assoc = {title:0 for title in page_titles}
            for page_title in page_titles:
                filename = str.join("_", [word\
                                       for word in re.split("\W",\
                                                            page_title.lower())\
                                       if word != ""])
                filename_assoc[page_title] = filename
                with open(json_dir + "{}.json".format(filename), "w") as fh:
                    pass

            for page in json_pages:
                page_title = page[0][0]
                filename = filename_assoc[page_title]
                with open(json_dir + "{}.json".format(filename), "a") as fh:
                    fh.write(str(page[1]) + "\n")

            # push pages to mongoDB alternatively
            #try:
            #    links = spark.createDataFrame(pair_rdd, ["page", "link"])
            #    links.write.\
            #        format("com.mongodb.spark.sql.DefaultSource")\
            #        .mode("append")\
            #        .save()
            #except ValueError:
            #    logging.warning("Empty RDD.")

# end functions for task1 ------------------------

# functions for task2 ----------------------------
def arg_sort(iterable):
    """Return an array with the indexes that
    would make the given array sorted following
    the ascending order."""
    return sorted(range(len(iterable)), key=lambda x: iterable[x])

def collect_page_elements(json_page):
    """Return a tuple (page, timestamp, text, location)
    for each token in the unstrucuted part.
    Text in case of links will contain the explicit link
    and not the visible text appearing on the page.

    Note:
      Due to the fact that normal text and liks are grouped
      in the same way, we cannot distinguish one from
      the other in any way.

      A solution could be to prefix links with a special
      mark.
    """
    MARK = "[E]"
    results = []
    page = json.loads(json_page)
    title = page['title']
    timestamp = page['timestamp']
    # unstructured_part: [word, link, position]
    for un_sp in page['unstructured_part']:
        if un_sp[1] is not None:
            # if it's a link then return it
            results.append((title, timestamp, MARK + un_sp[1], un_sp[2]))
        else:
            results.append((title, timestamp, un_sp[0], un_sp[2]))
    return results

def indexer(timestamp_list):
    """Given a timestamp_list, sort it in ascending
    order and associate to each element an increasing
    index starting from 0.

    Note:
      It's possible to directly use the sort function
      since revision timestamps follow the same string
      format. If this wasn't the case, this decision
      would have lead to problems.
    """
    ts_list = list(timestamp_list)
    ts_list.sort()
    result = []
    for index in range(0, len(ts_list)):
        result.append((ts_list[index], index))
    return result

def gen_pair_indexer(ts_index_pairs):
    """Given an entry containing a list
    of (timestampString, timestampIndex) pairs
    return a list pairing consecutive timestamps:
    
        (<timestamp1Index, timestamp2Index>),
            (timestamp1String, timestamp2String)

    This allows to associate the timestamp of a
    revision (either its string representation
    and the numeric index associate) to the
    string and numeric representation of the
    timestamp of the next adjacent revision.

    Note:
      The use of the indexer function prior
      to this is **assumed**.

      Having associated the index we can establish
      clearly the relationship between two
      revision, meaning which one happened before
      and whether they are adjacent with respect
      to time.
    """
    tss_str = []
    tss_index = []
    for ts_str, ts_index in ts_index_pairs:
        tss_str.append(ts_str)
        tss_index.append(ts_index)
        
    result = []
    for ind in range(0, len(ts_index_pairs)-1):
        result.append((tss_index[ind], tss_index[ind + 1],\
                       tss_str[ind], tss_str[ind + 1]))
    return result

def diff(entry):
    if entry[0][0] < entry[1][0]:
        entry_current = entry[0]
        entry_successive = entry[1]
    else:
        entry_current = entry[1]
        entry_successive = entry[0]

    version_current = entry_current[0]
    sequence_current = entry_current[1]
    version_successive = entry_successive[0]
    sequence_successive = entry_successive[1]
    # used tokendiff instead of lcsdiff
    return version_current, version_successive,\
        tuple(tokendiff.token_diff(sequence_current, sequence_successive))


# end functions for task2 ------------------------

# functions to collect statistics ----------------

def collect_page_links(json_page):
    """Return a tuple (page, link)
    for each link found in page."""
    results = []
    page = json.loads(json_page)
    title = page['title']
    # structured_part: [name, text, link]
    for sp in page['structured_part']:
        if sp[2] is not None:
            results.append((title, sp[2]))
    # unstructured_part: [word, link, position]
    for un_sp in page['unstructured_part']:
        if un_sp[1] is not None:
            results.append((title, un_sp[1]))
    return results

def collect_page_revision(json_page):
    page = json.loads(json_page)
    title = page['title']
    timestamp = page['timestamp']
    return title, timestamp

def collect_statistics1(json_dir, statistics_dir=None):
    """Retrieve the json pages obtain form task1 and
    get the following statistics:

    1. number of revisions per page
    2. total number of unique links per page
    3. number of tokens per each revision of each page
    """
    if statistics_dir is None:
        statistics_dir = json_dir
    filename = "statistics.csv"
    statistics_filepath = statistics_dir + os.sep + filename

    # flush existing statistics file and add
    # headers
    with open(statistics_filepath, "w") as fh:
        csv_writer = csv.writer(fh, quoting=csv.QUOTE_NONNUMERIC)
        csv_writer.writerow(("page", "revs", "links", "avg_tokens"))

    
    json_files = [file_ for file_ in os.listdir(json_dir)\
                  if file_[-5:] == ".json"]
    
    for json_file in json_files:
        # create an rdd of json strings
        json_text_rdd = spark\
                        .read\
                        .text(json_dir + json_file)\
                        .rdd\
                        .map(lambda entry: entry.value)
        # obtain (P, timestamp, text, location)
        unstruct_parts_rdd = json_text_rdd.flatMap(collect_page_elements)

        # if a page has no revision, then the page doesn't exists
        # so no problem on nulls. This rdd will be used as
        # base structure (it cannot exist an entry without
        # a revision associated) in case other rdd will
        # be empty, in order to have a consistent csv with
        # all pages eventually
        page_revisions_rdd = json_text_rdd\
                             .map(collect_page_revision)\
                             .map(lambda entry:\
                                  ((entry[0], entry[1]), 1))\
                             .distinct()\
                             .map(lambda entry:\
                                  (entry[0][0], 1))\
                             .reduceByKey(lambda num_rev1,\
                                          num_rev2:\
                                          num_rev1 + num_rev2)
        # from the flatMap obtain (Page, link).
        # Since we are not grouping by <page, timestamp>,
        # but only by page, we ignore link appearance by
        # revisions, we are interested only in the
        # total amount of links for a given a page.
        #
        # This is a quality that describe the amount of information
        # connected (and therefore available) within the page.
        #
        # if a page has no links, the first map returns <Page, null>,
        # which makes problem when joining later.
        links_rdd = json_text_rdd\
                    .flatMap(collect_page_links)\
                    .map(lambda entry: ((entry[0], entry[1]), 1))\
                    .distinct()\
                    .map(lambda entry: (entry[0][0], entry[1]))\
                    .reduceByKey(lambda num_link1, num_link2:\
                                 num_link1 + num_link2)

        if links_rdd.isEmpty():
            links_rdd = page_revisions_rdd\
                        .map(lambda entry: (entry[0], 0))

        page_rev_tokens_rdd = unstruct_parts_rdd\
                              .map(lambda entry: (entry[0],\
                                                  1))\
                              .reduceByKey(lambda num_token1,\
                                           num_token2:\
                                           num_token1 + num_token2)
        if page_rev_tokens_rdd.isEmpty():
            page_rev_tokens_rdd = page_revisions_rdd\
                        .map(lambda entry: (entry[0], 0))
        
        page_statistics_rdd = page_revisions_rdd\
                              .join(links_rdd)\
                              .join(page_rev_tokens_rdd)\
                              .mapValues(lambda entry:\
                                         (entry[0][0],\
                                          entry[0][1],\
                                          math.ceil(entry[1] / entry[0][0])))
        with open(statistics_filepath, "a+") as fh:
            csv_writer = csv.writer(fh, quoting=csv.QUOTE_NONNUMERIC)
            for page, statistics in page_statistics_rdd.collect():
                contents = [page] + [stat for stat in statistics]
                csv_writer.writerow(contents)
    logging.info("Statistics printed in {}".format(statistics_filepath))

# end functions to collect statistics ------------

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
    for un_sp in page['unstructured_part']:
        if un_sp[1] is not None:
            results.append((title, timestamp, un_sp[1]))
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
    pwu.generate_stopwords()
    filtered_words = pwu.remove_stopword(words)
    for word in filtered_words:
        results.append((title, timestamp, word))
    return results

def versions_diff(iterable):
    """Given an iterable set containing timestamps
    for the same page, order them according to the timestamp,
    pick the timestamp two by two in sequence and compute
    the diff over the token.

    If words on a given location in two consecutive
    timestamps are different than it means
    that the word in the current timestamp has been
    deleted and the word in the new timestamp has been
    added.

    Return:
      A tuple
      <current_timestamp, next_timestamp, action_type, word>.

    Note:
      This method is not actually the one we used, due to
      its dumbness into noticing diffs.
      In case of a single addition, this method would notice
      diffs in each token following the effective diff element.

      eg: "I like Trento" <--> "I really really like Trento"
         diff-set: really really like Trento
      Which is very imprecise. Eventually we opted to use
      the difflib module provided by python.
    """
    list_iter = list(iterable)
    timestamps = [value[0] for value in list_iter]

    sorted_ts = arg_sort(timestamps)
    version_pairs = [(sorted_ts[t], sorted_ts[t+1]) \
                  for t in range(len(sorted_ts) - 1)]
    results = []
    for pair in version_pairs:
        current_index, successive_index = pair
        current = list_iter[current_index]
        successive = list_iter[successive_index]
        text_current = current[1]
        text_successive = successive[1]
        if text_current != text_successive:
            results.append((current[0], successive[0], "INS", text_successive))
            results.append((current[0], successive[0], "DEL", text_current))
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


            
# These are the schema used to save (and later retrieve)
# diff elements for task2
diff_elements_schema = StructType([\
                          StructField("action", StringType(), False),\
                          StructField("position", IntegerType(), False),
                          StructField("tokens", ArrayType(StringType()), False)])

diff_schema = StructType([\
                     StructField("page", StringType(), False),\
                     StructField("ver1", StringType(), False),\
                     StructField("ver2", StringType(), False),\
                     StructField("diff",\
                                 ArrayType(diff_elements_schema),\
                                 False)])         
if __name__ == "__main__":
    """Divide the wikipedia source into chunks of several
    pages, each chunk is processed within an RDD in order
    to parallelize the process of conversion into json files.
    """
    # initialization given and CLI arguments management
    args = cu.define_argparse().parse_args()
    spark_configs = cu.DEFAULT_SPARK_CONFIGURATIONS
    online_configs = cu.DEFAULT_ONLINE_CONFIGURATIONS
    if args.online_conffile:
        with open(args.online_conffile, "r") as conf_file:
            online_configs = cu.get_config(conf_file)
    wiki_version,\
        process_in_memory,\
        max_dump = cu.get_online_params(online_configs)

    # if a directory for the dumps is provided then
    # process all the dumps in it, otherwise
    # retrieve the dumps online
    if args.dump_dir:
        dump_dir = args.dump_dir
        get_dump = lambda: dm.get_offline_dump(dump_dir)
    else:
        get_dump = lambda: dm.get_online_dump(wiki_version,\
                                           max_dump,\
                                           process_in_memory)
    if args.spark_conffile:
        with open(args.spark_conffile, "r") as conf_file:
            spark_configs = cu.get_config(conf_file)
    
    spark = cu.spark_builder(SparkSession.builder, spark_configs)
    sc = spark.sparkContext

    json_dir = "/tmp/json/"
    if not os.path.exists(json_dir):
        os.makedirs(json_dir)
    diffs_dir = "/tmp/diffs/"
    if not os.path.exists(diffs_dir):
        os.makedirs(diffs_dir)

    # TASK1
    dump_to_json(sc, get_dump, json_dir)
    #collect_statistics1(json_dir)

    # TASK2.1

    # read json created previously in json_dir
    json_files = [file_ for file_ in os.listdir(json_dir)\
                  if file_[-5:] == ".json"]
    
    for json_file in json_files:

        # create an rdd of json strings
        json_text_rdd = spark\
                        .read\
                        .text(json_dir + json_file)\
                        .rdd\
                        .map(lambda entry: entry.value)
        
        # from json to <page, timestamp, text, location>
        unstruct_parts_rdd = json_text_rdd.flatMap(collect_page_elements)

        # SUBTASK: Indexing timestamps -------

        # obtain <P, [(t1_str, t1_int), ..., (tn_str, t2_int)]>
        # where timestamps are sorted
        timestamp_indexer_rdd = unstruct_parts_rdd\
                                .map(lambda entry: (entry[0],\
                                                    (entry[1],)))\
                                .distinct()\
                                .reduceByKey(lambda t1_list, t2_list:\
                                             tuple(list(t1_list) +\
                                                   list(t2_list)))
        str2int_rdd = timestamp_indexer_rdd\
                      .flatMapValues(indexer)\
                      .map(lambda entry: ((entry[0], entry[1][0]),\
                                          entry[1][1]))

        # by joining the two rdds we obtain
        #     (<P, TimestampString>, ((text, location), TimestampIndex))
        # we want to replace TimestampString with TimestampIndex
        # that's what the last map does
        vectorized_rdd = unstruct_parts_rdd\
                         .map(lambda entry: ((entry[0], entry[1]),\
                                             (entry[2], entry[3])))\
                         .join(str2int_rdd)\
                         .map(lambda entry: ((entry[0][0], entry[1][1]),\
                                             entry[1][0]))
        # END-SUBTASK ------------------------
        # we now have
        #    (<P, TimestampIndex>, (text, location))

        words_timestamp_rdd = vectorized_rdd\
                              .map(lambda entry: (entry[0], (entry[1],)))\
                              .reduceByKey(lambda entry1, entry2:\
                                           tuple(list(entry1) + list(entry2)))

        ts_rdd = words_timestamp_rdd.\
                 map(lambda entry: \
                     ((entry[0][0], entry[0][1]),\
                      (entry[0][1], entry[1])))

        ts_successive_rdd = words_timestamp_rdd.\
                            map(lambda entry:\
                                ((entry[0][0], entry[0][1] + 1),\
                                 (entry[0][1], entry[1])))

        # TODO: check it out
        consecutive_ts_rdd = ts_rdd\
                             .leftOuterJoin(ts_successive_rdd)\
                             .filter(lambda entry:\
                                     entry[1][0] is not None and\
                                     entry[1][1] is not None)

        # obtain diff objects:
        # <P, timestampIndex1, timestampIndex2, diff-list>
        diff_object_rdd = consecutive_ts_rdd\
                          .mapValues(diff)\
                          .map(lambda entry:\
                               ((entry[0][0], entry[1][0], entry[1][1]),\
                                entry[1][2]))

        # SUBSTASK: reversing timestamp index ----

        int2str_rdd = timestamp_indexer_rdd\
                      .mapValues(indexer)\
                      .flatMapValues(gen_pair_indexer)\
                      .map(lambda entry:\
                           ((entry[0], entry[1][0], entry[1][1]),\
                            (entry[1][2], entry[1][3])))

        final_diff_rdd = int2str_rdd\
              .join(diff_object_rdd)\
              .map(lambda entry:\
                   (entry[0][0], entry[1][0][0], entry[1][0][1],\
                   entry[1][1]))
        
        # END SUBTASK ----------------------------
        result_df = final_diff_rdd.toDF(diff_schema)
        # keep in mind we are assuming to work with only one page,
        # if this is not the case this file will contain more pages
        # differences (although the diff operation can take into
        # account different pages)
        filename = json_file[:-5]
        with open(diffs_dir +\
                  "diff_{}.json".format(filename), "w") as fh:
            for i in result_df.toJSON().collect():
                fh.write(str(i) + "\n")
    # END TASK2.1 --------------------------------

    """
    # TASK2.2: entity diff and word diff without location
    for json_file in json_files:

        # create an rdd of json strings
        json_text_rdd = spark\
                        .read\
                        .text(json_dir + json_file)\
                        .rdd\
                        .map(lambda entry: entry.value)
        
        links_rdd = json_text_rdd.flatMap(collect_links)
        pair_links_rdd = links_rdd.map(lambda entry: ((entry[0], entry[1]), entry[2]))

        # collect all links within a page in a single rdd
        links_page_rdd = pair_links_rdd\
                         .groupByKey()\
                         .mapValues(lambda entry_values: list(entry_values))

        # relax the structure and change the
        # pair rdd key-value values
        relaxed_rdd = links_page_rdd.map(lambda entry: (entry[0][0], entry[0][1], entry[1]))

        # the new pair rdd will have the following structure:
        # <page, <timestamp, words>>
        page_versions_rdd = relaxed_rdd.map(lambda entry: (entry[0], (entry[1], entry[2])))
        links_diff_rdd = page_versions_rdd.groupByKey().flatMapValues(time_diff)

        # WORDS DIFF
        # select all the words within a page and
        # build a large tuple set
        words_rdd = json_text_rdd.flatMap(collect_words)
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

        filename = json_file[:-5]
        with open("/tmp/diff2_{}.txt".format(filename), "w") as fh:
                for i in links_diff_rdd.collect():
                    fh.write(str(i) + "\n")
        
        with open("/tmp/diff2_{}.txt".format(filename), "w") as fh:
            for i in words_diff_rdd.collect():
                fh.write(str(i) + "\n")
    """
