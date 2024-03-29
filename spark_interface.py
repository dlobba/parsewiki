import os
import json
import csv
import logging
import re
import math
import time

import parsewiki.diff as tokendiff
import cli_utils as cu
import parsewiki.utils as pwu
import dumpmanager as dm

from collections import OrderedDict
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, ArrayType, IntegerType
from pyspark.sql.functions import size

logging.getLogger().setLevel(logging.INFO)


def plog(log_path, string):
    with open(log_path, "a+") as fh:
        fh.write("{} -- {}\n".format(string,\
                                     str(time.time())))

# functions for task1 ----------------------------


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


def dump_to_json(spark_session, dump_iter, json_dir):
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
    spark_context = spark_session.sparkContext
    for dump in dump_iter():
        for chunk in pwu.get_wikipedia_chunk(dump,\
                                             max_numpage=1,\
                                             max_iteration=None):
            chunk_rdd = spark_context.parallelize(chunk, numSlices=20)
            json_text_rdd = chunk_rdd.map(jsonify)
            json_heading_rdd = json_text_rdd\
                               .map(collect_heading)
            chunk_rdd.unpersist()
            json_text_rdd.unpersist()
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
            json_heading_rdd.unpersist()

            # create (flushing) a new file for each page
            filename_assoc = {title:0 for title in page_titles}
            for page_title in page_titles:
                filename = str.join("_", [word\
                                       for word in re.split("\W", page_title.lower())\
                                       if word != ""])
                filename_assoc[page_title] = filename
                with open(json_dir + "{}.json".format(filename), "w") as fh:
                    pass

            for page in json_pages:
                page_title = page[0][0]
                filename = filename_assoc[page_title]
                with open(json_dir + "{}.json".format(filename), "a") as fh:
                    fh.write(str(page[1]) + "\n")

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
    for ind in range(0, len(ts_index_pairs) - 1):
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


def compute_diff_set(spark_session, json_dir, diffs_dir):
    # TASK2
    # read json created previously in json_dir
    json_files = [file_ for file_ in os.listdir(json_dir)\
                  if file_[-5:] == ".json"]

    for json_file in json_files:

        # create an rdd of json strings
        json_text_rdd = spark_session\
                        .read\
                        .text(json_dir + json_file)\
                        .rdd\
                        .map(lambda entry: entry.value)

        # from json to <page, timestamp, text, location>
        unstruct_parts_rdd = json_text_rdd.flatMap(collect_page_elements)
        json_text_rdd.unpersist()
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

        unstruct_parts_rdd.unpersist()
        str2int_rdd.unpersist()
        # END-SUBTASK ------------------------
        # we now have
        #    (<P, TimestampIndex>, (text, location))
        words_timestamp_rdd = vectorized_rdd\
                              .map(lambda entry: (entry[0], (entry[1],)))\
                              .reduceByKey(lambda entry1, entry2:\
                                           tuple(list(entry1) + list(entry2)))
        vectorized_rdd.unpersist()

        ts_rdd = words_timestamp_rdd.\
                 map(lambda entry: \
                     ((entry[0][0], entry[0][1]),\
                      (entry[0][1], entry[1])))

        ts_successive_rdd = words_timestamp_rdd.\
                            map(lambda entry:\
                                ((entry[0][0], entry[0][1] + 1),\
                                 (entry[0][1], entry[1])))

        consecutive_ts_rdd = ts_rdd\
                             .leftOuterJoin(ts_successive_rdd)\
                             .filter(lambda entry:\
                                     entry[1][0] is not None and\
                                     entry[1][1] is not None)
        ts_rdd.unpersist()
        ts_successive_rdd.unpersist()
        words_timestamp_rdd.unpersist()
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
        int2str_rdd.unpersist()
        diff_object_rdd.unpersist()

        # END SUBTASK ----------------------------
        result_df = final_diff_rdd.toDF(diff_schema)
        # keep in mind we are assuming to work with only one page,
        # if this is not the case this file will contain more pages
        # differences (although the diff operation can take into
        # account different pages)
        filename = json_file[:-5]
        with open(diffs_dir +\
                  "{}_diff.json".format(filename), "w") as fh:
            for i in result_df.toJSON().collect():
                fh.write(str(i) + "\n")
    # END TASK2 ----------------------------------

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


def collect_statistics1(spark_session, json_dir, statistics_dir=None):
    """Retrieve the json pages obtain form task1 and
    get the following statistics:

    1. number of revisions per page
    2. total number of unique links per page
    3. number of tokens per each revision of each page
    """
    if statistics_dir is None:
        statistics_dir = json_dir
    filename = "statistics1.csv"
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
        json_text_rdd = spark_session\
                        .read\
                        .text(json_dir + json_file)\
                        .rdd\
                        .map(lambda entry: entry.value)
        # if a page has no revision, then the page doesn't exists
        # so no problem on nulls. This rdd will be used as
        # base structure (it cannot exist an entry without
        # a revision associated) in case other rdd will
        # be empty, in order to have a consistent csv with
        # all pages eventually
        page_revisions_rdd = json_text_rdd\
                             .map(collect_page_revision)\
                             .map(lambda entry:\
                                  (entry[0], 1))\
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
                    .map(lambda entry: (entry[0], entry[1]))\
                    .distinct()\
                    .map(lambda entry: (entry[0], 1))\
                    .reduceByKey(lambda num_link1, num_link2:\
                                 num_link1 + num_link2)

        if links_rdd.isEmpty():
            links_rdd = page_revisions_rdd\
                        .map(lambda entry: (entry[0], 0))

        # obtain (P, timestamp, text, location)
        unstruct_parts_rdd = json_text_rdd.flatMap(collect_page_elements)
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
        page_revisions_rdd.unpersist()
        links_rdd.unpersist()
        page_rev_tokens_rdd.unpersist()
        json_text_rdd.unpersist()
        unstruct_parts_rdd.unpersist()
        with open(statistics_filepath, "a+") as fh:
            csv_writer = csv.writer(fh, quoting=csv.QUOTE_NONNUMERIC)
            for page, statistics in page_statistics_rdd.collect():
                contents = [page] + [stat for stat in statistics]
                csv_writer.writerow(contents)
    logging.info("Statistics printed in {}".format(statistics_filepath))

# end functions to collect statistics1 -----------

# begin functions to collect statistics2 ---------


def count_page_diffs(diff_set):
    """Given a a diff-set, of the type (action, position, tokens)
    describing the difference set, count the number of
    insertions and deletions and for each type of actions
    the amount of tokens involved.
    """
    tokens_added = 0
    tokens_removed = 0
    num_ins = 0
    num_dels = 0
    for action, _, tokens in diff_set:
        if action == "INS":
            num_ins += 1
            tokens_added += len(tokens)
        else:
            num_dels += 1
            tokens_removed += len(tokens)
    return (num_ins, num_dels, tokens_added, tokens_removed)


def merge_count_statistics(stat1, stat2):
    stat1 = list(stat1)
    stat2 = list(stat2)
    return tuple(stat1[i] + stat2[i] for i in range(0, len(stat1)))


def collect_statistics2(spark_session, diffs_dir, statistics_dir=None):
    if statistics_dir is None:
        statistics_dir = diffs_dir
    filename = "statistics2.csv"
    statistics_filepath = statistics_dir + os.sep + filename
    # flush existing statistics file and add
    # headers
    with open(statistics_filepath, "w") as fh:
        csv_writer = csv.writer(fh, quoting=csv.QUOTE_NONNUMERIC)
        csv_writer.writerow(("page", "inss", "dels", "added_tokens",\
                             "removed_tokens"))

    json_files = [file_ for file_ in os.listdir(diffs_dir)\
                  if file_[-5:] == ".json"]

    for json_file in json_files:
        # create an dataframe with json strings
        diffs_df = spark_session\
                   .read\
                   .json(diffs_dir + json_file, diff_schema)
        # remove entries without any edits
        # and return an rdd (of rows, so we could access
        # fields by their name)
        diff_df = diffs_df.filter(size("diff") != 0)
        diff_stat_rdd = diff_df.select("page", "ver1", "ver2", "diff")\
                               .rdd\
                               .map(lambda entry: ((entry.page,\
                                                    entry.ver1, entry.ver2),\
                                                   entry.diff))\
                               .mapValues(count_page_diffs)
        # obtain <<P, ver1, ver2>, <inss, dels, t_added, t_removed>>

        # merge revision statistics by page
        page_statistics_rdd = diff_stat_rdd\
                              .map(lambda entry: (entry[0][0], entry[1]))\
                              .reduceByKey(merge_count_statistics)

        diff_stat_rdd.unpersist()
        with open(statistics_filepath, "a+") as fh:
            csv_writer = csv.writer(fh, quoting=csv.QUOTE_NONNUMERIC)
            for page, statistics in page_statistics_rdd.collect():
                contents = [page] + [stat for stat in statistics]
                csv_writer.writerow(contents)

# end functions to collect statistics 2 ----------

# begin function to count entity (task 3) --------
def count_entities_revison(spark_session, json_dir, count_dir):
    json_files = [file_ for file_ in os.listdir(json_dir)\
                  if file_[-5:] == ".json"]

    for json_file in json_files:

        # create an rdd of json strings
        json_text_rdd = spark_session\
                        .read\
                        .text(json_dir + json_file)\
                        .rdd\
                        .map(lambda entry: entry.value)
        links_rdd = json_text_rdd.flatMap(collect_links)
        json_text_rdd.unpersist()
        # use P1, P2, revision as key and associate multiple links
        count_links = links_rdd\
                      .map(lambda entry: ((entry[0], entry[1], entry[2]), 1))\
                      .reduceByKey(lambda v1,v2: v1+v2)\
                      .map(lambda entry: (entry[0][0], entry[0][2], entry[0][1], entry[1]))
        # transform rdd to DF
        count_df = count_links.toDF(count_schema)
        count_links.unpersist()
        
        # write DF to json file
        filename = json_file[:-5]
        with open(count_dir + "{}_count.json".format(filename), "w") as fh:
            for i in count_df.toJSON().collect():
                fh.write(str(i) + "\n")


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

# end function to count entity (task 3)


# These are the schema used to save (and later retrieve)
# diff elements for task2
diff_elements_schema = StructType([\
                          StructField("action", StringType(), False),\
                          StructField("position", IntegerType(), False),
                          StructField("tokens", ArrayType(StringType()),\
                                      False)])

diff_schema = StructType([\
                     StructField("page", StringType(), False),\
                     StructField("ver1", StringType(), False),\
                     StructField("ver2", StringType(), False),\
                     StructField("diff",\
                                 ArrayType(diff_elements_schema),\
                                 False)])

count_schema = StructType([\
                     StructField("P1", StringType(), False),\
                     StructField("P2", StringType(), False),\
                     StructField("revision", StringType(), False),\
                     StructField("count", IntegerType(), False)])

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
        get_dump = lambda: dm.get_online_dump(wiki_version, max_dump, process_in_memory)
    if args.spark_conffile:
        with open(args.spark_conffile, "r") as conf_file:
            spark_configs = cu.get_config(conf_file)

    spark = cu.spark_builder(SparkSession.builder, spark_configs)

    data_dest_dir = "/tmp/"
    json_dir = data_dest_dir + "wikipage_json/"
    if not os.path.exists(json_dir):
        os.makedirs(json_dir)
    diffs_dir = data_dest_dir + "diffs_json/"
    if not os.path.exists(diffs_dir):
        os.makedirs(diffs_dir)
    count_dir = data_dest_dir + "count_entity_json/"
    if not os.path.exists(count_dir):
        os.makedirs(count_dir)

    # TASK1
    log_file_path = data_dest_dir + "exec.log"

    plog(log_file_path, "Start")

    plog(log_file_path, "BEGIN-TASK1")
    dump_to_json(spark, get_dump, json_dir)
    plog(log_file_path, "END-TASK1")

    # STATISTICS1
    plog(log_file_path, "BEGIN-STAT1")
    collect_statistics1(spark, json_dir, data_dest_dir)
    plog(log_file_path, "END-STAT1")

    # TASK2
    plog(log_file_path, "BEGIN-TASK2")
    compute_diff_set(spark, json_dir, diffs_dir)
    plog(log_file_path, "END-TASK2")

    # STATISTICS2
    plog(log_file_path, "BEGIN-STAT2")
    collect_statistics2(spark, diffs_dir, data_dest_dir)
    plog(log_file_path, "END-STAT2") 

    # TASK3
    plog(log_file_path, "BEGIN-TASK3")
    count_entities_revison(spark, json_dir, count_dir)
    plog(log_file_path, "END-TASK3")
    
    plog(log_file_path, "End")
