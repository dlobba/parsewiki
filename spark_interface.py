import findspark
findspark.init("/home/gabriel/Scrivania/UNI/Master/17-18/BigDataSocialNetwork/spark-2.2.0-bin-hadoop2.7")
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import parsewiki.utils as pwu
import requests
import json
import hashlib
import sys

# TODO: search for what those True values are meant for
schema = StructType([\
                     StructField("title", StringType(), True),\
                     StructField("link", StringType(), True)])

def get_wikipedia_chunk(bzip2_file, max_numpage=20, max_iteration=None):
    """Return a bzip2 file containing wikipedia pages in chunks
    of a given number of pages.

    It's possibile to limit the parsing by indicating the number
    of iteration for the process to be repeated. If this is `None`
    then all the bzip2 file is processed.

    Params:
      bzip2_file (str): path to the bzip2 file
      max_numpage (int): length of each chunk
      max_iteration (int): number of iterations of the process
    """
    num_iteration = 0
    num_page = 0
    pages = []
    for wikipage in pwu.bz2_memory_page_iter(bzip2_file):
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


if __name__ == "__main__":
    """Divide the wikipedia source into chunks of several
    pages, each chunk is processed within an RDD in order
    to parallelize the process of conversion into json files.
    """
    spark = SparkSession.builder\
                        .master("local")\
                        .appName("WikiCrunching")\
                        .config("spark.executor.memory", "1g")\
                        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.coll") \
                        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.coll") \
                        .getOrCreate()

    # conf = SparkConf()
    # conf.setMaster("local")
    # conf.setAppName(")
    # conf.set("spark.executor.memory", "1g")
    # sc = SparkContext(conf=conf)
    sc = spark.sparkContext
    pair_rdd = spark.createDataFrame(sc.emptyRDD(), schema).rdd
    json_text_rdd = spark.createDataFrame(sc.emptyRDD(), schema).rdd

    wiki_version = "20180201"
    status_url = "http://wikimedia.bytemark.co.uk/wikidatawiki/"+wiki_version+"/dumpstatus.json"
    status_json = requests.get(status_url).json()
    dumps = status_json['jobs']['articlesdump'] # testing dataset (no revision)
    #dumps = status_json['jobs']['metahistory7zdump'] # full dataset (70TB)
    if dumps['status'] != "done":
        print("Dump not ready\n")
        exit(-1)
    files = []
    for key in dumps['files'].keys():
        files.append({'url': dumps['files'][key]['url'], 'md5': dumps['files'][key]['md5'], 'size': dumps['files'][key]['size']})
    files.sort(key=lambda file: file['size'])

    for dump in files[:2]: #remove [:1] to downoad whole wiki
        url = "http://wikimedia.bytemark.co.uk"+dump['url']
        bz2_dump = requests.get(url).content
        if hashlib.md5(bz2_dump).hexdigest() != dump['md5']: #fix it using haslib
            break #TODO: MANAGE ERRORS
        # with open("wikidatawiki-20171103-pages-articles19.xml-p19072452p19140743.bz2", 'rb') as dump:
        #     bz2_dump = dump.read()
        for chunk in get_wikipedia_chunk(bz2_dump, max_numpage=20, max_iteration=10):
            rdd = sc.parallelize(chunk)
            #rdd.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()
            json_text_rdd = rdd.map(jsonify)
            #json_text_rdd = json_text_rdd.union(rdd.map(jsonify))
            # Note: here json_text_rdd contains
            # one line of json for each wikipage
            # so it's not a problem either if we
            # would like to work on it as a single
            # line or if we want to parse is as a
            # single line json string (single line json
            # and multiline json are treated differently
            # by spark)

            # json_df = spark.read.json(json_text_rdd)
            # z = json_df.select('title').rdd.collect()
            # for i in z:
            #     print(i)

            # try to read each line individually
            pair_rdd = pair_rdd.union(json_text_rdd.flatMap(collect_links))
    #pairs_csv = pair_rdd.map(lambda title_link_pair: str.join(";", title_link_pair))
    pair_list = pair_rdd.collect()
    for i in pair_list:
        print(i)
    json_text = json_text_rdd.collect()
    print(len(json_text))
