from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import parsewiki.utils as pwu
import json
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
    for wikipage in pwu.bzip2_page_iter(bzip2_file):
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
                        .getOrCreate()

    # conf = SparkConf()
    # conf.setMaster("local")
    # conf.setAppName(")
    # conf.set("spark.executor.memory", "1g")
    # sc = SparkContext(conf=conf)
    sc = spark.sparkContext
    
    pairs_rdd = spark.createDataFrame(sc.emptyRDD(), schema).rdd
    for chunk in get_wikipedia_chunk(sys.argv[1], max_numpage=20, max_iteration=3):
        rdd = sc.parallelize(chunk)
        json_text_rdd = rdd.map(jsonify)
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
        current_pair_rdd = json_text_rdd.flatMap(collect_links)

        # TODO: be sure that by doing this it would still work
        # on clusters, since we are augmenting an rdd by
        # itself...
        pairs_rdd = sc.union([pairs_rdd, current_pair_rdd])

    pairs_csv = pairs_rdd.map(lambda title_link_pair: str.join(";", title_link_pair))
    pairs_csv.saveAsTextFile("../out")
