from pyspark import SparkConf, SparkContext
import parsewiki.utils as pwu
import sys

def get_wikipedia_chunk(bzip2_file, max_numpage=20):
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
    if len(pages) > 0:
        yield pages

def jsonify(wiki_tuple):
    title, timestamp, page = wiki_tuple
    return pwu.wikipage_to_json(page, title, timestamp)

if __name__ == "__main__":
    """Divide the wikipedia source into chunks of several
    pages, each chunk is processed within an RDD in order
    to parallelize the process of conversion into json files.
    """
    conf = SparkConf()
    conf.setMaster("local")
    conf.setAppName("WikiCrunching")
    conf.set("spark.executor.memory", "1g")

    sc = SparkContext(conf=conf)

    for chunk in get_wikipedia_chunk(sys.argv[1]):
        rdd = sc.parallelize(chunk)
        json = rdd.map(jsonify)
        for x in json.collect():
            print(x)
