import bz2
import logging
import xml.etree.ElementTree as ET
import parsewiki.parsepage as pp


def split_bzip2(bzip2_file, num_pages, max_iteration, file_prefix="chunks_"):
    """Given a bz2 file, split it into several bz2 files,
    each one with a given maximum number of pages."""
    page_count = 0
    iteration_count = 0
    chunks = []
    for wikipage in bzip2_page_iter(bzip2_file):
        chunks.append(wikipage)
        page_count += 1
        if page_count >= num_pages:
            iteration_count += 1
            chunks.insert(0, "<mediawiki>\n")
            chunks.append("\n</mediawiki>\n")
            chunk_file_name = file_prefix + \
                str(iteration_count) + ".xml"
            with open(chunk_file_name, "w") as fh:
                fh.write(str.join("", chunks))
            chunks = []
            page_count = 0
        if iteration_count >= max_iteration:
            break


def page_iter(text_stream):
    """Given a generic text stream,
    copy all the content within `<page> ... </page>`
    tags."""
    wikipage = []
    read = False
    start_word = "<page"
    end_word = "</page"
    searched_word = start_word
    tmp = ""
    for line in text_stream:
        for char in line:
            tmp += char
            if not searched_word.startswith(tmp):
                tmp = ""
            else:
                if tmp == start_word:
                    read = True
                    start_word_char = [c for c in start_word]
                    # "e" is been reading now, so [:-1]
                    wikipage.extend(start_word_char[:-1])
                    searched_word = end_word
                    tmp = ""
                elif end_word == tmp:
                    read = False
                    try:
                        wikipage.extend(("e", ">"))
                        searched_word = start_word
                        # return partial result and reset
                        # objective
                        if len(wikipage) > len(end_word):
                            yield str.join("", wikipage)
                    except MemoryError as me:
                        logging.warning("MemoryError: The wikipage has been ignored. Continuing...")
                    wikipage = []
            if read is True:
                try:
                    wikipage.append(char)
                except MemoryError as me:
                        logging.warning("MemoryError: The wikipage will be ignored. Continuing...")
                        read = False
                        wikipage = []

def bzip2_page_iter(bz2_filename):
    """Given a bzip2 filename,
    copy all the content within `<page> ... </page>`
    tags."""
    with bz2.open(bz2_filename, "rt") as bz2_fh:
        for page in page_iter(bz2_fh):
            yield page


def bzip2_memory_page_iter(stream_dump):
    """Given a streaming dump, decode it and
    retrieve the pages it contains."""
    decoded_dump = bz2.decompress(stream_dump).decode()
    for page in page_iter(decoded_dump):
        yield page


def iter_revisions(xml_wikipage):
    """Given a wikidump page structure, return
    all the given revisions inside it.

    If the revision is not of type
    `text/x-wiki` then it won't be taken
    into account.

    Notes:
      The number of revisions within a page can
      be very large..."""
    parsed_page = ET.fromstring(xml_wikipage)
    title = parsed_page.find('title').text
    for revision in parsed_page.iterfind('revision'):
        content_format = revision.find('format').text.strip()
        if content_format != "text/x-wiki":
            logging.info("Incorrect content format found, " +
                         "it will be **ignored**" +
                         "found {}".format(content_format))
            break
        timestamp = revision.find('timestamp').text
        contributor = revision.find('contributor').find('username')
        if contributor is not None:
            contributor = str(contributor.text).strip()
        plain_wikitext = revision.find('text').text
        yield (title, timestamp, contributor, plain_wikitext)


def parse_single_wikipage(filename):
    """Given a file containing just a single page
    of wikicode, parse it and return his Page
    representation."""
    with open(filename, "r") as fh:
        str_content = fh.read()
    content = pp.pfh.parse(str_content)
    mw_page = pp.Page.parse_page(content)
    return mw_page

def wikipage_to_json(wikitext, title=None, timestamp=None, contributor=None):
    content = pp.pfh.parse(wikitext)
    mw_page = pp.Page.parse_page(content, title, timestamp, contributor)
    return mw_page.to_json()


def get_wikipedia_chunk(bzip2_source, max_numpage=5, max_iteration=1):
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
    if max_numpage <= 0:
        raise ValueError("Insufficient number of pages specified.")
    if max_iteration is not None and max_iteration <= 0:
        raise ValueError("Insufficient number of iterations specified.")
    num_iteration = 0
    num_page = 0
    pages = []
    # the method used to iterate over pages
    # ALERT: pointer to function here!
    page_iterator = bzip2_memory_page_iter
    # if a filename is given then process as a bz2 file
    if type(bzip2_source) == str:
        page_iterator = bzip2_page_iter
    for wikipage in page_iterator(bzip2_source):
        for revision in iter_revisions(wikipage):
            pages.append(revision)
            # remember that revision is a tuple
            # (title, timestamp, contributor, plain_wikitext)
        num_page += 1
        if num_page >= max_numpage:
            yield pages
            pages = []
            num_page = 0
            num_iteration += 1
        if max_iteration and num_iteration >= max_iteration:
            break
    if len(pages) > 0:
        yield pages
