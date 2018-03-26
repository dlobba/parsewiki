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
                          str(iteration_count) + \
                          ".xml"
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
                    wikipage.extend(("e", ">"))
                    searched_word = start_word
                    # return partial result and reset
                    # objective
                    yield str.join("", wikipage)
                    wikipage = []
            if read is True:
                wikipage.append(char)

def bzip2_page_iter(bz2_filename):
    """Given a bzip2 filename,
    copy all the content within `<page> ... </page>`
    tags."""
    with bz2.open(bz2_filename, "rt") as bz2_fh:
        for page in  page_iter(bz2_fh):
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
    for revision in parsed_page.iterfind("revision"):
        content_format = revision.find('format').text.strip()
        if content_format != "text/x-wiki":
            logging.info("Incorrect content format found, " +\
                          "it will be **ignored**" +\
                          "found {}".format(content_format))
            break
        timestamp = revision.find('timestamp').text
        plain_wikitext = revision.find('text').text
        yield (title, timestamp, plain_wikitext)

def parse_single_wikipage(filename):
    """Given a file containing just a single page
    of wikicode, parse it and return his Page
    representation."""
    with open(filename, "r") as fh:
        str_content = fh.read()
    content = pp.pfh.parse(str_content)
    mw_page = pp.Page.parse_page(content)
    return mw_page

def wikipage_to_json(wikitext, title=None, timestamp=None):
    content = pp.pfh.parse(wikitext)
    mw_page = pp.Page.parse_page(content, title, timestamp)
    return mw_page.to_json()
