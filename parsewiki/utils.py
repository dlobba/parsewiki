import parsewiki.parsepage as pp
import bz2
import xml.etree.ElementTree as ET

def split_bzip2(bzip2_file, num_pages, max_iteration, file_prefix="chunks_"):
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

def bzip2_page_iter(filename):
    """Given the stream copy all the content
    withing `<page> ... </page>` tags."""
    wikipage = []
    read = False
    start_word = "<page"
    end_word = "</page"
    searched_word = start_word
    tmp = ""
    with bz2.open(filename, "rt") as fh:
        for line in fh:
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

def iter_revisions(xml_wikipage):
    parsed_page = ET.fromstring(xml_wikipage)
    title = parsed_page.find('title').text
    for revision in parsed_page.iterfind("revision"):
        timestamp = revision.find('timestamp').text
        plain_wikitext = revision.find('text').text
        wikicode = pp.pfh.parse(plain_wikitext)
        page = pp.Page.parse_page(wikicode, title, timestamp)
        yield page

def parse_single_wikipage(filename):
    """Given a file containing just a single page
    of wikicode, parse it and return his Page
    representation."""
    with open(filename, "r") as fh:
        str_content = fh.read()
    content = pp.pfh.parse(str_content)
    mw_page = pp.Page.parse_page(content)
    return mw_page

