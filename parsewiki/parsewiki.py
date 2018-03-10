import sys
import parsepage as pp
import bz2
import xml.etree.ElementTree as ET

def load_infoboxes(infobox_source_file):
    """Retrieve list of templates representing infoboxes from
    file.""" 
    infoboxes = set()
    with open(infobox_source_file, "r") as infobox_fh:
        for line in infobox_fh:
            infoboxes.add(line.strip())
    return infoboxes

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

def get_wikipage(xml_wikipage):
    parsed_page = ET.fromstring(xml_wikipage)
    title = parsed_page.find('title').text
    timestamp = parsed_page.find('revision/timestamp').text
    plain_wikitext = parsed_page.find('revision/text').text
    wikitext = pp.pfh.parse(plain_wikitext)
    page = pp.Page.parse_page(wikitext, title, timestamp)
    return page

def parse_single_wikipage(filename):
    with open(filename, "r") as fh:
        str_content = fh.read()
    content = pp.pfh.parse(str_content)
    mw_page = pp.Page.parse_page(content)
    return mw_page
   

if __name__ == "__main__":
    """
    page = parse_single_wikipage(sys.argv[1])
    print(page.to_json())
    """
    fh = bz2.open(sys.argv[1], "rt") # read in text mode
    remainder = ""
    for wikipage in bzip2_page_iter(sys.argv[1]):
        page = get_wikipage(wikipage)
        print(page.to_json())

    
