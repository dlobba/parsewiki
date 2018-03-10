import sys
import parsepage as pp
import bz2

def load_infoboxes(infobox_source_file):
    """Retrieve list of templates representing infoboxes from
    file.""" 
    infoboxes = set()
    with open(infobox_source_file, "r") as infobox_fh:
        for line in infobox_fh:
            infoboxes.add(line.strip())
    return infoboxes

def get_next_wikipage(filehandler, line=""):
    """Given the stream copy all the content
    withing `<page> ... </page>` tags."""
    wikipage = []
    read = False
    page_finished = False
    start_word = "<page"
    end_word = "</page"
    searched_word = start_word
    tmp = ""
    while filehandler and \
        page_finished is False:
        for char in line:
            tmp += char
            if not searched_word.startswith(tmp):
                tmp = ""
            else:
                if start_word == tmp:
                    read = True
                    start_word_char = [c for c in start_word]
                    # "e" has been reading now, so [:-1]
                    wikipage.extend(start_word_char[:-1])
                    tmp = ""
                    searched_word = end_word
                elif end_word == tmp:
                    read = False
                    page_finished = True
            if read is True:
                wikipage.append(char)
        line = filehandler.readline()
    wikipage.extend(("e", ">")) # terminate <page> tag
    return str.join("", wikipage)

if __name__ == "__main__":
    """with open(sys.argv[1], "r") as fh:
        str_content = fh.read()
        
       infoboxes = None
    content = pp.pfh.parse(str_content)
    mw_page = pp.Page.parse_page(content, infoboxes=infoboxes)
    print(mw_page.to_json())
    """
    fh = bz2.open(sys.argv[1], "rt") # read in text mode
    page = get_next_wikipage(fh)
    fh.close()

    print(page)
