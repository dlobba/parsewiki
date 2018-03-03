import sys
import parsepage as pp

infobox_file = "template.txt"

def load_infoboxes(infobox_source_file):
    """Retrieve list of templates representing infoboxes from
    file.""" 
    infoboxes = set()
    with open(infobox_source_file, "r") as infobox_fh:
        for line in infobox_fh:
            infoboxes.add(line.strip())
    return infoboxes

infoboxes = load_infoboxes(infobox_file)

if __name__ == "__main__":
    with open(sys.argv[1], "r") as fh:
        str_content = fh.read()
        
    content = pp.pfh.parse(str_content)
    mw_page = pp.Page.parse_page(content, infoboxes=pp.infoboxes)
    print(mw_page.to_json())
