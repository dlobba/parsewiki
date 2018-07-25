import sys
import re
import os

import parsewiki.utils as pwu

def get_title(page):
    title_regex = r"<title>([^<]+)</title>"
    match = next(re.finditer(title_regex, page), None)
    if match is not None:
        return match.group(1)

def interactive_selection(page, len_excerpt=1000):
    title = get_title(page)
    print("Page: {}, length: {} chars\n Excerpt: {}\n"\
          .format(match.group(1),\
                  len(page),
                  page[0:len_excerpt]))
    keep = input("Do you want to keep it?")
    if not keep.startswith(("n", "N")):
        return True
    return False

def heuristic_selection(page, min_length, exclude_patterns=[]):
    title = get_title(page)
    if len(page) < min_length:
        return False
    for exclude_pattern in exclude_patterns:
        if next(re.finditer(exclude_pattern, title),\
                None) is not None:
            return False
    return True

def main(args):
    bzip_path = args[1]
    destination_path = args[2]
    if destination_path[-4:] == ".bz2":
        print("You are probably rewriting the bzip!\nTerminating")
        sys.exit()
    print(bzip_path)

    part = 0
    dest_file = open(destination_path + os.sep + "part" + str(part) + ".xml", "w")
    dest_file.write("<mediawiki>\n")

    # 1 python char is approximately 2 byte
    chars_gigabyte = int(10**9 / 2) # approximately (a lot!)
    dimension = 1 # not 0 to avoid division by zero later on
    last_page = "<enter last page here>"
    skip_to_last = False
    for wikipage in pwu.bzip2_page_iter(bzip_path):
        try:
            title = get_title(wikipage)
            if skip_to_last is True:
                if title == last_page:
                    skip_to_last = False
            else:
                picked = heuristic_selection(wikipage,\
                                             200000,\
                                             ("User:",\
                                              "Talk:"))
                if picked is True:
                    print("{} -- Picked!".format(title))
                    dest_file.write(wikipage)
                    dimension += len(wikipage)
                    progress = int((dimension/chars_gigabyte) * 100)
                    if progress % 5 == 0:
                        print("=" * int(progress / 8)  + str(progress) + "%" )
                else:
                    print("{} -- Not picked!".format(title))
                if dimension > chars_gigabyte:
                    dest_file.write("\n</mediawiki>\n")
                    dest_file.close()
                    print("## Last pagetitle: " + title)
                    p = input("\nContinue?\n")
                    if p.startswith(("n", "N")):
                        sys.exit(0)
                    part += 1
                    dest_file = open(destination_path + os.sep + "part" + str(part) + ".xml", "w")
                    dest_file.write("\n<mediawiki>\n")
                    dimension = 1
        except MemoryError:
            pass

    dest_file.write("\n</mediawiki>\n")
    dest_file.close()

if __name__ == "__main__":
    main(sys.argv)
