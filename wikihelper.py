import parsewiki.utils as pwu
import sys

if __name__ == "__main__":
    if sys.argv[2] == "-s":
        page = pwu.parse_single_wikipage(sys.argv[1])
        print(page.to_json())
    elif sys.argv[2] == "-c":
        for wikipage in pwu.bzip2_page_iter(sys.argv[1]):
            for title, timestamp, page in pwu.iter_revisions(wikipage):
                print(wikipage_to_json(page, title, timestamp))
    elif sys.argv[2] == "-d":
        pwu.split_bzip2(sys.argv[1], 5, 20, sys.argv[3])
