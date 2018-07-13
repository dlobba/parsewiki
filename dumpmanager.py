import os
import logging
import requests

class MD5IntegrityException(Exception): pass

def get_online_dump(wiki_version, max_dump=None, memory=False):
    """Download a specific wikipedia version dumps online,
    returing each dump.

    Parameters:
      wiki_version (str): date string of the wiki version to
        work on
      max_dump (int): the number of dump to take into account
        (None if all dumps shuld be considered)
      memory (bool): True if the dumps must be processed in memory.
        If False then each dump is first saved temporarly into
        the system and then processed as a file.

    Notes:
      Keep in mind that by working with dumps in memory, in case
      the dump is 1GB then at least 1GB of ram is needed and then
      a much higher requirement is needed whene decompression
      is performed.
    """
    endpoint = "http://wikimedia.bytemark.co.uk"
    status_url = endpoint + "/wikidatawiki/" + wiki_version + "/dumpstatus.json"
    status_json = requests.get(status_url).json()
    # testing dataset (no revision)
    dumps = status_json['jobs']['articlesdump']
    # full dataset (70TB)
    #dumps = status_json['jobs']['metahistory7zdump']
    if dumps['status'] != "done":
        print("The dump is incomplete, stopping the execution...\n")
        exit(-1)
    files = []
    for key in dumps['files'].keys():
        files.append({'url': dumps['files'][key]['url'],\
                      'md5': dumps['files'][key]['md5'],\
                      'size': dumps['files'][key]['size']})
    # sort in order to download smaller files first
    files.sort(key=lambda file: file['size'])
    # if not set, consider all files
    if max_dump is None:
        max_dump = len(files)

    for dump in files[:max_dump]:
        if not memory:
            # Try to open the file matching with the md5sum of the resource.
            # If it does not exist download it and write in the file as it's md5sum
            filename_r = "/tmp/wikidump_" + dump['md5']
            try:
                with open(filename_r, "rt") as bz2_fh:
                        # If the file exists we are sure that it is not corrupted.
                        logging.info("Dump already downloaded, skipping download and reloading it.")
                        yield filename_r
            except FileNotFoundError:
                url = endpoint + dump['url']
                logging.info("Downloading dump: {}".format(url))
                bz2_dump = requests.get(url).content
                md5sum = hashlib.md5(bz2_dump).hexdigest()
                if md5sum != dump['md5']:
                    raise MD5IntegrityException("The dump downloaded has a wrong MD5 value.")
                filename_w = "/tmp/wikidump_" + md5sum
                with open(filename_w, "wb") as bz2_fh:
                    bz2_fh.write(bz2_dump)
                yield filename_w
        else:
            url = endpoint + dump['url']
            logging.info("Downloading dump: {}".format(url))
            bz2_dump = requests.get(url).content
            yield bz2_dump

def get_offline_dump(dump_folder):
    """Given a path to the folder containing wikipedia
    dumpps (in bz2 format), return the
    path to each dump file"""
    dump_files = [dump for dump in os.listdir(dump_folder) \
                  if dump[-4:] == ".bz2"]
    for dump in dump_files:
        yield dump_folder + os.sep + dump
