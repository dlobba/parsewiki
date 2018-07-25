import csv
import sys
import re
import os

def read_stat_data(rows_iter, col_index=0, has_header=True):
    """Return a dictionary where keys are values
    from the column stated in the argument and
    key-values are the remaining column fields.
    If header is true then the first row is
    saved and returned with the data.
    """
    header = None
    if has_header is True:
        header = next(rows_iter)
    data = dict()
    for row in rows_iter:
        page = row[col_index]
        vals = [int(x) for x in row[1:]]
        data[page] = vals
    return data, header

def extend_stat_dict(stat1_dict, stat2_dict):
    """Given two dictionaries create a new one
    with all entries from the two. In case
    the same key belongs to both dictionaries
    raise an error, do duplicates can be accepted.
    """
    data = dict()
    for key, vals in stat1_dict.items():
        data[key] = vals
    for key, vals in stat2_dict.items():
        if key in data:
            raise ValueError("Duplicated keys while extending")
        else:
            data[key] = vals
    return data

def merge_stat_dict(stat1_dict, stat2_dict, fill_missing=0):
    """Given two dictionaries merge values from
    the first to values from the second.
    """
    data = dict()
    keys1 = set(stat1_dict.keys())
    keys2 = set(stat2_dict.keys())
    keys = keys1.union(keys2)

    nvals1 = len(list(stat1_dict.values())[0])
    nvals2 = len(list(stat2_dict.values())[0])
    for key in keys:
        if not key in keys1:
            data[key] = [fill_missing] * nvals1 + stat2_dict[key]
        if not key in keys2:
            data[key] = stat1_dict[key] + [fill_missing] * nvals2
        else:
            data[key] = stat1_dict[key] + stat2_dict[key]    
    return data

if __name__ == "__main__":
    stats_dir = sys.argv[1]
    stat1 = "statistics1"
    stat2 = "statistics2"

    stat1_files = [stats_dir + os.sep + f for f in os.listdir(stats_dir)\
                   if f.startswith(stat1) and f[-4:] == ".csv"]
    stat2_files = [stats_dir + os.sep + f for f in os.listdir(stats_dir)\
                   if f.startswith(stat2) and f[-4:] == ".csv"]
    stat1_dict = dict()
    header1 = None
    for file in stat1_files:
        with open(file, "r") as fh:
            rows = csv.reader(fh)
            data, h = read_stat_data(rows)
            if not h is None and\
               header1 is None:
                header1 = h
            stat1_dict = extend_stat_dict(stat1_dict, data)

    stat2_dict = dict()
    header2 = None
    for file in stat2_files:
        with open(file, "r") as fh:
            rows = csv.reader(fh)
            data, h = read_stat_data(rows)
            if not h is None and\
               header2 is None:
                header2 = h
            stat2_dict = extend_stat_dict(stat2_dict, data)
    stat_dict = merge_stat_dict(stat1_dict, stat2_dict)

    out_stat_file = "statistics.csv"
    out_stat1_file = "statistics1.csv"
    out_stat2_file = "statistics2.csv"
    header = None
    if header1 and header2:
        header = header1 + header2[1:]
        
    with open("." + os.sep + out_stat_file, "w") as fh:
        csv_writer = csv.writer(fh, quoting=csv.QUOTE_NONNUMERIC)
        if header is not None:
            csv_writer.writerow(header)
        for key, vals in stat_dict.items():
            csv_writer.writerow([key] + vals)
