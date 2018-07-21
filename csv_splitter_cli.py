#!/usr/bin/python
"""
    this version can be used in command line/shel with arguments
    type >> python csv_splitter.py -h
    for a mini help
"""


def split(csv_file,
          delimiter=',',
          row_limit=1000,
          output_name_template='split_%s.csv',
          output_path='.', keep_headers=True):

    import os
    import csv

    class MyDialect(csv.excel):
        def __init__(self, delimiter=','):
            self.delimiter = delimiter
        lineterminator = '\n'

    my_dialect = MyDialect(delimiter=delimiter)
    reader = csv.reader(csv_file, my_dialect)
    current_piece = 1
    current_out_path = os.path.join(output_path, output_name_template % current_piece)
    current_out_writer = csv.writer(open(current_out_path, 'w'), my_dialect)
    current_limit = row_limit

    if keep_headers:
        headers = reader.next()
        current_out_writer.writerow(headers)

    for i, row in enumerate(reader):
        if i + 1 > current_limit:
            current_piece += 1
            current_limit = row_limit * current_piece
            current_out_path = os.path.join(output_path, output_name_template % current_piece)
            current_out_writer = csv.writer(open(current_out_path, 'w'), my_dialect)
            if keep_headers:
                current_out_writer.writerow(headers)
        current_out_writer.writerow(row)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Splits a CSV file into multiple pieces.',
                                     prefix_chars='-+')
    parser.add_argument('input_file', metavar='in-file', type=argparse.FileType('rb'),
                        help='The CSV file to split')
    parser.add_argument('-o', '--output_path', type=str, default='.',
                        help='Where to stick the output files. (default: ".")')
    parser.add_argument('-d', '--delimiter', type=str, default=';',
                        help='CSV field delimiter')
    parser.add_argument('-l', '--row_limit', type=int, default=1000,
                        help='The number of rows you want in each output file. (default: 1000)')
    parser.add_argument('-t', '--output_name_template', type=str, default='split_%s.csv',
                        help='A %%s-style template for the numbered output files.')
    parser.add_argument('+k', '++keep_headers', action="store_true", default=True,
                        help='Print the headers in each output file.')
    parser.add_argument('-k', '--keep_headers', action="store_false", default=True,
                        help='Do Not print the headers in each output file.')

    args = parser.parse_args()

    split(args.input_file, args.delimiter, args.row_limit, args.output_name_template, args.output_path, args.keep_headers)
