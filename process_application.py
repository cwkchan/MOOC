# Copyright (C) 2013 The Regents of the University of Michigan
#
# This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see [http://www.gnu.org/licenses/].

from util.config import *

import re
import PyPDF2
import argparse
from os import listdir

parser = argparse.ArgumentParser(description='To check the pdf applications for key words')
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--pdfs', help='A list of pdf files to scan')
group.add_argument('--dir', help='A directory with pdf files in it')
args = parser.parse_args()

if args.dir is not None:
    pdfs = listdir(args.dir)
    pdfs = map(lambda x: args.dir + x, pdfs)
else:
    pdfs = args.pdfs.split(',')

def pdf_has_match(pdf, test_words): # tests to see if any word or phrase in test_words is in pdf
    for word in test_words:
        for page in pdf.pages:
            line = bytes(page.extractText(), encoding="UTF-8")
            if re.search(word, line):
                return True
    return False


test_words = (get_properties()['search_phrases']) #program searches the pdf for these phrases
for pdf_file in pdfs:
    pdf = PyPDF2.PdfFileReader(open(pdf_file, "rb")) #open the PDF
    if pdf_has_match(pdf, test_words):
        print("Matched file: {}".format(pdf_file))

