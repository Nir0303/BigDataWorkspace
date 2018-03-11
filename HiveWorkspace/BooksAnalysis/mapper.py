#!/usr/bin/env python
import sys
import csv
for line in sys.stdin:
	book_id,user_id,rating= line.split()
	print  "{},{}".format(book_id,rating)