#!/usr/bin/env python
import sys

max_rating =0
for line in sys.stdin:
	(book_id,rating) = line.split()
	if max_rating < rating:
		max_rating = rating

print max_rating