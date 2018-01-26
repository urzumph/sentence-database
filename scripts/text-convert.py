#!/usr/bin/python3

import re
import sys

sentsplit = re.compile('[。.]')
sentmatch = re.compile('[をがは]')

for line in sys.stdin:
  strarray = sentsplit.split(line)
  for sent in strarray:
    fixed = sent.strip()
    fixed = fixed.lstrip('＊* 　')
    # Note: lstrip strips both half and full-width spaces
    if len(fixed) > 0 and sentmatch.search(fixed):
      print(fixed)
