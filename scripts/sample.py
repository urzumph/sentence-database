#!/usr/bin/python3

import sys
import bz2
import random

count = 100
randommax = 100000

with bz2.open(sys.argv[1], mode="rt") as inf:
  while count > 0:
    rand = random.randrange(randommax)
    while rand > 0:
      line = inf.readline()
      if len(line) == 0:
        # end of file
        inf.seek(0)
        line = inf.readline()
      rand -= 1
    print(line)
    count -= 1
