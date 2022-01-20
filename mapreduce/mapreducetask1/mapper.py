#!/usr/bin/env python

import sys
import random

for line in sys.stdin:
    try:
        shuffler = str(random.randint(0, 9))
        result = line.strip() + shuffler
    except ValueError as e:
        continue
    print(result)
