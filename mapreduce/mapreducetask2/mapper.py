#!/usr/bin/env python3

import sys
import re

for line in sys.stdin:
    try:
        id, text = line.strip().split('\t', 1)
    except ValueError as e:
        continue
    words = re.split('[^A-Za-z]+', text.strip())

    for word in words:
        word = word.lower().strip()
        if len(word) < 3:
            continue
        key = ''.join(sorted(list(word)))
        print('{}\t{}'.format(key, word))
