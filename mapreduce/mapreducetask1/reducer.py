#!/usr/bin/env python

import sys
import random

ids_to_stack = random.randint(1, 5)
current_result = str()
for line in sys.stdin:
    try:
        id = line.strip()[:-1]
    except ValueError as e:
        continue
    if ids_to_stack == 1:
        current_result += id
        print(current_result)
        current_result = str()
        ids_to_stack = random.randint(1, 5)
    else:
        current_result += id +','
        ids_to_stack -= 1

if current_result:
    print(current_result.rstrip(','))
