#!/usr/bin/env python3

import sys


def get_top_as_str(d: dict, threshold: int = 5):
    result = str()
    for i, (k, v) in enumerate(sorted(d.items(), key=lambda item: item[1], reverse=True)):
        if i >= threshold:
            break
        result += str(k) + ':' + str(v) + ','
    return result.rstrip(',')

def generate_output_str(d: dict, key: str):
    overall_count = sum(d.values())
    tops = get_top_as_str(d)
    return '{}\t{}\t{}'.format(key, overall_count, tops)

current_key = None
word_to_count = dict()
for line in sys.stdin:
    try:
        key, value = line.strip().split('\t', 1)
    except ValueError as e:
        continue
    if current_key == key:
        already_counted = word_to_count.setdefault(value, 0)
        word_to_count[value] = already_counted + 1

    else:
        if current_key is not None:
            print(generate_output_str(d=word_to_count, key=current_key))
        current_key = key
        word_to_count = dict()
        word_to_count[value] = 1

if current_key:
    print(generate_output_str(d=word_to_count, key=current_key))
