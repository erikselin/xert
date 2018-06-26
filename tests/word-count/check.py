import os
from sys import argv

filename_in = argv[1]
dirname_out = argv[2]

with open(filename_in) as f_in:
    expected = {}
    for word in f_in:
        expected[word] = 1 + (expected.get(word) or 0)
    expected = [f'{v}\t{k}' for (k,v) in expected.items()]
    actual = []
    for filename_out in os.listdir(dirname_out):
        with open(os.path.join(dirname_out, filename_out)) as f_out:
            actual = actual + [line for line in f_out]
    if len(expected) != len(actual):
        exit(1)
    expected.sort()
    actual.sort()
    for i in range(len(actual)):
        if actual[i] != expected[i]:
            exit(1)
