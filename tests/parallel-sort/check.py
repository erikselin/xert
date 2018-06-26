import os
from sys import argv

filename_in = argv[1]
dirname_out = argv[2]

with open(filename_in) as f_in:
    expected = [line for line in f_in]
    actual = []
    for filename_out in sorted(os.listdir(dirname_out)):
        with open(os.path.join(dirname_out, filename_out)) as f_out:
            actual += [line for line in f_out]
    if len(expected) != len(actual):
        exit(1)
    expected.sort()
    for i in range(len(actual)):
        if actual[i] != expected[i]:
            exit(1)
