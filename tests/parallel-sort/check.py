import os

from sys import argv

dirname_in = argv[1]
dirname_out = argv[2]

expected = []
for filename in os.listdir(dirname_in):
    with open(os.path.join(dirname_in, filename)) as f:
        expected = expected + [line for line in f]

actual = []
for filename in os.listdir(dirname_out):
    with open(os.path.join(dirname_out, filename)) as f:
        actual = actual + [line for line in f]

if len(expected) != len(actual):
    exit(1)

expected.sort()
actual.sort()

for i in range(len(actual)):
    if actual[i] != expected[i]:
        exit(1)
