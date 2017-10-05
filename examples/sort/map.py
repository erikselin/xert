from itertools import product
from string import lowercase
from os import environ
from sys import stdin, stdout

workers = int(environ['WORKER_COUNT'])

keys = [x[0] + x[1] for x in product(lowercase, lowercase)]
lookup = {}
range_size = len(keys) / workers
for i in range(len(keys)):
    lookup[keys[i]] = min(i / range_size, workers - 1)

if __name__ == "__main__":
    for line in stdin:
        word, _ = line.split('\t')
        key = lookup[word[:2]]
        stdout.write("{}\t{}".format(key, line))
