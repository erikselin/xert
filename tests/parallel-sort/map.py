from os import environ
from sys import stdin, stdout

n = int(environ['REDUCERS'])

for line in stdin:
    p = int(line[0] + line[5] + line[10])
    reducer = int(p * n / 1000)
    stdout.write(f'{reducer}\t{line}')
