from hashlib import md5
from os import environ
from sys import stdin, stdout

n = int(environ['REDUCERS'])

for line in stdin:
    reducer = int(md5(line.encode('utf-8')).hexdigest(), 16) % n
    stdout.write(f'{reducer}\t{line}')
