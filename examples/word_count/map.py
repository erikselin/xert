from os import environ
from sys import stdin, stdout

n = int(environ.get('REDUCERS', 1))
for word in stdin:
    key = hash(word.lower()) % n
    stdout.write(f'{key}\t{word.lower()}')
