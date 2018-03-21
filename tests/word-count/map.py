from os import environ
from sys import stdin, stdout

n = int(environ['REDUCERS'])

for line in stdin:
    word = line.strip()
    key = hash(word.lower()) % n
    stdout.write(f'{key}\t{word.lower()}\n')
