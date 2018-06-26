from os import environ
from sys import stdin, stdout

n = int(environ['REDUCERS'])

for line in stdin:
    word = line.strip().lower()
    key = ord(word[0]) % n
    stdout.write(f'{key}\t{word}\n')
