from os import environ
from sys import stdin, stdout

for line in stdin:
    stdout.write(f'0\t{line}')
