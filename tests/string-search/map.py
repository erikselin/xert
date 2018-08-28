from re import search
from sys import stdin, stdout

for line in stdin:
    if search('00000', line):
        stdout.write(line)
