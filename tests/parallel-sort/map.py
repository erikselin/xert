from os import environ
from sys import stdin, stdout

workers = int(environ['REDUCERS'])

if __name__ == "__main__":
    for line in stdin:
        word = line.strip()
        n = 26 * (ord(word[0])-97) + ord(word[1])-97
        p = int(workers * (n/26**2))
        stdout.write("{}\t{}".format(p, line))

