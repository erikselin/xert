from os import environ
from sys import stdin, stdout

workers = int(environ['WORKER_COUNT'])
#data = {}
#
# if __name__ == "__main__":
#    for line in stdin:
#        word, _ = line.split('\t')
#        data[word] = (data.get(word) or 0) + 1
#
#    for word, count in data.iteritems():
#        stdout.write("{}\t{}\t{}\n".format(ord(word[0]) % workers, word, count))

if __name__ == "__main__":
    for line in stdin:
        word, _ = line.split('\t')
        stdout.write("{}\t{}\n".format(ord(word[0]) % workers, word))
