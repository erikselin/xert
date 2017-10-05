import string
import random

from sys import stdout

key_size = 5
val_size = 93
word_count = 10000000


def random_word():
    word = ""
    for i in range(key_size):
        word += random.choice(string.lowercase)
    word += "\t"
    for i in range(val_size):
        word += random.choice(string.lowercase)
    return word


for i in range(word_count):
    stdout.write('{}\n'.format(random_word()))
