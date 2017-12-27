import random
import string

from sys import stdout, argv


MODE_UNIFORM = 'uniform'
MODE_SKEWED = 'skewed'
KEY_SIZE = 5
VALUE_SIZE = 93


def random_word(size):
    word = ''
    for i in range(size):
        word += random.choice(string.lowercase)
    return word


def generate(key_size,value_size, words, prefix_prob=0):
    key_prefix = ''
    if random.random() < prefix_prob:
        key_prefix = 'a'
        key_size -= 1

    for i in range(words):
        k = '{}{}'.format(key_prefix, random_word(key_size))
        v = random_word(value_size)
        stdout.write('{}\t{}\n'.format(k, v))


if __name__ == "__main__":
    mode = argv[1]
    words = int(argv[2])

    if mode == MODE_UNIFORM:
        generate(KEY_SIZE, VALUE_SIZE, words)
    elif mode == MODE_SKEWED:
        generate(KEY_SIZE, VALUE_SIZE, words, prefix_prob=0.5)
    else:
        raise Exception("datagen requires type and count")


