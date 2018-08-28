from random import randint
from sys import stdin, stdout, argv


def record(mode, keys):
    record = ''
    if mode == 'skewed':
        if randint(0, 1) == 0:
            return '0' * 100
    part = "".join([str(randint(0, 9)) * 5 for _ in range(10)])
    if keys == 'long':
        return '0' * 50 + part
    return part + part


if __name__ == "__main__":
    mode = argv[1]
    keys = argv[2]
    count = int(argv[3])
    for _ in range(count):
        row = record(mode, keys)
        stdout.write(row + '\n')
