from random import randint
from sys import stdin, stdout, argv


def row(size):
    f = int(1 + size / 4)
    return "".join([str(randint(0, 9)) * f for _ in range(32)])[0:size] + "\n"


if __name__ == "__main__":
    size = int(argv[1])
    while size > 0:
        s = min([size, randint(1, 32)])
        stdout.write(row(s))
        size = size - s
