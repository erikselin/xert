from sys import stdin, stdout


def write(word, count):
    stdout.write("{},{}\n".format(word, count))


if __name__ == "__main__":
    word = stdin.readline()
    if not word:
        quit()
    count = 1

    for w in stdin:
        if word != w:
            write(word, count)
            word = w
            count = 0
        count += 1

    write(word, count)
