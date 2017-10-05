from sys import stdin, stdout

if __name__ == "__main__":
    for line in stdin:
        _, value = line.split('\t')
        if value.startswith('aaaaa'):
            stdout.write(line)
