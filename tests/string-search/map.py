from sys import stdin, stdout

if __name__ == "__main__":
    for line in stdin:
        if line.startswith('aaaa'):
            stdout.write(line)
