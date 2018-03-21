from sys import stdin, stdout

word = stdin.readline()
if not word:
    exit()

count = 1
for next in stdin:
    if word != next:
        stdout.write(f'{count}\t{word}')
        word = next
        count = 0
    count += 1
stdout.write(f'{count}\t{word}')
