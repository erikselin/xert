import random
import string
from sys import stdout, stderr, argv

key_size = 2
options = len(string.ascii_lowercase)**key_size

def sorted_asc(count):
    for i in range(count):
        h = int(options * (i / count))
        key = ''
        for _ in range(key_size):
            key = string.ascii_lowercase[int(h % len(string.ascii_lowercase))]*50 + key
            h = int(h / len(string.ascii_lowercase))
        yield f'{key}\n'

def sorted_desc(count):
    for i in range(count):
        h = int(options * (i / count))
        key = ''
        for _ in range(key_size):
            key = string.ascii_lowercase[int(len(string.ascii_lowercase) - (h % len(string.ascii_lowercase)) - 1)]*50 + key
            h = int(h / len(string.ascii_lowercase))
        yield f'{key}\n'

def uniform(count):
    for _ in range(count):
        key = ''
        for _ in range(key_size):
            key += random.choice(string.ascii_lowercase)*50
        yield f'{key}\n'

def skewed(count):
    for _ in range(count):
        key = ''
        if random.random() < 0.5:
            key = 'a'*key_size
        else:
            for _ in range(key_size):
                key += random.choice(string.ascii_lowercase)*50
        yield f'{key}\n'

def single(count):
    key = 'a'*key_size*50
    for _ in range(count):
        yield f'{key}\n'


if __name__ == '__main__':
    mode = argv[1]
    count = int(argv[2])
    output = argv[3]
    generator = {
        'sorted-asc': sorted_asc,
        'sorted-desc': sorted_desc,
        'uniform': uniform,
        'skewed': skewed,
        'single': single
    }[mode](count)
    with open(output, 'w') as f:
        for record in generator:
            f.write(record)
