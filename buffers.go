package main

// buffers is a matrix of buffers with reducer-rows and mappers-columns partitioning the
// allocated memory to ensure that it can be accessed without any locking during mapping and
// reducing. In particular, mapper[i] will write to all buffers in buffers[i][*] while
// reducer[j] will read from all buffers in buffers[*][j].
//
//                          0   1   2
//                        +---+---+---+
//                      0 |b00|b01|b02|
//                        +---+---+---+
// mapper[1] - write -> 1 |b10|b11|b12|
//                        +---+---+---+
//                      2 |b20|b21|b22|
//                        +---+---+---+
//                              |
//                              +- read -> reducer[1]
type buffers [][]*buffer
