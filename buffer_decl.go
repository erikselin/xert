// +build !gccgo
// +build amd64

package main

//go:noescape
func readInt(b []byte, i int) int

//go:noescape
func writeInt(b []byte, i int, n int)

//go:noescape
func compare(b []byte, i int, j int) int

//go:noescape
func swap(b []byte, i int, j int)
