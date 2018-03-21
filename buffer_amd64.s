// +build !gccgo

#include "textflag.h"

// func readInt(b []byte, i int) int
TEXT ·readInt(SB), NOSPLIT, $0-40
	MOVQ b+0(FP), AX
	ADDQ i+24(FP), AX
	MOVQ (AX), AX
	MOVQ AX, ret+32(FP)
	RET

// func writeInt(b []byte, i int, n int)
TEXT ·writeInt(SB), NOSPLIT, $0-40
	MOVQ b+0(FP), AX
	ADDQ i+24(FP), AX
	MOVQ n+32(FP), BX
	MOVQ BX, (AX)
	RET

// func compare(b []byte, i int, j int) int
TEXT ·compare(SB), NOSPLIT, $0-48
	MOVQ b+0(FP), AX
	MOVQ i+24(FP), SI
	MOVQ j+32(FP), DI
	SHLQ $3, SI
	SHLQ $3, DI
	ADDQ AX, SI
	ADDQ AX, DI
	MOVQ (SI), SI
	MOVQ (DI), DI
	ADDQ AX, SI
	ADDQ AX, DI
	MOVQ (DI), DX
	MOVQ (SI), BX
	ADDQ $8, SI
	ADDQ $8, DI
// Just for fun and to maybe enable sharing of optimization efforts this
// should be a near drop-in for cmpbody (src/internal/bytealg/compare_amd64.s)
// from this point on.
// input:
// SI = a
// DI = b
// BX = alen
// DX = blen
	CMPQ BX, DX
	MOVQ DX, CX
	CMOVQLT BX, CX
loop:
	MOVQ (SI), R10
	MOVQ (DI), R11
	CMPQ CX, $8
	JBE diff_4
	CMPQ R10, R11
	JNE diff_4
	ADDQ $8, SI
	ADDQ $8, DI
	SUBQ $8, CX
	JMP loop
diff_4:
	CMPQ CX, $4
	JBE diff_2
	CMPL R10, R11
	JNE diff_2
	SHRQ $32, R10
	SHRQ $32, R11
	SUBQ $4, CX
diff_2:
	CMPQ CX, $2
	JBE diff_1
	CMPW R10, R11
	JNE diff_1
	SHRQ $16, R10
	SHRQ $16, R11
	SUBQ $2, CX
diff_1:
	CMPQ CX, $0
	JBE equal
	CMPB R10, R11
	JB below
	JA above
	CMPQ CX, $1
	JBE equal
	SHRQ $8, R10
	SHRQ $8, R11
	CMPB R10, R11
	JB below
	JA above
equal:
	CMPQ BX, DX
	JB below
	JA above
	MOVQ $0, ret+40(FP)
	RET
below:
	MOVQ $-1, ret+40(FP)
	RET
above:
	MOVQ $1, ret+40(FP)
	RET

// func swap(b []byte, i int, j int)
TEXT ·swap(SB), NOSPLIT, $0-40
	MOVQ b+0(FP), AX
	MOVQ AX, BX
	MOVQ b+24(FP), SI
	MOVQ b+32(FP), DI
	SHLQ $3, SI
	SHLQ $3, DI
	ADDQ SI, AX
	ADDQ DI, BX
	MOVQ (AX), CX
	MOVQ (BX), DX
	MOVQ CX, (BX)
	MOVQ DX, (AX)
	RET
