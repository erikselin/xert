// +build !gccgo

#include "textflag.h"

// func readInt(b []byte, i int) int
TEXT ·readInt(SB), NOSPLIT, $0-40
	MOVQ	b+0(FP), AX	// move starting address of b into AX
	ADDQ	i+24(FP), AX	// add offset i to AX; AX is now starting address of integer to read in b
	MOVQ	(AX), AX		// move 8-bytes starting at AX into AX
	MOVQ	AX, ret+32(FP)	// move AX into return slot
	RET

// func writeInt(b []byte, i int, n int)
TEXT ·writeInt(SB), NOSPLIT, $0-40
	MOVQ	b+0(FP), AX	// move starting address of b into AX
	ADDQ	i+24(FP), AX	// add offset i to AX; AX is now starting address of integer to write in b
	MOVQ	n+32(FP), BX	// move n to BX
	MOVQ	BX, (AX)		// move BX to memory starting at address AX
	RET

// func swap(b []byte, i int, j int)
TEXT ·swap(SB), NOSPLIT, $0-40
	MOVQ	b+0(FP), AX	// move starting address of b into AX
	MOVQ	b+24(FP), SI	// move record index i into SI
	MOVQ	b+32(FP), DI	// move record index j into DI
	SHLQ	$5, SI		// SI = SI*32; since record header is 32 bytes
	SHLQ	$5, DI		// DI = DI*32; since record header is 32 bytes
	ADDQ	AX, SI		// SI = SI+AX; SI is now starting address of i in b
	ADDQ	AX, DI		// DI = DI+AX; DI is now starting address of j in b
	MOVOU	(SI), X0		// move first 16 bytes of record i header in X0
	MOVOU	16(SI), X1	// move last 16 bytes of record i header in X1
	MOVOU	(DI), X2		// move first 16 bytes of record j header in X2
	MOVOU	16(DI), X3	// move last 16 bytes of record j header in X3
	MOVOU	X0, (DI)		// move X0 as first 16 bytes of record j header
	MOVOU	X1, 16(DI)	// move X1 as last 16 bytes of record j header
	MOVOU	X2, (SI)		// move X2 as first 16 bytes of record i header
	MOVOU	X3, 16(SI)	// move X3 as last 16 bytes of record i header
	RET

// func compare(b []byte, i int, j int) int
TEXT ·compare(SB), NOSPLIT, $0-48
	MOVQ	b+0(FP), AX	// move starting address of b into AX
	MOVQ	i+24(FP), SI	// move record index i into SI
	MOVQ	j+32(FP), DI	// move record index j into DI
	SHLQ	$5, SI		// SI = SI*32; since record header is 32 bytes
	SHLQ	$5, DI		// DI = DI*32; since record header is 32 bytes
	ADDQ	AX, SI		// SI = SI+AX; SI is now starting address of i in b
	ADDQ	AX, DI		// DI = DI+AX; DI is now starting address of j in b
	MOVQ	(SI), BX		// BX = length of record i
	MOVQ	(DI), DX		// DX = length of record j
	MOVQ	DX, R8		// move DX to R8
	CMPQ	BX, DX		// compare the length of record i to the lentgh of record j
	CMOVQLT	BX, R8		// move BX to R8 if BX < DX; R8 = min(BX, DX)
// Try to establish a ordering using only the prefix.
// If we can establish a ordering here we can achieve
// significantly better performance through better
// CPU cache locality.
	MOVOU	8(SI), X0
	MOVOU	8(DI), X1
	PCMPEQB	X0, X1
	PMOVMSKB	X1, R9
	XORQ	$0xffff, R9
	JNE	diff_prefix
	JMP	equal_prefix



diff_prefix:
	BSFQ	R9, R10
	CMPQ	R10, R8
	JA	allsame
	MOVB	8(SI)(R10*1), CX
	CMPB	CX, 8(DI)(R10*1)
	JA	above
	JB	below
	JMP	allsame
// -- cmpbody ------------------------------
	// CMPQ	SI, DI
	// JEQ	allsame
equal_prefix:
	CMPQ	R8, $16
	JBE	allsame
	SUBQ	$16, R8
	MOVQ	24(SI), SI
	MOVQ	24(DI), DI
	ADDQ	AX, SI
	ADDQ	AX, DI
	CMPQ	R8, $16
	JB	diff0to15mem
	CMPQ	R8, $32
	JB	loop
big_loop:
	MOVOU	(SI), X0
	MOVOU	(DI), X1
	PCMPEQB	X0, X1
	PMOVMSKB	X1, AX
	XORQ	$0xffff, AX
	JNE	diff16reg
	MOVOU	16(SI), X0
	MOVOU	16(DI), X1
	PCMPEQB	X0, X1
	PMOVMSKB	X1, AX
	XORQ	$0xffff, AX
	JNE	diff32reg
	ADDQ	$32, SI
	ADDQ	$32, DI
	SUBQ	$32, R8
	CMPQ	R8, $32
	JAE	big_loop
	CMPQ	R8, $16
	JB	diff0to15reg
loop:
	MOVOU	(SI), X0
	MOVOU	(DI), X1
	PCMPEQB	X0, X1
	PMOVMSKB	X1, AX
	XORQ	$0xffff, AX
	JNE	diff16reg
	ADDQ	$16, SI
	ADDQ	$16, DI
	SUBQ	$16, R8
	CMPQ	R8, $16
	JB	diff0to15reg
	JMP	loop
diff0to15mem:
	CMPQ	R8, $8
	JB	diff0to7mem
	MOVQ	(SI), R10
	MOVQ	(DI), R11
	CMPQ	R10, R11
	JNE	diff8reg
	SUBQ	$8, R8
	CMPQ	R8, $0
	JEQ	allsame
	MOVQ	(SI)(R8*1), R10
	MOVQ	(DI)(R8*1), R11
	CMPQ	R10, R11
	JNE	diff8reg
	JMP	allsame
diff0to7mem:
	CMPQ	R8, $4
	JB	diff0to3mem
	MOVL	(SI), R10
	MOVL	(DI), R11
	CMPL	R10, R11
	JNE	diff4reg
	SUBQ	$4, R8
	CMPQ	R8, $0
	JEQ	allsame
	MOVL	(SI)(R8*1), R10
	MOVL	(DI)(R8*1), R11
	CMPL	R10, R11
	JNE	diff4reg
	JMP	allsame
diff0to3mem:
	CMPQ	R8, $2
	JB	diff0to1mem
	MOVW	(SI), R10
	MOVW	(DI), R11
	CMPW	R10, R11
	JNE	diff2reg
	SUBQ	$2, R8
	CMPQ	R8, $0
	JEQ	allsame
	MOVW	(SI)(R8*1), R10
	MOVW	(DI)(R8*1), R11
	CMPW	R10, R11
	JNE	diff2reg
	JMP	allsame
diff0to1mem:
	CMPQ	R8, $0
	JEQ	allsame
	MOVB	(SI), R10
	MOVB	(DI), R11
	CMPB	R10, R11
	JB	below
	JA	above
	JMP	allsame
diff0to15reg:
	CMPQ	R8, $0
	JEQ	allsame
	ADDQ	R8, SI
	ADDQ	R8, DI
	SUBQ	$16, SI
	SUBQ	$16, DI
	MOVOU	(SI), X0
	MOVOU	(DI), X1
	PCMPEQB	X0, X1
	PMOVMSKB	X1, AX
	XORQ	$0xffff, AX
	JNE	diff16reg
	JMP	allsame
diff32reg:
	ADDQ	$16, SI
	ADDQ	$16, DI
diff16reg:
	BSFQ	AX, BX
	MOVB	(SI)(BX*1), CX
	CMPB	CX, (DI)(BX*1)
	JA	above
	JB	below
diff8reg:
	CMPL	R10, R11
	JNE	diff4reg
	SHRQ	$32, R10
	SHRQ	$32, R11
diff4reg:
	CMPW	R10, R11
	JNE	diff2reg
	SHRL	$16, R10
	SHRL	$16, R11
	CMPW	R10, R11
diff2reg:
	JA	diff2regA
	JB	diff2regB
diff2regA:
	CMPB	R10, R11
	JB	below
	JMP	above
diff2regB:
	CMPB	R10, R11
	JA	above
	JMP	below
allsame:
	CMPQ	BX, DX
	JB	below
	JA	above
	MOVQ	$0, ret+40(FP)
	RET
below:
	MOVQ	$-1, ret+40(FP)
	RET
above:
	MOVQ	$1, ret+40(FP)
	RET
