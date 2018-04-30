// +build !gccgo

#include "textflag.h"

// func readInt(b []byte, i int) int
TEXT ·readInt(SB), NOSPLIT, $0-40
	MOVQ	b+0(FP), AX
	ADDQ	i+24(FP), AX
	MOVQ	(AX), AX
	MOVQ	AX, ret+32(FP)
	RET

// func writeInt(b []byte, i int, n int)
TEXT ·writeInt(SB), NOSPLIT, $0-40
	MOVQ	b+0(FP), AX
	ADDQ	i+24(FP), AX
	MOVQ	n+32(FP), BX
	MOVQ	BX, (AX)
	RET

// func swap(b []byte, i int, j int)
TEXT ·swap(SB), NOSPLIT, $0-40
	MOVQ	b+0(FP), AX
	MOVQ	AX, BX
	MOVQ	b+24(FP), SI
	MOVQ	b+32(FP), DI
	SHLQ	$3, SI
	SHLQ	$3, DI
	ADDQ	SI, AX
	ADDQ	DI, BX
	MOVQ	(AX), CX
	MOVQ	(BX), DX
	MOVQ	CX, (BX)
	MOVQ	DX, (AX)
	RET

// func compare(b []byte, i int, j int) int
TEXT ·compare(SB), NOSPLIT, $0-48
	MOVQ	b+0(FP), AX
	MOVQ	i+24(FP), SI
	MOVQ	j+32(FP), DI
	SHLQ	$3, SI
	SHLQ	$3, DI
	ADDQ	AX, SI
	ADDQ	AX, DI
	MOVQ	(SI), SI
	MOVQ	(DI), DI
	ADDQ	AX, SI
	ADDQ	AX, DI
	MOVQ	(DI), DX
	MOVQ	(SI), BX
	ADDQ	$8, SI
	ADDQ	$8, DI
	LEAQ	ret+40(FP), R9
	JMP	cmpbody<>(SB)

// input:
//   SI = a
//   DI = b
//   BX = alen
//   DX = blen
//   R9 = address of output word (stores -1/0/1 here)
TEXT cmpbody<>(SB),NOSPLIT,$0-0
	CMPQ	SI, DI
	JEQ	allsame
	CMPQ	BX, DX
	MOVQ	DX, R8
	CMOVQLT	BX, R8
	CMPQ	R8, $16
	JB	diff0to15mem
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
	SHRQ	$16, R10
	SHRQ	$16, R11
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
	MOVQ	$0, (R9)
	RET
below:
	MOVQ	$-1, (R9)
	RET
above:
	MOVQ	$1, (R9)
	RET
