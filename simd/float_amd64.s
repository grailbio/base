// Copyright 2021 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build amd64,!appengine

        DATA ·ExponentMask<>+0x00(SB)/8, $0x7ff07ff07ff07ff0
        DATA ·ExponentMask<>+0x08(SB)/8, $0x7ff07ff07ff07ff0
        DATA ·ExponentMask<>+0x10(SB)/8, $0x7ff07ff07ff07ff0
        DATA ·ExponentMask<>+0x18(SB)/8, $0x7ff07ff07ff07ff0
        // NOPTR = 16, RODATA = 8
        GLOBL ·ExponentMask<>(SB), 24, $32

        DATA ·FirstShuffle<>+0x00(SB)/8, $0xffffffff0f0e0706
        DATA ·FirstShuffle<>+0x08(SB)/8, $0xffffffffffffffff
        DATA ·FirstShuffle<>+0x10(SB)/8, $0xffffffff0f0e0706
        DATA ·FirstShuffle<>+0x18(SB)/8, $0xffffffffffffffff
        GLOBL ·FirstShuffle<>(SB), 24, $32

        DATA ·SecondShuffle<>+0x00(SB)/8, $0x0f0e0706ffffffff
        DATA ·SecondShuffle<>+0x08(SB)/8, $0xffffffffffffffff
        DATA ·SecondShuffle<>+0x10(SB)/8, $0x0f0e0706ffffffff
        DATA ·SecondShuffle<>+0x18(SB)/8, $0xffffffffffffffff
        GLOBL ·SecondShuffle<>(SB), 24, $32

        DATA ·ThirdShuffle<>+0x00(SB)/8, $0xffffffffffffffff
        DATA ·ThirdShuffle<>+0x08(SB)/8, $0xffffffff0f0e0706
        DATA ·ThirdShuffle<>+0x10(SB)/8, $0xffffffffffffffff
        DATA ·ThirdShuffle<>+0x18(SB)/8, $0xffffffff0f0e0706
        GLOBL ·ThirdShuffle<>(SB), 24, $32

        DATA ·FourthShuffle<>+0x00(SB)/8, $0xffffffffffffffff
        DATA ·FourthShuffle<>+0x08(SB)/8, $0x0f0e0706ffffffff
        DATA ·FourthShuffle<>+0x10(SB)/8, $0xffffffffffffffff
        DATA ·FourthShuffle<>+0x18(SB)/8, $0x0f0e0706ffffffff
        GLOBL ·FourthShuffle<>(SB), 24, $32

TEXT ·findNaNOrInf64SSSE3Asm(SB),4,$0-24
        // findNaNOrInf64SSSE3Asm returns x if the first NaN/inf in data is at
        // position x, or -1 if no NaN/inf is present.  nElem must be at least
        // 8.
        //
        // The implementation exploits the fact that we only need to look at
        // the exponent bits to determine NaN/inf status, and these occupy just
        // the top two bytes of each 8-byte float.  Thus, we can pack the
        // exponent-containing-bytes of 8 consecutive float64s into a single
        // 16-byte vector, and check them in parallel.
        //
        // Register allocation:
        //   AX: data
        //   BX: nElem - 8
        //   CX: current index
        //   DX: comparison result
        //   SI: &(data[2])
        //   DI: &(data[4])
        //   R8: &(data[6])
        //   R9: nElem
        //   X0: exponent mask
        //   X1: first shuffle mask
        //   X2: second shuffle mask
        //   X3: third shuffle mask
        //   X4: fourth shuffle mask
        MOVQ    data+0(FP), AX
        MOVQ	nElem+8(FP), BX
        MOVQ    BX, R9
        SUBQ    $8, BX
        XORL    CX, CX
        MOVQ    AX, SI
        MOVQ    AX, DI
        MOVQ    AX, R8
        ADDQ    $16, SI
        ADDQ    $32, DI
        ADDQ    $48, R8

        MOVOU   ·ExponentMask<>(SB), X0
        MOVOU   ·FirstShuffle<>(SB), X1
        MOVOU   ·SecondShuffle<>(SB), X2
        MOVOU   ·ThirdShuffle<>(SB), X3
        MOVOU   ·FourthShuffle<>(SB), X4

findNaNOrInf64SSSE3AsmLoop:
        // Scan 8 float64s, starting from &(data[CX]), into X5..X8.
        MOVOU   (AX)(CX*8), X5
        MOVOU   (SI)(CX*8), X6
        MOVOU   (DI)(CX*8), X7
        MOVOU   (R8)(CX*8), X8

        // Extract exponent bytes.
        PSHUFB  X1, X5
        PSHUFB  X2, X6
        PSHUFB  X3, X7
        PSHUFB  X4, X8

        // Collect into X5.
        POR     X6, X5
        POR     X8, X7
        POR     X7, X5

        // Mask out non-exponent bits, and then compare 2-byte groups in
        // parallel.
        PAND    X0, X5
        PCMPEQW X0, X5

        // Check result.
        PMOVMSKB        X5, DX
        TESTQ   DX, DX
        JNE     findNaNOrInf64SSSE3AsmFound

        // Advance loop.
        ADDQ    $8, CX
        CMPQ    BX, CX
        JGE     findNaNOrInf64SSSE3AsmLoop

        // Less than 8 float64s left...
        CMPQ    R9, CX
        JE      findNaNOrInf64SSSE3AsmNotFound

        // ...but more than zero.  Set CX := nElem - 8, and start one last
        // loop iteration.
        MOVQ    BX, CX
        JMP     findNaNOrInf64SSSE3AsmLoop

findNaNOrInf64SSSE3AsmNotFound:
        MOVQ    $-1, ret+16(FP)
        RET

findNaNOrInf64SSSE3AsmFound:
        // Determine the position of the lowest set bit in DX, i.e. the byte
        // offset of the first comparison success.
        BSFQ    DX, BX
        // We compared 2-byte groups, so divide by 2 to determine the original
        // index.
        SHRQ    $1, BX
        ADDQ    CX, BX
        MOVQ    BX, ret+16(FP)
        RET


TEXT ·findNaNOrInf64AVX2Asm(SB),4,$0-24
        // findNaNOrInf64AVX2Asm is nearly identical to the SSSE3 version; it
        // just compares 16 float64s at a time instead of 8.
        MOVQ    data+0(FP), AX
        MOVQ	nElem+8(FP), BX
        MOVQ    BX, R9
        SUBQ    $16, BX
        XORL	CX, CX
        MOVQ    AX, SI
        MOVQ    AX, DI
        MOVQ    AX, R8
        ADDQ    $32, SI
        ADDQ    $64, DI
        ADDQ    $96, R8

        VMOVDQU ·ExponentMask<>(SB), Y0
        VMOVDQU ·FirstShuffle<>(SB), Y1
        VMOVDQU ·SecondShuffle<>(SB), Y2
        VMOVDQU ·ThirdShuffle<>(SB), Y3
        VMOVDQU ·FourthShuffle<>(SB), Y4

findNaNOrInf64AVX2AsmLoop:
        // Scan 16 float64s, starting from &(data[CX]), into Y5..Y8.
        VMOVDQU (AX)(CX*8), Y5
        VMOVDQU (SI)(CX*8), Y6
        VMOVDQU (DI)(CX*8), Y7
        VMOVDQU (R8)(CX*8), Y8

        // Extract exponent bytes.
        VPSHUFB Y1, Y5, Y5
        VPSHUFB Y2, Y6, Y6
        VPSHUFB Y3, Y7, Y7
        VPSHUFB Y4, Y8, Y8

        // Collect into Y5.
        VPOR    Y6, Y5, Y5
        VPOR    Y8, Y7, Y7
        VPOR    Y7, Y5, Y5

        // Mask out non-exponent bits, and then compare 2-byte groups in
        // parallel.
        VPAND   Y0, Y5, Y5
        VPCMPEQW        Y0, Y5, Y5

        // Check result.
        VPMOVMSKB       Y5, DX
        TESTQ   DX, DX
        JNE     findNaNOrInf64AVX2AsmFound

        // Advance loop.
        ADDQ    $16, CX
        CMPQ    BX, CX
        JGE     findNaNOrInf64AVX2AsmLoop

        // Less than 8 float64s left...
        CMPQ    R9, CX
        JE      findNaNOrInf64AVX2AsmNotFound

        // ...but more than zero.  Set CX := nElem - 8, and start one last
        // loop iteration.
        MOVQ    BX, CX
        JMP     findNaNOrInf64AVX2AsmLoop

findNaNOrInf64AVX2AsmNotFound:
        MOVQ    $-1, ret+16(FP)
        RET

findNaNOrInf64AVX2AsmFound:
        // Since the PSHUFB instruction acts separately on the two 16-byte
        // "lanes", the 2-byte chunks in Y5, and consequently the 2-bit groups
        // in DX here, are drawn from &(data[CX])..&(data[CX+15]) in the
        // following order:
        //   0 1 4 5 8 9 12 13 2 3 6 7 10 11 14 15
        // We "unscramble" this before grabbing the lowest set bit.

        // Clear odd bits.
        ANDQ    $0x55555555, DX

        // Rearrange to
        //   0 1 * * 4 5 * * 8 9 * * 12 13 * * 2 3 ...
        // where the above refers to single bits, and * denotes a cleared bit.
        MOVQ    DX, BX
        SHRQ    $1, BX
        ORQ     BX, DX
        ANDQ    $0x33333333, DX

        //   0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 ...
        MOVQ    DX, BX
        SHRQ    $14, BX
        ORQ     BX, DX

        // Okay, now we're ready.
        BSFQ    DX, BX
        ADDQ    CX, BX
        MOVQ    BX, ret+16(FP)
        RET
