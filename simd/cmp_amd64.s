// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build amd64,!appengine

TEXT ·firstGreater8SSSE3Asm(SB),4,$0-40
        MOVQ    arg+0(FP), DI
        MOVQ	val+8(FP), BX
        MOVQ    startPos+16(FP), AX
        MOVQ    endPos+24(FP), R9

        ADDQ    DI, AX
        // AX is now &(arg[startPos])
        PXOR    X1, X1
        LEAQ    -16(DI)(R9*1), R8
        // R8 is now &(arg[endPos - 16])

        // We now distinguish two cases.
        // 1. val <= 127.  Then we saturate-add (127 - val) to each byte before
        //    movemask.
        // 2. val > 127.  Then we saturate-subtract (val - 127) from each byte
        //    before movemask.
        CMPQ    BX, $127
        JG      firstGreater8SSSE3HighVal

        XORQ    $127, BX
        MOVD    BX, X0
        PSHUFB  X1, X0
        // all bytes of X0 are now equal to (127 - val)
        CMPQ    R8, AX
        JLE     firstGreater8SSSE3LowValFinal

firstGreater8SSSE3LowValLoop:
        MOVOU   (AX), X1
        PADDUSB X0, X1
        PMOVMSKB        X1, BX
        TESTQ   BX, BX
        JNE     firstGreater8SSSE3Found
        ADDQ    $16, AX
        CMPQ    R8, AX
        JG      firstGreater8SSSE3LowValLoop

firstGreater8SSSE3LowValFinal:
        MOVQ    R8, AX
        MOVOU   (R8), X1
        PADDUSB X0, X1
        PMOVMSKB        X1, BX
        TESTQ   BX, BX
        JNE     firstGreater8SSSE3Found
        MOVQ    R9, ret+32(FP)
        RET

firstGreater8SSSE3Found:
        BSFQ    BX, DX
        SUBQ    DI, AX
        ADDQ    DX, AX
        MOVQ    AX, ret+32(FP)
        RET

firstGreater8SSSE3HighVal:
        SUBQ    $127, BX
        MOVD    BX, X0
        PSHUFB  X1, X0
        // all bytes of X0 are now equal to (val - 127)
        CMPQ    R8, AX
        JLE     firstGreater8SSSE3HighValFinal

firstGreater8SSSE3HighValLoop:
        MOVOU   (AX), X1
        PSUBUSB X0, X1
        PMOVMSKB        X1, BX
        TESTQ   BX, BX
        JNE     firstGreater8SSSE3Found
        ADDQ    $16, AX
        CMPQ    R8, AX
        JG      firstGreater8SSSE3HighValLoop

firstGreater8SSSE3HighValFinal:
        MOVQ    R8, AX
        MOVOU   (R8), X1
        PSUBUSB X0, X1
        PMOVMSKB        X1, BX
        TESTQ   BX, BX
        JNE     firstGreater8SSSE3Found
        MOVQ    R9, ret+32(FP)
        RET


TEXT ·firstLeq8SSSE3Asm(SB),4,$0-40
        MOVQ    arg+0(FP), DI
        MOVD	val+8(FP), X0
        MOVQ    startPos+16(FP), AX
        MOVQ    endPos+24(FP), R9

        ADDQ    DI, AX
        // AX is now &(arg[startPos])
        PXOR    X1, X1
        // X1 is a fixed all-zero vector
        LEAQ    -16(DI)(R9*1), R8
        // R8 is now &(arg[endPos - 16])
        PSHUFB  X1, X0
        // all bytes of X0 are now equal to val
        CMPQ    R8, AX
        JLE     firstLeq8SSSE3Final

firstLeq8SSSE3Loop:
        MOVOU   (AX), X2
        PSUBUSB X0, X2
        // X2 is 0 for all bytes originally <= val, and nonzero otherwise.
        PCMPEQB X1, X2
        // X2 is now 255 for all bytes originally <= val, and 0 otherwise.
        PMOVMSKB        X2, BX
        TESTQ   BX, BX
        JNE     firstLeq8SSSE3Found
        ADDQ    $16, AX
        CMPQ    R8, AX
        JG      firstLeq8SSSE3Loop

firstLeq8SSSE3Final:
        MOVQ    R8, AX
        MOVOU   (R8), X2
        PSUBUSB X0, X2
        PCMPEQB X1, X2
        PMOVMSKB        X2, BX
        TESTQ   BX, BX
        JNE     firstLeq8SSSE3Found
        MOVQ    R9, ret+32(FP)
        RET

firstLeq8SSSE3Found:
        BSFQ    BX, DX
        SUBQ    DI, AX
        ADDQ    DX, AX
        MOVQ    AX, ret+32(FP)
        RET
