// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build amd64,!appengine

TEXT ·addConst8TinyInplaceSSSE3Asm(SB),4,$0-16
        // DI = pointer to current main[] element.
        MOVQ    main+0(FP), DI
        MOVD    val+8(FP), X0

        PXOR    X1, X1
        PSHUFB  X1, X0
        // all bytes of X0 are now equal to val

        MOVOU   (DI), X1
        PADDB   X0, X1
        MOVOU   X1, (DI)
        RET

TEXT ·addConst8OddInplaceSSSE3Asm(SB),4,$0-24
        // DI = pointer to current main[] element.
        MOVQ    main+0(FP), DI
        MOVD    val+8(FP), X0
        MOVQ    nByte+16(FP), SI

        PXOR    X1, X1
        PSHUFB  X1, X0
        // all bytes of X0 are now equal to val

        LEAQ    -32(DI)(SI*1), AX
        CMPQ    AX, DI
        JLE     addConst8OddInplaceSSSE3Final

addConst8OddInplaceSSSE3Loop:
        // tried 2x unroll, benefit appears to exist but is smaller than ~4% so
        // I won't bother for now
        MOVOU   (DI), X1
        PADDB   X0, X1
        MOVOU   X1, (DI)
        ADDQ    $16, DI
        CMPQ    AX, DI
        JG      addConst8OddInplaceSSSE3Loop

addConst8OddInplaceSSSE3Final:
        // Load and parallel-add to last two vectors (which usually overlap)
        // simultaneously, before writing back.
        ADDQ    $16, AX
        MOVOU   (DI), X1
        MOVOU   (AX), X2
        PADDB   X0, X1
        PADDB   X0, X2
        MOVOU   X1, (DI)
        MOVOU   X2, (AX)
        RET

TEXT ·addConst8SSSE3Asm(SB),4,$0-32
        // DI = pointer to current src[] element.
        // R8 = pointer to current dst[] element.
        MOVQ    dst+0(FP), R8
        MOVQ    src+8(FP), DI
        MOVD	val+16(FP), X0
        MOVQ	nByte+24(FP), SI

        PXOR    X1, X1
        PSHUFB  X1, X0
        // all bytes of X0 are now equal to val

        // SI = pointer to end of src[].
        ADDQ    DI, SI

addConst8SSSE3Loop:
        MOVOU   (DI), X1
        PADDB   X0, X1
        MOVOU   X1, (R8)
        ADDQ    $16, DI
        ADDQ    $16, R8
        CMPQ    SI, DI
        JG      addConst8SSSE3Loop

        RET

TEXT ·addConst8OddSSSE3Asm(SB),4,$0-32
        // DI = pointer to current src[] element.
        // R8 = pointer to current dst[] element.
        MOVQ    dst+0(FP), R8
        MOVQ    src+8(FP), DI
        MOVD	val+16(FP), X0
        MOVQ	nByte+24(FP), BX

        PXOR    X1, X1
        PSHUFB  X1, X0

        // set AX to 16 bytes before end of src[].
        // change BX to 16 bytes before end of dst[].
        SUBQ    $16, BX
        LEAQ    0(DI)(BX*1), AX
        ADDQ    R8, BX

addConst8OddSSSE3Loop:
        MOVOU   (DI), X1
        PADDB   X0, X1
        MOVOU   X1, (R8)
        ADDQ    $16, DI
        ADDQ    $16, R8
        CMPQ    AX, DI
        JG      addConst8OddSSSE3Loop

        // Final usually-unaligned read and write.
        MOVOU   (AX), X1
        PADDB   X0, X1
        MOVOU   X1, (BX)
        RET

TEXT ·subtractFromConst8TinyInplaceSSSE3Asm(SB),4,$0-16
        // Almost identical to addConst8TinyInplaceSSSE3Asm.
        // DI = pointer to current main[] element.
        MOVQ    main+0(FP), DI
        MOVD    val+8(FP), X0

        PXOR    X1, X1
        PSHUFB  X1, X0
        // all bytes of X0 are now equal to val

        MOVOU   (DI), X1
        PSUBB   X1, X0
        MOVOU   X0, (DI)
        RET

TEXT ·subtractFromConst8OddInplaceSSSE3Asm(SB),4,$0-24
        // Almost identical to addConst8OddInplaceSSSE3Asm.
        // DI = pointer to current main[] element.
        MOVQ    main+0(FP), DI
        MOVD    val+8(FP), X0
        MOVQ    nByte+16(FP), SI

        PXOR    X1, X1
        PSHUFB  X1, X0
        // all bytes of X0 are now equal to val

        LEAQ    -32(DI)(SI*1), BX
        CMPQ    BX, DI
        JLE     subtractFromConst8OddInplaceSSSE3Final

subtractFromConst8OddInplaceSSSE3Loop:
        MOVOU   (DI), X2
        MOVO    X0, X1
        PSUBB   X2, X1
        MOVOU   X1, (DI)
        ADDQ    $16, DI
        CMPQ    BX, DI
        JG      subtractFromConst8OddInplaceSSSE3Loop

subtractFromConst8OddInplaceSSSE3Final:
        ADDQ    $16, BX
        MOVOU   (DI), X2
        MOVOU   (BX), X3
        MOVO    X0, X1
        PSUBB   X2, X0
        PSUBB   X3, X1
        MOVOU   X0, (DI)
        MOVOU   X1, (BX)
        RET

TEXT ·subtractFromConst8SSSE3Asm(SB),4,$0-32
        // Almost identical to addConst8SSSE3Asm.
        // DI = pointer to current src[] element.
        // R8 = pointer to current dst[] element.
        MOVQ    dst+0(FP), R8
        MOVQ    src+8(FP), DI
        MOVD	val+16(FP), X0
        MOVQ	nByte+24(FP), SI

        PXOR    X1, X1
        PSHUFB  X1, X0
        // all bytes of X0 are now equal to val

        // SI = pointer to end of src[].
        ADDQ    DI, SI

subtractFromConst8SSSE3Loop:
        MOVOU   (DI), X2
        MOVO    X0, X1
        PSUBB   X2, X1
        MOVOU   X1, (R8)
        ADDQ    $16, DI
        ADDQ    $16, R8
        CMPQ    SI, DI
        JG      subtractFromConst8SSSE3Loop

        RET

TEXT ·subtractFromConst8OddSSSE3Asm(SB),4,$0-32
        // Almost identical to addConst8OddSSSE3Asm.
        // DI = pointer to current src[] element.
        // R8 = pointer to current dst[] element.
        MOVQ    dst+0(FP), R8
        MOVQ    src+8(FP), DI
        MOVD	val+16(FP), X0
        MOVQ	nByte+24(FP), BX

        PXOR    X1, X1
        PSHUFB  X1, X0

        // set AX to 16 bytes before end of src[].
        // change BX to 16 bytes before end of dst[].
        SUBQ    $16, BX
        LEAQ    0(DI)(BX*1), AX
        ADDQ    R8, BX

subtractFromConst8OddSSSE3Loop:
        MOVOU   (DI), X2
        MOVO    X0, X1
        PSUBB   X2, X1
        MOVOU   X1, (R8)
        ADDQ    $16, DI
        ADDQ    $16, R8
        CMPQ    AX, DI
        JG      subtractFromConst8OddSSSE3Loop

        // Final usually-unaligned read and write.
        MOVOU   (AX), X1
        PSUBB   X1, X0
        MOVOU   X0, (BX)
        RET
