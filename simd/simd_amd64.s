// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build amd64,!appengine

        DATA ·Mask0f0f<>+0x00(SB)/8, $0x0f0f0f0f0f0f0f0f
        DATA ·Mask0f0f<>+0x08(SB)/8, $0x0f0f0f0f0f0f0f0f
        // NOPTR = 16, RODATA = 8
        GLOBL ·Mask0f0f<>(SB), 24, $16

        DATA ·Reverse8<>+0x00(SB)/8, $0x08090a0b0c0d0e0f
        DATA ·Reverse8<>+0x08(SB)/8, $0x0001020304050607
        GLOBL ·Reverse8<>(SB), 24, $16

TEXT ·unpackedNibbleLookupTinyInplaceSSSE3Asm(SB),4,$0-16
        // DI = pointer to current main[] element.
        MOVQ    main+0(FP), DI
        MOVQ	tablePtr+8(FP), SI
        MOVOU   (SI), X0
        MOVOU   (DI), X1
        PSHUFB  X1, X0
        MOVOU   X0, (DI)
        RET

TEXT ·unpackedNibbleLookupOddInplaceSSSE3Asm(SB),4,$0-24
        // DI = pointer to current main[] element.
        MOVQ    main+0(FP), DI
        MOVQ	tablePtr+8(FP), SI
        MOVQ	nByte+16(FP), R9

        MOVOU   (SI), X0

        // set AX to 32 bytes before end of main[].
        LEAQ    -32(DI)(R9*1), AX

        CMPQ    AX, DI
        JLE     unpackedNibbleLookupOddInplaceSSSE3Finish

// 'Odd' refers to handling of byte counts which aren't multiples of
// bytesPerVec.  They don't have to literally be odd (or non-multiples of
// bytesPerVec, for that matter).
unpackedNibbleLookupOddInplaceSSSE3Loop:
        MOVOU   (DI), X1
        MOVO    X0, X2
        PSHUFB  X1, X2
        MOVOU   X2, (DI)
        ADDQ    $16, DI
        CMPQ    AX, DI
        JG      unpackedNibbleLookupOddInplaceSSSE3Loop

unpackedNibbleLookupOddInplaceSSSE3Finish:
        // These loads usually overlap, so they must both occur before the
        // first write-back.
        ADDQ    $16, AX
        MOVOU   (DI), X1
        MOVO    X0, X2
        MOVOU   (AX), X3
        PSHUFB  X1, X2
        PSHUFB  X3, X0
        MOVOU   X2, (DI)
        MOVOU   X0, (AX)
        RET

TEXT ·unpackedNibbleLookupSSSE3Asm(SB),4,$0-32
        // DI = pointer to current src[] element.
        // R8 = pointer to current dst[] element.
        MOVQ    dst+0(FP), R8
        MOVQ    src+8(FP), DI
        MOVQ	tablePtr+16(FP), SI
        MOVQ	nByte+24(FP), R9

        MOVOU   (SI), X0

        // R9 = pointer to end of src[].
        ADDQ    DI, R9

unpackedNibbleLookupSSSE3Loop:
        MOVOU   (DI), X1
        MOVO    X0, X2
        PSHUFB  X1, X2
        MOVOU   X2, (R8)
        ADDQ    $16, DI
        ADDQ    $16, R8
        CMPQ    R9, DI
        JG      unpackedNibbleLookupSSSE3Loop

        RET

TEXT ·unpackedNibbleLookupOddSSSE3Asm(SB),4,$0-32
        // DI = pointer to current src[] element.
        // R8 = pointer to current dst[] element.
        MOVQ    dst+0(FP), R8
        MOVQ    src+8(FP), DI
        MOVQ	tablePtr+16(FP), SI
        MOVQ	nByte+24(FP), R9

        MOVOU   (SI), X0

        // set AX to 16 bytes before end of src[].
        // change R9 to 16 bytes before end of dst[].
        SUBQ    $16, R9
        LEAQ    0(DI)(R9*1), AX
        ADDQ    R8, R9

unpackedNibbleLookupOddSSSE3Loop:
        MOVOU   (DI), X1
        MOVO    X0, X2
        PSHUFB  X1, X2
        MOVOU   X2, (R8)
        ADDQ    $16, DI
        ADDQ    $16, R8
        CMPQ    AX, DI
        JG      unpackedNibbleLookupOddSSSE3Loop

        // Final usually-unaligned read and write.
        MOVOU   (AX), X1
        PSHUFB  X1, X0
        MOVOU   X0, (R9)
        RET

TEXT ·packedNibbleLookupSSSE3Asm(SB),4,$0-32
        // DI = pointer to current src[] element.
        // R8 = pointer to current dst[] element.
        MOVQ    dst+0(FP), R8
        MOVQ    src+8(FP), DI
        MOVQ	tablePtr+16(FP), SI
        MOVQ	nSrcByte+24(FP), R9

        MOVOU   (SI), X0
        MOVOU   ·Mask0f0f<>(SB), X1

        // AX = pointer to last relevant word of src[].
        // (note that 8 src bytes -> 16 dst bytes)
        LEAQ    -8(DI)(R9*1), AX
        CMPQ    AX, DI
        JLE     packedNibbleLookupSSSE3Final

packedNibbleLookupSSSE3Loop:
        MOVOU   (DI), X3
        MOVO    X0, X4
        MOVO    X0, X5
        // Isolate high and low nibbles, then parallel-lookup.
        MOVO    X3, X2
        PSRLQ   $4, X3
        PAND    X1, X2
        PAND    X1, X3
        PSHUFB  X2, X4
        PSHUFB  X3, X5
        // Use unpacklo/unpackhi to stitch results together.
        // Even bytes (0, 2, 4, ...) are in X2/X4, odd in X5.
        MOVO    X4, X2
        PUNPCKLBW       X5, X4
        PUNPCKHBW       X5, X2
        MOVOU   X4, (R8)
        MOVOU   X2, 16(R8)
        ADDQ    $16, DI
        ADDQ    $32, R8
        CMPQ    AX, DI
        JG      packedNibbleLookupSSSE3Loop
packedNibbleLookupSSSE3Final:
        // Necessary to write one more vector.  We skip unpackhi, but must
        // execute the rest of the loop body.
        MOVOU   (DI), X3
        MOVO    X0, X4
        MOVO    X3, X2
        PSRLQ   $4, X3
        PAND    X1, X2
        PAND    X1, X3
        PSHUFB  X2, X4
        PSHUFB  X3, X0
        PUNPCKLBW       X0, X4
        MOVOU   X4, (R8)
        RET

TEXT ·packedNibbleLookupOddSSSE3Asm(SB),4,$0-32
        // DI = pointer to current src[] element.
        // R8 = pointer to current dst[] element.
        MOVQ    dst+0(FP), R8
        MOVQ    src+8(FP), DI
        MOVQ	tablePtr+16(FP), SI
        MOVQ	nSrcFullByte+24(FP), R9

        MOVOU   (SI), X0
        MOVOU   ·Mask0f0f<>(SB), X1

        // set AX to 32 bytes before end of dst[].
        // change R9 to 16 bytes before end of src[].
        SUBQ    $16, R9
        LEAQ    0(R8)(R9*2), AX
        ADDQ    DI, R9

packedNibbleLookupOddSSSE3Loop:
        MOVOU   (DI), X3
        MOVO    X0, X4
        MOVO    X0, X5
        // Isolate high and low nibbles, then parallel-lookup.
        MOVO    X3, X2
        PSRLQ   $4, X3
        PAND    X1, X2
        PAND    X1, X3
        PSHUFB  X2, X4
        PSHUFB  X3, X5
        // Use unpacklo/unpackhi to stitch results together.
        // Even bytes (0, 2, 4, ...) are in X2/X4, odd in X5.
        MOVO    X4, X2
        PUNPCKLBW       X5, X4
        PUNPCKHBW       X5, X2
        MOVOU   X4, (R8)
        MOVOU   X2, 16(R8)
        ADDQ    $16, DI
        ADDQ    $32, R8
        CMPQ    R9, DI
        JG      packedNibbleLookupOddSSSE3Loop

        // Final usually-unaligned read and write.
        MOVOU   (R9), X3
        MOVO    X0, X4
        MOVO    X3, X2
        PSRLQ   $4, X3
        PAND    X1, X2
        PAND    X1, X3
        PSHUFB  X2, X4
        PSHUFB  X3, X0
        MOVO    X4, X2
        PUNPCKLBW       X0, X4
        PUNPCKHBW       X0, X2
        MOVOU   X4, (AX)
        MOVOU   X2, 16(AX)
        RET

TEXT ·interleave8SSE2Asm(SB),4,$0-32
        MOVQ    dst+0(FP), R8
        MOVQ    even+8(FP), SI
        MOVQ    odd+16(FP), DI
        MOVQ    nDstByte+24(FP), R9

        // AX = pointer to last vec of dst[].
        LEAQ    -16(R8)(R9*1), AX
        CMPQ    AX, R8
        JLE     interleave8SSE2Final

interleave8SSE2Loop:
        // Read 16 bytes from even[] and odd[], and use _mm_unpacklo_epi8() and
        // _mm_unpackhi_epi8() to interleave and write 32 bytes to dst[].
        MOVOU   (SI), X0
        MOVOU   (DI), X1
        MOVO    X0, X2
        PUNPCKLBW       X1, X0
        PUNPCKHBW       X1, X2
        MOVOU   X0, (R8)
        MOVOU   X2, 16(R8)
        ADDQ    $16, SI
        ADDQ    $32, R8
        ADDQ    $16, DI
        CMPQ    AX, R8
        JG      interleave8SSE2Loop

interleave8SSE2Final:
        MOVOU   (SI), X0
        MOVOU   (DI), X1
        PUNPCKLBW       X1, X0
        MOVOU   X0, (R8)
        RET

TEXT ·interleave8OddSSE2Asm(SB),4,$0-32
        MOVQ    dst+0(FP), R8
        MOVQ    even+8(FP), SI
        MOVQ    odd+16(FP), DI
        MOVQ    nOddByte+24(FP), DX

        // AX = 16 bytes before end of even[].
        LEAQ    -16(SI)(DX*1), AX
        LEAQ    -16(DI)(DX*1), BX
        LEAQ    -32(R8)(DX*2), R9

interleave8OddSSE2Loop:
        // At least 32 bytes left to write to dst[].
        MOVOU   (SI), X0
        MOVOU   (DI), X1
        MOVO    X0, X2
        PUNPCKLBW       X1, X0
        PUNPCKHBW       X1, X2
        MOVOU   X0, (R8)
        MOVOU   X2, 16(R8)
        ADDQ    $16, SI
        ADDQ    $16, DI
        ADDQ    $32, R8
        CMPQ    AX, SI
        JG      interleave8OddSSE2Loop

        // Final read/write: back up to 16 bytes before end of even[]/odd[] and
        // 32 bytes before end of dst[].  This will usually re-write a bunch of
        // dst[] bytes, but that's okay.
        MOVOU   (AX), X0
        MOVOU   (BX), X1
        MOVO    X0, X2
        PUNPCKLBW       X1, X0
        PUNPCKHBW       X1, X2
        MOVOU   X0, (R9)
        MOVOU   X2, 16(R9)
        RET

TEXT ·reverse8InplaceSSSE3Asm(SB),4,$0-16
        MOVQ    main+0(FP), SI
        MOVQ    nByte+8(FP), AX

        // DI iterates backwards from the end of main[].
        LEAQ    -16(SI)(AX*1), DI
        CMPQ    SI, DI
        JGE     reverse8InplaceSSSE3Final
        MOVOU   ·Reverse8<>(SB), X0

reverse8InplaceSSSE3Loop:
        MOVOU   (SI), X1
        MOVOU   (DI), X2
        PSHUFB  X0, X1
        PSHUFB  X0, X2
        MOVOU   X2, (SI)
        MOVOU   X1, (DI)
        ADDQ    $16, SI
        SUBQ    $16, DI
        CMPQ    SI, DI
        JL      reverse8InplaceSSSE3Loop

reverse8InplaceSSSE3Final:
        // 16 or fewer bytes left, [SI, DI+16).
        // If 8..16, load two words, bswap64, write back.
        // If 4..7, load two u32s, bswap32, write back.
        // If 2..3, swap two bytes.
        // If <= 1 (can be as small as -16), return immediately.
        SUBQ    SI, DI
        // Now DI has remaining byte count - 16.
        CMPQ    DI, $-14
        JL      reverse8InplaceSSSE3Ret
        CMPQ    DI, $-8
        JL      reverse8InplaceSSSE3TwoThroughSeven
        LEAQ    8(SI)(DI*1), BX
        MOVQ    (SI), R8
        MOVQ    (BX), R9
        BSWAPQ  R8
        BSWAPQ  R9
        MOVQ    R9, (SI)
        MOVQ    R8, (BX)

reverse8InplaceSSSE3Ret:
        RET

reverse8InplaceSSSE3TwoThroughSeven:
        CMPQ    DI, $-12
        JL      reverse8InplaceSSSE3TwoOrThree
        LEAQ    12(SI)(DI*1), BX
        MOVL    (SI), R8
        MOVL    (BX), R9
        BSWAPL  R8
        BSWAPL  R9
        MOVL    R9, (SI)
        MOVL    R8, (BX)
        RET

reverse8InplaceSSSE3TwoOrThree:
        LEAQ    15(SI)(DI*1), BX
        MOVB    (SI), R8
        MOVB    (BX), R9
        MOVB    R8, (BX)
        MOVB    R9, (SI)
        RET

TEXT ·reverse8SSSE3Asm(SB),4,$0-24
        MOVQ    dst+0(FP), DI
        MOVQ    src+8(FP), SI
        MOVQ    nByte+16(FP), DX

        // R8 iterates backwards from the end of src[].
        LEAQ    -16(SI)(DX*1), R8
        MOVOU   ·Reverse8<>(SB), X0
        // Save final dst[] pointer for later.
        LEAQ    -16(DI)(DX*1), R9

reverse8SSSE3Loop:
        MOVOU   (R8), X1
        PSHUFB  X0, X1
        MOVOU   X1, (DI)
        SUBQ    $16, R8
        ADDQ    $16, DI
        CMPQ    SI, R8
        JL      reverse8SSSE3Loop

        MOVOU   (SI), X1
        PSHUFB  X0, X1
        MOVOU   X1, (R9)
        RET

TEXT ·bitFromEveryByteSSE2Asm(SB),4,$0-32
        // bitFromEveryByteSSE2Asm grabs a single bit from every src[] byte,
        // and packs them into dst[].
        // The implementation is based on the _mm_movemask_epi8() instruction,
        // which grabs the *high* bit from each byte, so this function takes a
        // 'lshift' argument instead of the wrapper's bitIdx.

        // Register allocation:
        //   AX: pointer to start of dst
        //   BX: pointer to start of src
        //   CX: nDstByte (must be even), minus 2 to support 2x unroll
        //       (rule of thumb: if the loop is less than ~10 operations,
        //       unrolling is likely to make a noticeable difference with
        //       minimal effort; otherwise don't bother)
        //   DX: loop counter
        //   SI, DI: intermediate movemask results
        //
        //   X0: lshift
        MOVQ    dst+0(FP), AX
        MOVQ    src+8(FP), BX
        MOVQ    lshift+16(FP), X0

        MOVQ    nDstByte+24(FP), CX
        SUBQ    $2, CX
        // Compilers emit this instead of XORQ DX,DX since it's smaller and has
        // the same effect.
        XORL    DX, DX

        CMPQ    CX, DX
        JLE     bitFromEveryByteSSE2AsmOdd

bitFromEveryByteSSE2AsmLoop:
        MOVOU   (BX)(DX*8), X1
        MOVOU   16(BX)(DX*8), X2
        PSLLQ   X0, X1
        PSLLQ   X0, X2
        PMOVMSKB        X1, SI
        PMOVMSKB        X2, DI
        MOVW    SI, (AX)(DX*1)
        MOVW    DI, 2(AX)(DX*1)
        ADDQ    $4, DX
        CMPQ    CX, DX
        JG      bitFromEveryByteSSE2AsmLoop

        JL      bitFromEveryByteSSE2AsmFinish

        // Move this label up one line if we ever need to accept nDstByte == 0.
bitFromEveryByteSSE2AsmOdd:
        MOVOU   (BX)(DX*8), X1
        PSLLQ   X0, X1
        PMOVMSKB        X1, SI
        MOVW    SI, (AX)(DX*1)

bitFromEveryByteSSE2AsmFinish:
        RET
