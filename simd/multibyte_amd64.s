// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build amd64,!appengine

        DATA ·Reverse16<>+0x00(SB)/8, $0x09080b0a0d0c0f0e
        DATA ·Reverse16<>+0x08(SB)/8, $0x0100030205040706
        GLOBL ·Reverse16<>(SB), 24, $16
        // NOPTR = 16, RODATA = 8

TEXT ·index16SSE2Asm(SB),4,$0-32
        // index16SSE2Asm scans main[], searching for the first instance of
        // val.  If no instances are found, it returns -1.
        // It requires nElem >= 8.
        // The implementation is based on a loop which uses _mm_cmpeq_epi16()
        // to scan 8 uint16s in parallel, and _mm_movemask_epi8() to extract
        // the result of that scan.  It is similar to firstLeq8 in cmp_amd64.s.

        // There's a ~10% benefit from 2x-unrolling the main loop so that only
        // one test is performed per loop iteration (i.e. just look at the
        // bitwise-or of the comparison results, and backtrack a bit on a hit).
        // I'll leave that on the table for now to keep the logic simpler.

        // Register allocation:
        //   AX: pointer to start of main[]
        //   BX: nElem - 8
        //   CX: current index
        //   X0: vector with 8 copies of val
        MOVQ    main+0(FP), AX

        // clang compiles _mm_set1_epi16() to this, I'll trust it.
        MOVQ    val+8(FP), X0
        PSHUFLW $0xe0, X0, X0
        PSHUFD  $0, X0, X0

        MOVQ    nElem+16(FP), BX
        SUBQ    $8, BX
        XORL    CX, CX

index16SSE2AsmLoop:
        // Scan 8 elements starting from &(main[CX]).
        MOVOU   (AX)(CX*2), X1
        PCMPEQW X0, X1
        PMOVMSKB        X1, DX
        // Bits 2k and 2k+1 are now set in DX iff the uint16 at position k
        // compared equal.
        TESTQ   DX, DX
        JNE     index16SSE2AsmFound
        ADDQ    $8, CX
        CMPQ    BX, CX
        JG      index16SSE2AsmLoop

        // Scan the last 8 elements; this may partially overlap with the
        // previous scan.
        MOVQ    BX, CX
        MOVOU   (AX)(CX*2), X1
        PCMPEQW X0, X1
        PMOVMSKB        X1, DX
        TESTQ   DX, DX
        JNE     index16SSE2AsmFound
        // No match found, return -1.
        MOVQ    $-1, ret+24(FP)
        RET

index16SSE2AsmFound:
        BSFQ    DX, AX
        // AX now has the index of the lowest set bit in DX.
        SHRQ    $1, AX
        ADDQ    CX, AX
        MOVQ    AX, ret+24(FP)
        RET

TEXT ·reverse16InplaceSSSE3Asm(SB),4,$0-16
        // This is only called with nElem > 8.  So we can safely divide this
        // into two cases:
        // 1. (nElem+7) % 16 in {0..7}.  Execute (nElem+7)/16 normal iterations
        //    and exit.  Last two writes usually overlap.
        // 2. (nElem+7) % 16 in {8..15}.  Execute (nElem-9)/16 normal
        //    iterations.  Then we have between 17 and 24 central elements
        //    left; handle them by processing *three* vectors at once at the
        //    end.
        // Logic is essentially identical to reverseComp4InplaceSSSE3Asm,
        // except we don't need to complement here.
        MOVQ    main+0(FP), SI
        MOVQ    nElem+8(FP), AX

        // DI iterates backwards from the end of seq8[].
        LEAQ    -16(SI)(AX*2), DI

        MOVOU   ·Reverse16<>(SB), X0
        SUBQ    $1, AX
        MOVQ    AX, BX
        ANDQ    $8, BX
        // BX is now 0 when we don't need to process 3 vectors at the end, and
        // 8 when we do.
        LEAQ    0(AX)(BX*2), R9
        // R9 is now nElem+15 when we don't need to process 3 vectors at the
        // end, and nElem-1 when we do.
        LEAQ    -24(SI)(R9*1), AX
        // AX can now be used for the loop termination check:
        //   if nElem == 9, R9 == 24, so AX == uintptr(main) + 0.
        //   if nElem == 16, R9 == 31, so AX == uintptr(main) + 7.
        //   if nElem == 17, R9 == 16, so AX == uintptr(main) - 8.
        //   if nElem == 24, R9 == 23, so AX == uintptr(main) - 1.
        CMPQ    AX, SI
        JL      reverse16InplaceSSSE3LastThree

reverse16InplaceSSSE3Loop:
        MOVOU   (SI), X1
        MOVOU   (DI), X2
        PSHUFB  X0, X1
        PSHUFB  X0, X2
        MOVOU   X2, (SI)
        MOVOU   X1, (DI)
        ADDQ    $16, SI
        SUBQ    $16, DI
        CMPQ    AX, SI
        JGE     reverse16InplaceSSSE3Loop

        TESTQ   BX, BX
        JNE     reverse16InplaceSSSE3Ret
reverse16InplaceSSSE3LastThree:
        MOVOU   (SI), X1
        MOVOU   16(SI), X2
        MOVOU   (DI), X3
        PSHUFB  X0, X1
        PSHUFB  X0, X2
        PSHUFB  X0, X3
        MOVOU   X3, (SI)
        MOVOU   X2, -16(DI)
        MOVOU   X1, (DI)

reverse16InplaceSSSE3Ret:
        RET

TEXT ·reverse16SSSE3Asm(SB),4,$0-24
        MOVQ    dst+0(FP), DI
        MOVQ    src+8(FP), SI
        MOVQ    nElem+16(FP), AX

        // R8 iterates backwards from the end of src[].
        LEAQ    -16(SI)(AX*2), R8
        MOVOU   ·Reverse16<>(SB), X0
        // Save final dst[] pointer for later.
        LEAQ    -16(DI)(AX*2), R9

reverse16SSSE3Loop:
        MOVOU   (R8), X1
        PSHUFB  X0, X1
        MOVOU   X1, (DI)
        SUBQ    $16, R8
        ADDQ    $16, DI
        CMPQ    SI, R8
        JL      reverse16SSSE3Loop

        MOVOU   (SI), X1
        PSHUFB  X0, X1
        MOVOU   X1, (R9)
        RET
