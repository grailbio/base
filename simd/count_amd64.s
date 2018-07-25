// Copyright 2018 GRAIL, Inc.  All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build amd64,!appengine

        DATA ·Mask0f0f<>+0x00(SB)/8, $0x0f0f0f0f0f0f0f0f
        DATA ·Mask0f0f<>+0x08(SB)/8, $0x0f0f0f0f0f0f0f0f
        GLOBL ·Mask0f0f<>(SB), 24, $16
        // NOPTR = 16, RODATA = 8

        DATA LeadingByteMask<>+0x00(SB)/8, $0x0000000000000000
        DATA LeadingByteMask<>+0x08(SB)/8, $0x0000000000000000
        DATA LeadingByteMask<>+0x10(SB)/8, $0xffffffffffffffff
        DATA LeadingByteMask<>+0x18(SB)/8, $0xffffffffffffffff
        GLOBL LeadingByteMask<>(SB), 24, $32

// Original code had LOOP, which was substantially slower than JNE.  2x
// unrolling of the loop also makes a big (~35% with single thread) difference,
// probably because of POPCNTQ's 3-cycle latency.
// (4x benchmarks even better than 2x on both my test machines, but only by
// ~5%, so I'll skip it and spend any remaining implementation effort on Mula's
// faster AVX2 algorithm.)
TEXT ·popcntWordArraySSE42Asm(SB),4,$0-24
        MOVQ	nWord+8(FP), BX
        XORQ	AX, AX
        TESTQ   BX, BX
        // Length == 0?  Return immediately.
        JE      popcntWordArraySSE42Finish

        // SI = pointer to current element of array.
        MOVQ	bytes+0(FP), SI
        // DI = pointer to last element of array.
        LEAQ    -8(SI)(BX*8), DI
        ANDQ    $1, BX
        // Skip next block if array length is even.
        JE      popcntWordArraySSE42Loop

        POPCNTQ (SI), AX
        CMPQ    DI, SI
        // If array length was exactly 1, return.
        JE      popcntWordArraySSE42Finish
        ADDQ    $8, SI

popcntWordArraySSE42Loop:
        // Remaining word count must be even.  Process 2 words at a time.
        POPCNTQ (SI), DX
        ADDQ	DX, AX
        POPCNTQ 8(SI), R8
        ADDQ    R8, AX
        ADDQ	$16, SI
        CMPQ    DI, SI
        JG      popcntWordArraySSE42Loop

popcntWordArraySSE42Finish:
        MOVQ	AX, ret+16(FP)
        RET

TEXT ·maskThenCountByteSSE41Asm(SB),4,$0-40
        // This assumes nByte >= 16.  We can revisit the tiny case if it's ever
        // a bottleneck, but I'd guess that the obvious Golang loop is close
        // enough to optimal there.
        MOVQ	src+0(FP), DI
        MOVD    mask+8(FP), X0
        MOVD    val+16(FP), X1
        MOVQ    nByte+24(FP), BX

        // Make X6 a permanent all-zero vector.
        PXOR    X6, X6

        // Save start of last 16-byte vector.  This is used for both
        // loop-comparison and as the final (usually-)unaligned-load address.
        LEAQ    -16(DI)(BX*1), SI
        // Make all bytes of X0 and X1 equal to mask and val, respectively.
        PSHUFB  X6, X0
        PSHUFB  X6, X1

        MOVQ    $LeadingByteMask<>(SB), AX
        // X2 is the "inner accumulator".  Each of its bytes is a count from
        // 0-255 of how many masked bytes of src[] at the same position mod 16
        // are equal to val.
        // Up to 255*16=4080 bytes, this is enough.  Beyond that, the counts
        // might overflow, so we save the intermediate results to an "outer
        // accumulator" (X7), as a pair of uint64 counts.  Conveniently, the
        // PSADBW instruction directly converts the inner count representation
        // to the outer representation, though its latency is high enough that
        // it's best avoided in innermost loops.
        PXOR    X2, X2
        PXOR    X7, X7
        CMPQ    BX, $4080
        JG      maskThenCountByteSSE41Large

maskThenCountByteSSE41Loop:
        // Load 16 bytes from src.
        MOVOU   (DI), X3
        // Apply mask.
        PAND    X0, X3
        // Parallel-compare.
        PCMPEQB X1, X3
        // X3 now has 255 on equality, and zero on inequality.
        // Bytewise-*subtract* it from the inner accumulator, to add 1 for each
        // match.
        PSUBB   X3, X2
        // Advance to next 16 bytes, check for loop end.
        ADDQ    $16, DI
        CMPQ    SI, DI
        JG      maskThenCountByteSSE41Loop

        // Load last 16 bytes from src.  This may overlap with the previous
        // main-loop load.
        MOVOU   (SI), X3
        PAND    X0, X3
        PCMPEQB X1, X3
        // We now want to mask out the first k bytes of X3, where k is the
        // number of overlapping bytes between the last two loads.
        //
        // DI is the end of the previous load, while SI is the position of the
        // current load.  So (SI-DI) is a number in [-16, 0] which is
        // -[# of overlapping bytes].
        // AX points to 'LeadingByteMask', which is a read-only array where the
        // first 16 bytes are 0, and the next 16 bytes are 255.
        // So:
        // - if SI-DI=-16, LEAQ 16(AX)(SI*1) loads the 16 bytes starting from
        //   &(LeadingByteMask[16 + (-16)]).  These are all zero, which is the
        //   correct mask when all 16 bytes overlap.
        // - if SI-DI=-15, LEAQ 16(AX)(SI*1) loads the 16 bytes starting from
        //   &(LeadingByteMask[16 + (-15)]).  The first 15 bytes are zero, and
        //   the last byte is 255.  This is the correct mask when the first 15
        //   bytes overlap.
        // etc.
        SUBQ    DI, SI
        LEAQ    16(AX)(SI*1), DX
        MOVOU   (DX), X4
        PAND    X4, X3
        PSUBB   X3, X2
        // Now extract results.
        PSADBW  X6, X2
        PADDQ   X2, X7
        MOVQ    X7, BX
        PEXTRQ  $1, X7, AX
        ADDQ    AX, BX
        MOVQ    BX, ret+32(FP)
        RET

maskThenCountByteSSE41Large:
        MOVQ    DI, R8
        // Tried forcing 16-byte alignment, no noticeable performance impact on
        // my Mac.
        // Stop this loop after 254 rather than 255 iterations.  This way,
        // we guarantee there are at least 16 bytes left when we jump to
        // maskThenCountByteSSE41Loop.
        ADDQ    $4064, R8
maskThenCountByteSSE41LargeLoop:
        MOVOU   (DI), X3
        PAND    X0, X3
        PCMPEQB X1, X3
        PSUBB   X3, X2
        ADDQ    $16, DI
        CMPQ    R8, DI
        JG      maskThenCountByteSSE41LargeLoop

        PSADBW  X6, X2
        PADDQ   X2, X7
        PXOR    X2, X2
        SUBQ    $4064, BX
        CMPQ    BX, $4080
        JG      maskThenCountByteSSE41Large
        JMP     maskThenCountByteSSE41Loop

TEXT ·count2BytesSSE41Asm(SB),4,$0-40
        // This is almost identical to maskThenCountByteSSE41Asm.
        MOVQ	src+0(FP), DI
        MOVD    val1+8(FP), X0
        MOVD    val2+16(FP), X1
        MOVQ    nByte+24(FP), BX

        PXOR    X6, X6
        LEAQ    -16(DI)(BX*1), SI
        PSHUFB  X6, X0
        PSHUFB  X6, X1

        PXOR    X2, X2
        PXOR    X7, X7
        CMPQ    BX, $4080
        JG      count2BytesSSE41Large

count2BytesSSE41Loop:
        MOVOU   (DI), X3
        MOVO    X3, X4
        PCMPEQB X0, X3
        PCMPEQB X1, X4
        POR     X4, X3
        PSUBB   X3, X2
        ADDQ    $16, DI
        CMPQ    SI, DI
        JG      count2BytesSSE41Loop

        MOVOU   (SI), X3
        PCMPEQB X3, X0
        PCMPEQB X3, X1
        POR     X1, X0
        MOVQ    $LeadingByteMask<>(SB), AX
        SUBQ    DI, SI
        LEAQ    16(AX)(SI*1), DX
        MOVOU   (DX), X4
        PAND    X4, X0
        PSUBB   X0, X2
        PSADBW  X6, X2
        PADDQ   X2, X7
        MOVQ    X7, BX
        PEXTRQ  $1, X7, AX
        ADDQ    AX, BX
        MOVQ    BX, ret+32(FP)
        RET

count2BytesSSE41Large:
        MOVQ    DI, R8
        ADDQ    $4064, R8
count2BytesSSE41LargeLoop:
        MOVOU   (DI), X3
        MOVO    X3, X4
        PCMPEQB X0, X3
        PCMPEQB X1, X4
        POR     X4, X3
        PSUBB   X3, X2
        ADDQ    $16, DI
        CMPQ    R8, DI
        JG      count2BytesSSE41LargeLoop

        PSADBW  X6, X2
        PADDQ   X2, X7
        PXOR    X2, X2
        SUBQ    $4064, BX
        CMPQ    BX, $4080
        JG      count2BytesSSE41Large
        JMP     count2BytesSSE41Loop

TEXT ·count3BytesSSE41Asm(SB),4,$0-48
        // This is almost identical to maskThenCountByteSSE41Asm.
        MOVQ	src+0(FP), DI
        MOVD    val1+8(FP), X0
        MOVD    val2+16(FP), X1
        MOVD    val3+24(FP), X8
        MOVQ    nByte+32(FP), BX

        PXOR    X6, X6
        LEAQ    -16(DI)(BX*1), SI
        PSHUFB  X6, X0
        PSHUFB  X6, X1
        PSHUFB  X6, X8

        PXOR    X2, X2
        PXOR    X7, X7
        CMPQ    BX, $4080
        JG      count3BytesSSE41Large

count3BytesSSE41Loop:
        MOVOU   (DI), X3
        MOVO    X3, X4
        MOVO    X3, X9
        PCMPEQB X0, X3
        PCMPEQB X1, X4
        PCMPEQB X8, X9
        POR     X4, X3
        POR     X9, X3
        PSUBB   X3, X2
        ADDQ    $16, DI
        CMPQ    SI, DI
        JG      count3BytesSSE41Loop

        MOVOU   (SI), X3
        PCMPEQB X3, X0
        PCMPEQB X3, X1
        PCMPEQB X3, X8
        POR     X1, X0
        POR     X8, X0
        MOVQ    $LeadingByteMask<>(SB), AX
        SUBQ    DI, SI
        LEAQ    16(AX)(SI*1), DX
        MOVOU   (DX), X4
        PAND    X4, X0
        PSUBB   X0, X2
        PSADBW  X6, X2
        PADDQ   X2, X7
        MOVQ    X7, BX
        PEXTRQ  $1, X7, AX
        ADDQ    AX, BX
        MOVQ    BX, ret+40(FP)
        RET

count3BytesSSE41Large:
        MOVQ    DI, R8
        ADDQ    $4064, R8
count3BytesSSE41LargeLoop:
        MOVOU   (DI), X3
        MOVO    X3, X4
        MOVO    X3, X9
        PCMPEQB X0, X3
        PCMPEQB X1, X4
        PCMPEQB X8, X9
        POR     X4, X3
        POR     X9, X3
        PSUBB   X3, X2
        ADDQ    $16, DI
        CMPQ    R8, DI
        JG      count3BytesSSE41LargeLoop

        PSADBW  X6, X2
        PADDQ   X2, X7
        PXOR    X2, X2
        SUBQ    $4064, BX
        CMPQ    BX, $4080
        JG      count3BytesSSE41Large
        JMP     count3BytesSSE41Loop

TEXT ·countNibblesInSetSSE41Asm(SB),4,$0-32
        // This is a hybrid of unpackSeqSSE2Asm and the byte-counting functions
        // above.
        //
        // It assumes nSrcByte >= 16.  We can revisit the tiny case if it's
        // ever a bottleneck.
        MOVQ	src+0(FP), DI
        MOVQ    tablePtr+8(FP), DX
        MOVQ    nByte+16(FP), BX

        MOVOU   ·Mask0f0f<>(SB), X0

        // Make X8 a permanent all-zero vector.
        PXOR    X8, X8

        // Save start of last 16-byte vector.  This is used for both
        // loop-comparison and as the final (usually-)unaligned-load address.
        LEAQ    -16(DI)(BX*1), SI
        MOVOU   (DX), X1

        // X2 is the "inner accumulator".  Each of its bytes is a count from
        // 0-254 of how many bases of src[] at the same position mod 16 are
        // equal to val.
        // Up to 127*16=2032 bytes, this is enough, assuming all table entries
        // are in {0, 1}.  Beyond that, the counts might overflow, so we save
        // the intermediate results to an "outer accumulator" (X7), as a pair
        // of uint64 counts.  Conveniently, the PSADBW instruction directly
        // converts the inner count representation to the outer representation,
        // though its latency is high enough that it's best avoided in
        // innermost loops.
        PXOR    X2, X2
        PXOR    X7, X7
        CMPQ    BX, $2032
        JG      countNibblesInSetSSE41Large

countNibblesInSetSSE41Loop:
        // Load 16 bytes from src.
        MOVOU   (DI), X3
        // Separate high and low nibbles.
        MOVO    X3, X4
        PSRLQ   $4, X3
        PAND    X0, X4
        PAND    X0, X3
        // Check set membership.
        MOVO    X1, X5
        MOVO    X1, X6
        PSHUFB  X3, X5
        PSHUFB  X4, X6
        // X5 and X6 now have 1 for set members, and 0 for non-members.  Add
        // them to the inner accumulator.
        PADDB   X5, X2
        PADDB   X6, X2
        // Advance to next 16 bytes, check for loop end.
        ADDQ    $16, DI
        CMPQ    SI, DI
        JG      countNibblesInSetSSE41Loop

        // Load last 16 bytes from src.  This may overlap with the previous
        // main-loop load.
        MOVOU   (SI), X3
        MOVO    X3, X4
        PSRLQ   $4, X3
        PAND    X0, X4
        PAND    X0, X3
        MOVO    X1, X5
        PSHUFB  X3, X5
        PSHUFB  X4, X1
        MOVQ    $LeadingByteMask<>(SB), AX
        // We now want to mask out the first k bytes of X1/X5, where k is the
        // number of overlapping bytes between the last two loads.
        //
        // DI is the end of the previous load, while SI is the position of the
        // current load.  So (SI-DI) is a number in [-16, 0] which is
        // -[# of overlapping bytes].
        // AX points to 'LeadingByteMask', which is a read-only array where the
        // first 16 bytes are 0, and the next 16 bytes are 255.
        // So:
        // - if SI-DI=-16, LEAQ 16(AX)(SI*1) loads the 16 bytes starting from
        //   &(LeadingByteMask[16 + (-16)]).  These are all zero, which is the
        //   correct mask when all 16 bytes overlap.
        // - if SI-DI=-15, LEAQ 16(AX)(SI*1) loads the 16 bytes starting from
        //   &(LeadingByteMask[16 + (-15)]).  The first 15 bytes are zero, and
        //   the last byte is 255.  This is the correct mask when the first 15
        //   bytes overlap.
        // etc.
        SUBQ    DI, SI
        PADDB   X1, X5
        LEAQ    16(AX)(SI*1), DX
        MOVOU   (DX), X6
        PAND    X6, X5
        PADDB   X5, X2
        // Now extract results.
        PSADBW  X8, X2
        PADDQ   X2, X7
        MOVQ    X7, BX
        PEXTRQ  $1, X7, AX
        ADDQ    AX, BX
        MOVQ    BX, ret+24(FP)
        RET

countNibblesInSetSSE41Large:
        MOVQ    DI, R8
        // Stop this loop after 126 rather than 127 iterations.  This way, we
        // guarantee there are at least 16 bytes left when we jump to
        // countNibblesInSetSSE41Loop.
        ADDQ    $2016, R8
countNibblesInSetSSE41LargeLoop:
        MOVOU   (DI), X3
        MOVO    X3, X4
        PSRLQ   $4, X3
        PAND    X0, X4
        PAND    X0, X3
        MOVO    X1, X5
        MOVO    X1, X6
        PSHUFB  X3, X5
        PSHUFB  X4, X6
        PADDB   X5, X2
        PADDB   X6, X2
        ADDQ    $16, DI
        CMPQ    R8, DI
        JG      countNibblesInSetSSE41LargeLoop

        PSADBW  X8, X2
        PADDQ   X2, X7
        PXOR    X2, X2
        SUBQ    $2016, BX
        CMPQ    BX, $2032
        JG      countNibblesInSetSSE41Large
        JMP     countNibblesInSetSSE41Loop

TEXT ·countNibblesInTwoSetsSSE41Asm(SB),4,$0-48
        // This is a straightforward extension of countNibblesInSetSSE41Asm.
        //
        // It assumes nSrcByte >= 16.  We can revisit the tiny case if it's
        // ever a bottleneck.
        MOVQ    cnt2Ptr+0(FP), R10
        MOVQ	src+8(FP), DI
        MOVQ    table1Ptr+16(FP), DX
        MOVQ    table2Ptr+24(FP), R9
        MOVQ    nByte+32(FP), BX

        MOVOU   ·Mask0f0f<>(SB), X0

        PXOR    X13, X13

        PXOR    X2, X2
        PXOR    X9, X9
        LEAQ    -16(DI)(BX*1), SI
        MOVOU   (DX), X1
        MOVOU   (R9), X8

        PXOR    X7, X7
        PXOR    X10, X10
        CMPQ    BX, $2032
        JG      countNibblesInTwoSetsSSE41Large

countNibblesInTwoSetsSSE41Loop:
        MOVOU   (DI), X3
        MOVO    X3, X4
        PSRLQ   $4, X3
        PAND    X0, X4
        PAND    X0, X3
        MOVO    X1, X5
        MOVO    X1, X6
        MOVO    X8, X11
        MOVO    X8, X12
        PSHUFB  X3, X5
        PSHUFB  X4, X6
        PSHUFB  X3, X11
        PSHUFB  X4, X12
        PADDB   X5, X2
        PADDB   X11, X9
        PADDB   X6, X2
        PADDB   X12, X9
        ADDQ    $16, DI
        CMPQ    SI, DI
        JG      countNibblesInTwoSetsSSE41Loop

        MOVOU   (SI), X3
        MOVO    X3, X4
        PSRLQ   $4, X3
        PAND    X0, X4
        PAND    X0, X3
        MOVO    X1, X5
        MOVO    X8, X11
        PSHUFB  X3, X5
        PSHUFB  X4, X1
        PSHUFB  X3, X11
        PSHUFB  X4, X8
        MOVQ    $LeadingByteMask<>(SB), AX
        SUBQ    DI, SI
        PADDB   X1, X5
        PADDB   X8, X11
        LEAQ    16(AX)(SI*1), DX
        MOVOU   (DX), X6
        PAND    X6, X5
        PAND    X6, X11
        PADDB   X5, X2
        PADDB   X11, X9

        PSADBW  X13, X2
        PSADBW  X13, X9
        PADDQ   X2, X7
        PADDQ   X9, X10
        MOVQ    X7, BX
        MOVQ    X10, R9
        PEXTRQ  $1, X7, AX
        ADDQ    (R10), R9
        PEXTRQ  $1, X10, R11
        ADDQ    AX, BX
        ADDQ    R11, R9
        MOVQ    R9, (R10)
        MOVQ    BX, ret+40(FP)
        RET

countNibblesInTwoSetsSSE41Large:
        MOVQ    DI, R8
        ADDQ    $2016, R8
countNibblesInTwoSetsSSE41LargeLoop:
        MOVOU   (DI), X3
        MOVO    X3, X4
        PSRLQ   $4, X3
        PAND    X0, X4
        PAND    X0, X3
        MOVO    X1, X5
        MOVO    X1, X6
        MOVO    X8, X11
        MOVO    X8, X12
        PSHUFB  X3, X5
        PSHUFB  X4, X6
        PSHUFB  X3, X11
        PSHUFB  X4, X12
        PADDB   X5, X2
        PADDB   X11, X9
        PADDB   X6, X2
        PADDB   X12, X9
        ADDQ    $16, DI
        CMPQ    R8, DI
        JG      countNibblesInTwoSetsSSE41LargeLoop

        PSADBW  X13, X2
        PSADBW  X13, X9
        PADDQ   X2, X7
        PADDQ   X9, X10
        PXOR    X2, X2
        PXOR    X9, X9
        SUBQ    $2016, BX
        CMPQ    BX, $2032
        JG      countNibblesInTwoSetsSSE41Large
        JMP     countNibblesInTwoSetsSSE41Loop

TEXT ·countUnpackedNibblesInSetSSE41Asm(SB),4,$0-32
        // This is a slightly simpler variant of countNibblesInSetSSE41Asm (we
        // ignore the high bits of each byte).
        //
        // It assumes nSrcByte >= 16.  We can revisit the tiny case if it's
        // ever a bottleneck.
        MOVQ	src+0(FP), DI
        MOVQ    tablePtr+8(FP), DX
        MOVQ    nByte+16(FP), BX

        // Make X8 a permanent all-zero vector.
        PXOR    X8, X8

        // Save start of last 16-byte vector.  This is used for both
        // loop-comparison and as the final (usually-)unaligned-load address.
        LEAQ    -16(DI)(BX*1), SI
        MOVOU   (DX), X1

        // X2 is the "inner accumulator".  Each of its bytes is a count from
        // 0-255 of how many bases of src[] at the same position mod 16 are
        // equal to val.
        // Up to 255*16=4080 bytes, this is enough, assuming all table entries
        // are in {0, 1}.  Beyond that, the counts might overflow, so we save
        // the intermediate results to an "outer accumulator" (X7), as a pair
        // of uint64 counts.  Conveniently, the PSADBW instruction directly
        // converts the inner count representation to the outer representation,
        // though its latency is high enough that it's best avoided in
        // innermost loops.
        PXOR    X2, X2
        PXOR    X7, X7
        CMPQ    BX, $4080
        JG      countUnpackedNibblesInSetSSE41Large

countUnpackedNibblesInSetSSE41Loop:
        // Load 16 bytes from src.
        MOVOU   (DI), X3
        // Check set membership.
        MOVO    X1, X5
        PSHUFB  X3, X5
        // X5 now has 1 for set members, and 0 for non-members.  Add to the
        // inner accumulator.
        PADDB   X5, X2
        // Advance to next 16 bytes, check for loop end.
        ADDQ    $16, DI
        CMPQ    SI, DI
        JG      countUnpackedNibblesInSetSSE41Loop

        // Load last 16 bytes from src.  This may overlap with the previous
        // main-loop load.
        MOVOU   (SI), X3
        PSHUFB  X3, X1
        MOVQ    $LeadingByteMask<>(SB), AX
        // We now want to mask out the first k bytes of X1, where k is the
        // number of overlapping bytes between the last two loads.
        //
        // DI is the end of the previous load, while SI is the position of the
        // current load.  So (SI-DI) is a number in [-16, 0] which is
        // -[# of overlapping bytes].
        // AX points to 'LeadingByteMask', which is a read-only array where the
        // first 16 bytes are 0, and the next 16 bytes are 255.
        // So:
        // - if SI-DI=-16, LEAQ 16(AX)(SI*1) loads the 16 bytes starting from
        //   &(LeadingByteMask[16 + (-16)]).  These are all zero, which is the
        //   correct mask when all 16 bytes overlap.
        // - if SI-DI=-15, LEAQ 16(AX)(SI*1) loads the 16 bytes starting from
        //   &(LeadingByteMask[16 + (-15)]).  The first 15 bytes are zero, and
        //   the last byte is 255.  This is the correct mask when the first 15
        //   bytes overlap.
        // etc.
        SUBQ    DI, SI
        LEAQ    16(AX)(SI*1), DX
        MOVOU   (DX), X6
        PAND    X6, X1
        PADDB   X1, X2
        // Now extract results.
        PSADBW  X8, X2
        PADDQ   X2, X7
        MOVQ    X7, BX
        PEXTRQ  $1, X7, AX
        ADDQ    AX, BX
        MOVQ    BX, ret+24(FP)
        RET

countUnpackedNibblesInSetSSE41Large:
        MOVQ    DI, R8
        // Stop this loop after 254 rather than 255 iterations.  This way, we
        // guarantee there are at least 16 bytes left when we jump to
        // countUnpackedNibblesInSetSSE41Loop.
        ADDQ    $4064, R8
countUnpackedNibblesInSetSSE41LargeLoop:
        MOVOU   (DI), X3
        MOVO    X1, X5
        PSHUFB  X3, X5
        PADDB   X5, X2
        ADDQ    $16, DI
        CMPQ    R8, DI
        JG      countUnpackedNibblesInSetSSE41LargeLoop

        PSADBW  X8, X2
        PADDQ   X2, X7
        PXOR    X2, X2
        SUBQ    $4064, BX
        CMPQ    BX, $4080
        JG      countUnpackedNibblesInSetSSE41Large
        JMP     countUnpackedNibblesInSetSSE41Loop

TEXT ·countUnpackedNibblesInTwoSetsSSE41Asm(SB),4,$0-48
        // This is a slightly simpler variant of countNibblesInTwoSetsSSE41Asm
        // (we ignore the high bits of each byte).
        //
        // It assumes nSrcByte >= 16.  We can revisit the tiny case if it's
        // ever a bottleneck.
        MOVQ    cnt2Ptr+0(FP), R10
        MOVQ	src+8(FP), DI
        MOVQ    table1Ptr+16(FP), DX
        MOVQ    table2Ptr+24(FP), R9
        MOVQ    nByte+32(FP), BX

        PXOR    X13, X13
        PXOR    X2, X2
        PXOR    X9, X9
        LEAQ    -16(DI)(BX*1), SI
        MOVOU   (DX), X1
        MOVOU   (R9), X8

        PXOR    X7, X7
        PXOR    X10, X10
        CMPQ    BX, $4080
        JG      countUnpackedNibblesInTwoSetsSSE41Large

countUnpackedNibblesInTwoSetsSSE41Loop:
        MOVOU   (DI), X3
        MOVO    X1, X5
        MOVO    X8, X11
        PSHUFB  X3, X5
        PSHUFB  X3, X11
        PADDB   X5, X2
        PADDB   X11, X9
        ADDQ    $16, DI
        CMPQ    SI, DI
        JG      countUnpackedNibblesInTwoSetsSSE41Loop

        MOVOU   (SI), X3
        PSHUFB  X3, X1
        PSHUFB  X3, X8
        MOVQ    $LeadingByteMask<>(SB), AX
        SUBQ    DI, SI
        LEAQ    16(AX)(SI*1), DX
        MOVOU   (DX), X6
        PAND    X6, X1
        PAND    X6, X8
        PADDB   X1, X2
        PADDB   X8, X9

        PSADBW  X13, X2
        PSADBW  X13, X9
        PADDQ   X2, X7
        PADDQ   X9, X10
        MOVQ    X7, BX
        MOVQ    X10, R9
        PEXTRQ  $1, X7, AX
        ADDQ    (R10), R9
        PEXTRQ  $1, X10, R11
        ADDQ    AX, BX
        ADDQ    R11, R9
        MOVQ    R9, (R10)
        MOVQ    BX, ret+40(FP)
        RET

countUnpackedNibblesInTwoSetsSSE41Large:
        MOVQ    DI, R8
        ADDQ    $4064, R8
countUnpackedNibblesInTwoSetsSSE41LargeLoop:
        MOVOU   (DI), X3
        MOVO    X1, X5
        MOVO    X8, X11
        PSHUFB  X3, X5
        PSHUFB  X3, X11
        PADDB   X5, X2
        PADDB   X11, X9
        ADDQ    $16, DI
        CMPQ    R8, DI
        JG      countUnpackedNibblesInTwoSetsSSE41LargeLoop

        PSADBW  X13, X2
        PSADBW  X13, X9
        PADDQ   X2, X7
        PADDQ   X9, X10
        PXOR    X2, X2
        PXOR    X9, X9
        SUBQ    $4064, BX
        CMPQ    BX, $4080
        JG      countUnpackedNibblesInTwoSetsSSE41Large
        JMP     countUnpackedNibblesInTwoSetsSSE41Loop

TEXT ·accumulate8SSE41Asm(SB),4,$0-24
        // This assumes nByte >= 16.
        MOVQ	src+0(FP), DI
        MOVQ    nByte+8(FP), BX

        // X0 is a pair of uint64s containing partial sums.
        PXOR    X0, X0
        // X1 is a fixed all-zero vector.
        PXOR    X1, X1
        // SI points to 32 bytes before the end of src[].
        // (2x unroll improves the long-array benchmark by ~7%.)
        LEAQ    -32(DI)(BX*1), SI
        CMPQ    SI, DI
        JLE     accumulate8SSE41Final32

accumulate8SSE41Loop:
        MOVOU   (DI), X2
        MOVOU   16(DI), X3
        PSADBW  X1, X2
        PSADBW  X1, X3
        ADDQ    $32, DI
        PADDQ   X2, X0
        PADDQ   X3, X0
        CMPQ    SI, DI
        JG      accumulate8SSE41Loop

accumulate8SSE41Final32:
        ADDQ    $16, SI
        CMPQ    SI, DI
        JLE     accumulate8SSE41Final16
        MOVOU   (DI), X2
        PSADBW  X1, X2
        ADDQ    $16, DI
        PADDQ   X2, X0

accumulate8SSE41Final16:
        // Load last bytes, use LeadingByteMask to avoid double-counting.
        MOVOU   (SI), X2
        MOVQ    $LeadingByteMask<>(SB), AX
        SUBQ    DI, SI
        LEAQ    16(AX)(SI*1), DX
        MOVOU   (DX), X3
        PAND    X3, X2
        PSADBW  X1, X2
        PADDQ   X2, X0

        // Extract final sum.
        MOVQ    X0, BX
        PEXTRQ  $1, X0, AX
        ADDQ    AX, BX
        MOVQ    BX, ret+16(FP)
        RET

TEXT ·accumulate8GreaterSSE41Asm(SB),4,$0-32
        // Variant of accumulate8 that masks out bytes <= the given value.
        // If all bytes < 128, it is possible to speed this up by ~2-7% by
        // replacing the saturating-subtract + equality-to-zero combination
        // with a single _mm_cmpgt_epi8() operation.  But this is supposed to
        // be a safe function, so I don't think that minor gain is worth the
        // unvalidated condition.
        //
        // This assumes nByte >= 16.
        MOVQ	src+0(FP), DI
        MOVD    val+8(FP), X4
        MOVQ    nByte+16(FP), BX

        // X1 is a fixed all-zero vector.
        PXOR    X1, X1
        // X0 is a pair of uint64s containing partial sums.
        PXOR    X0, X0

        PSHUFB  X1, X4
        // X4 now has all bytes equal to val.

        // SI points to 16 bytes before the end of src[].
        LEAQ    -16(DI)(BX*1), SI

accumulate8GreaterSSE41Loop:
        MOVOU   (DI), X2
        MOVO    X2, X5
        PSUBUSB X4, X2
        // X2 is 0 for all bytes originally <= val, and nonzero elsewhere.
        PCMPEQB X1, X2
        // X2 is now 255 for all bytes originally <= val, and 0 elsewhere.
        PANDN   X5, X2
        PSADBW  X1, X2
        ADDQ    $16, DI
        PADDQ   X2, X0
        CMPQ    SI, DI
        JG      accumulate8GreaterSSE41Loop

        // Load last bytes, use LeadingByteMask to avoid double-counting.
        MOVOU   (SI), X2
        MOVQ    $LeadingByteMask<>(SB), AX
        MOVO    X2, X5
        SUBQ    DI, SI
        PSUBUSB X4, X2
        LEAQ    16(AX)(SI*1), DX
        PCMPEQB X1, X2
        MOVOU   (DX), X3
        PANDN   X5, X2
        PAND    X3, X2
        PSADBW  X1, X2
        PADDQ   X2, X0

        // Extract final sum.
        MOVQ    X0, BX
        PEXTRQ  $1, X0, AX
        ADDQ    AX, BX
        MOVQ    BX, ret+24(FP)
        RET
