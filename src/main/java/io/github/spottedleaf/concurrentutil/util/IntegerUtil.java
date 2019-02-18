package io.github.spottedleaf.concurrentutil.util;

public final class IntegerUtil {

    public static final int HIGH_BIT_U32 = Integer.MIN_VALUE;
    public static final long HIGH_BIT_U64 = Long.MIN_VALUE;

    public static int ceilLog2(final int value) {
        return 32 - Integer.numberOfLeadingZeros(value - 1);
    }

    public static long ceilLog2(final long value) {
        return 64 - Long.numberOfLeadingZeros(value - 1);
    }

    public static int floorLog2(final int value) {
        return 31 ^ Integer.numberOfLeadingZeros(value);
    }

    public static int floorLog2(final long value) {
        return 63 ^ Long.numberOfLeadingZeros(value);
    }

    public static int roundCeilLog2(final int value) {
        return HIGH_BIT_U32 >>> (Integer.numberOfLeadingZeros(value - 1) - 1);
    }

    public static long roundCeilLog2(final long value) {
        return HIGH_BIT_U64 >>> (Long.numberOfLeadingZeros(value - 1) - 1);
    }

    public static int roundFloorLog2(final int value) {
        return HIGH_BIT_U32 >>> Integer.numberOfLeadingZeros(value);
    }

    public static long roundFloorLog2(final long value) {
        return HIGH_BIT_U64 >>> Long.numberOfLeadingZeros(value);
    }

    public static boolean isPowerOfTwo(final int n) {
        return (-n & n) == n;
    }

    public static boolean isPowerOfTwo(final long n) {
        return (-n & n) == n;
    }

    private IntegerUtil() {
        throw new RuntimeException();
    }
}
