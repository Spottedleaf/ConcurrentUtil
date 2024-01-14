package ca.spottedleaf.concurrentutil.util;

public final class IntPairUtil {

    /**
     * Packs the specified integers into one long value.
     */
    public static long key(final int left, final int right) {
        return ((long)right << 32) | (left & 0xFFFFFFFFL);
    }

    /**
     * Retrieves the left packed integer from the key
     */
    public static int left(final long key) {
        return (int)key;
    }

    /**
     * Retrieves the right packed integer from the key
     */
    public static int right(final long key) {
        return (int)(key >>> 32);
    }

    private IntPairUtil() {}
}
