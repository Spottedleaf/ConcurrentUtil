package ca.spottedleaf.concurrentutil.util;

public final class HashPrimeUtil {

    /** Prime numbers in (0, pow(2, 31)), differing by approximately a factor of 2 */
    private static final int[] PRIMES = new int[] {
            7, 17, 23, 53, 97, 193, 389, 769, 1543, 3079, 6151, 12289, 24593,
            49157, 98317, 196613, 393241, 786433, 1572869, 3145739, 6291469, 12582917,
            25165843, 50331653, 100663319, 201326611, 402653189, 805306457, 1610612741,
            2147483629
    };

    private static final int[] MULTIPLES = new int[HashPrimeUtil.PRIMES.length];

    private static final int[] SHIFTS = new int[HashPrimeUtil.PRIMES.length];

    static {
        for (int i = 0, len = HashPrimeUtil.PRIMES.length; i < len; ++i) {
            final int prime = HashPrimeUtil.PRIMES[i];
            final long numbers = IntegerUtil.getDivisorNumbers(prime);
            MULTIPLES[i] = IntegerUtil.getDivisorMultiple(numbers);
            SHIFTS[i] = IntegerUtil.getDivisorShift(numbers);
        }
    }

    public static int getPrime(final int primeId) {
        return HashPrimeUtil.PRIMES[primeId];
    }

    public static int getDivisorMultiplier(final int primeId) {
        return HashPrimeUtil.MULTIPLES[primeId];
    }

    public static int getDivisorShift(final int primeId) {
        return HashPrimeUtil.SHIFTS[primeId];
    }

    private HashPrimeUtil() {
        throw new RuntimeException();
    }
}