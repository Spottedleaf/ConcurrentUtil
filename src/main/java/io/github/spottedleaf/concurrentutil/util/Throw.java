package io.github.spottedleaf.concurrentutil.util;

public class Throw {

    public static <T extends Throwable> void rethrow(final T throwable) {
        Throw.<RuntimeException>throwImpl(throwable);
    }

    private static <T extends Throwable> void throwImpl(final Throwable throwable) throws T {
        //noinspection unchecked
        throw (T) throwable;
    }

}
