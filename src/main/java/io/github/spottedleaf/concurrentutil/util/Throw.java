package io.github.spottedleaf.concurrentutil.util;

public final class Throw {

    /**
     * Silently rethrows the specified exception
     */
    @SuppressWarnings("unchecked")
    public static <T extends Throwable> void rethrow(final Throwable throwable) throws T {
        throw (T) throwable;
    }

    private Throw() {
        throw new RuntimeException();
    }

}
