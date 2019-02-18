package io.github.spottedleaf.concurrentutil;

import io.github.spottedleaf.concurrentutil.util.Throw;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public final class ConcurrentUtil {

    public static final int CACHE_LINE_SIZE = 64;

    /**
     * Closest possible implementation of the x86 PAUSE instruction.
     */
    public static void pause() {
        Thread.onSpinWait();
    }

    public static VarHandle getVarHandle(final Class<?> lookIn, final String fieldName, final Class<?> fieldType) {
        try {
            return MethodHandles.privateLookupIn(lookIn, MethodHandles.lookup()).findVarHandle(lookIn, fieldName, fieldType);
        } catch (final Exception ex) {
            Throw.rethrow(ex);
            throw new RuntimeException(ex); // unreachable
        }
    }

    public static VarHandle getStaticVarHandle(final Class<?> lookIn, final String fieldName, final Class<?> fieldType) {
        try {
            return MethodHandles.privateLookupIn(lookIn, MethodHandles.lookup()).findStaticVarHandle(lookIn, fieldName, fieldType);
        } catch (final Exception ex) {
            Throw.rethrow(ex);
            throw new RuntimeException(ex); // unreachable
        }
    }

    public static VarHandle getArrayHandle(final Class<?> type) {
        return MethodHandles.arrayElementVarHandle(type);
    }

    private ConcurrentUtil() {
        throw new RuntimeException();
    }
}