package io.github.spottedleaf.concurrentutil.integer;

import io.github.spottedleaf.concurrentutil.ConcurrentUtil;

import java.lang.invoke.VarHandle;
import java.util.concurrent.ThreadLocalRandom;

public class IntAdder extends Number {

    protected final int[] cells;
    protected final int totalCells;

    protected static final VarHandle INT_ARRAY_HANDLE = ConcurrentUtil.getArrayHandle(int[].class);

    protected static int getIndexFor(final int cellNumber) {
        return (cellNumber + 1) * (ConcurrentUtil.CACHE_LINE_SIZE / Integer.BYTES);
    }

    protected static void setPlain(final int[] array, final int index, final int value) {
        INT_ARRAY_HANDLE.set(array, index, value);
    }

    protected static void setOpaque(final int[] array, final int index, final int value) {
        INT_ARRAY_HANDLE.setOpaque(array, index, value);
    }

    protected static int getPlain(final int[] array, final int index) {
        return (int)INT_ARRAY_HANDLE.get(array, index);
    }

    protected static int getOpaque(final int[] array, final int index) {
        return (int)INT_ARRAY_HANDLE.getOpaque(array, index);
    }

    protected static int getVolatile(final int[] array, final int index) {
        return (int)INT_ARRAY_HANDLE.getVolatile(array, index);
    }

    protected static int getAndAddVolatile(final int[] array, final int index, final int value) {
        return (int)INT_ARRAY_HANDLE.getAndAdd(array, index, value);
    }

    protected static int compareAndExchangeVolatile(final int[] array, final int index, final int expect, final int update) {
        return (int)INT_ARRAY_HANDLE.compareAndExchange(array, index, expect, update);
    }

    public IntAdder() {
        this(Runtime.getRuntime().availableProcessors(), 0);
    }

    public IntAdder(final int totalCells, final int initialValue) {
        if (totalCells <= 0) {
            throw new IllegalArgumentException("Invalid cells arg: " + totalCells);
        }

        final int[] cells = new int[getIndexFor(totalCells + 1)];

        cells[getIndexFor(0)] = initialValue; // final guarantees publish

        this.totalCells = totalCells;
        this.cells = cells;
    }

    public int getAndAddContended(final int value) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int totalCells = this.totalCells;
        final int cellIndex = random.nextInt(totalCells);
        final int[] cells = this.cells;

        int sum = 0;

        for (int i = 0; i < cellIndex; ++i) {
            sum += getOpaque(cells, getIndexFor(i));
        }

        int failures = 0;
        for (int curr = getVolatile(cells, cellIndex);;) {
            final int next = curr + value;
            for (int i = 0; i < failures; ++i) {
                ConcurrentUtil.pause();
            }

            if (curr == (curr = compareAndExchangeVolatile(cells, cellIndex, curr, next))) {
                sum += curr;
                break;
            }
            ++failures;
        }

        for (int i = cellIndex + 1; i < totalCells; ++i) {
            sum += getOpaque(cells, getIndexFor(i));
        }

        return sum;
    }

    public void addContended(final int value) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int cellIndex = getIndexFor(random.nextInt(this.totalCells));
        final int[] cells = this.cells;

        int failures = 0;
        for (int curr = getVolatile(cells, cellIndex);;) {
            final int next = curr + value;
            for (int i = 0; i < failures; ++i) {
                ConcurrentUtil.pause();
            }

            if (curr == (curr = compareAndExchangeVolatile(cells, cellIndex, curr, next))) {
                return;
            }
            ++failures;
        }
    }

    public int getAndAddUncontended(final int value) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int totalCells = this.totalCells;
        final int cellIndex = random.nextInt(totalCells);
        final int[] cells = this.cells;

        int sum = 0;

        for (int i = 0; i < cellIndex; ++i) {
            sum += getOpaque(cells, getIndexFor(i));
        }

        sum += getAndAddVolatile(cells, getIndexFor(cellIndex), value);

        for (int i = cellIndex + 1; i < totalCells; ++i) {
            sum += getOpaque(cells, getIndexFor(i));
        }

        return sum;
    }

    public void addUncontended(final int value) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int cell = random.nextInt(this.totalCells);

        getAndAddVolatile(this.cells, getIndexFor(cell), value);
    }

    public void addOpaque(final int value) {
        final int[] cells = this.cells;
        final int index = getIndexFor(0);

        setOpaque(cells, index, getPlain(cells, index) + value);
    }

    public int getAndAddOpaque(final int value) {
        final int totalCells = this.totalCells;
        final int[] cells = this.cells;
        final int index = getIndexFor(0);

        int sum;

        setOpaque(cells, index, sum = (getPlain(cells, index) + value));

        for (int i = 1; i < totalCells; ++i) {
            sum += getOpaque(cells, getIndexFor(i));
        }

        return sum;
    }

    public int sumOpaque() {
        int sum = 0;

        final int totalCells = this.totalCells;
        final int[] cells = this.cells;
        for (int i = 0; i < totalCells; ++i) {
            sum += getOpaque(cells, getIndexFor(i));
        }

        return sum;
    }

    public int sum() {
        int sum = 0;

        final int totalCells = this.totalCells;
        final int[] cells = this.cells;
        for (int i = 0; i < totalCells; ++i) {
            sum += getVolatile(cells, getIndexFor(i));
        }

        return sum;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long longValue() {
        return (long)this.sum();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int intValue() {
        return this.sum();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short shortValue() {
        return (short)this.sum();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte byteValue() {
        return (byte)this.sum();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float floatValue() {
        return (float)this.sum();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double doubleValue() {
        return (double)this.sum();
    }
}