package io.github.spottedleaf.concurrentutil.integer;

import io.github.spottedleaf.concurrentutil.ConcurrentUtil;
import io.github.spottedleaf.concurrentutil.util.ArrayUtil;

import java.util.concurrent.ThreadLocalRandom;

public class IntAdder extends Number {

    protected final int[] cells;
    protected final int totalCells;

    protected static int getIndexFor(final int cellNumber) {
        return (cellNumber + 1) * ((2*ConcurrentUtil.CACHE_LINE_SIZE) / Integer.BYTES);
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
            sum += ArrayUtil.getOpaque(cells, getIndexFor(i));
        }

        sum += ArrayUtil.getAndAddVolatileContended(cells, getIndexFor(cellIndex), value);

        for (int i = cellIndex + 1; i < totalCells; ++i) {
            sum += ArrayUtil.getOpaque(cells, getIndexFor(i));
        }

        return sum;
    }

    public void addContended(final int value) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int cellIndex = random.nextInt(this.totalCells);
        final int[] cells = this.cells;

        ArrayUtil.getAndAddVolatileContended(cells, getIndexFor(cellIndex), value);
    }

    public int getAndAddUncontended(final int value) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int totalCells = this.totalCells;
        final int cellIndex = random.nextInt(totalCells);
        final int[] cells = this.cells;

        int sum = 0;

        for (int i = 0; i < cellIndex; ++i) {
            sum += ArrayUtil.getOpaque(cells, getIndexFor(i));
        }

        sum += ArrayUtil.getAndAddVolatile(cells, getIndexFor(cellIndex), value);

        for (int i = cellIndex + 1; i < totalCells; ++i) {
            sum += ArrayUtil.getOpaque(cells, getIndexFor(i));
        }

        return sum;
    }

    public void addUncontended(final int value) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int cell = random.nextInt(this.totalCells);

        ArrayUtil.getAndAddVolatile(this.cells, getIndexFor(cell), value);
    }

    public void addOpaque(final int value) {
        final int[] cells = this.cells;
        final int index = getIndexFor(0);

        ArrayUtil.setOpaque(cells, index, ArrayUtil.getPlain(cells, index) + value);
    }

    public int getAndAddOpaque(final int value) {
        final int totalCells = this.totalCells;
        final int[] cells = this.cells;
        final int index = getIndexFor(0);

        int sum;

        ArrayUtil.setOpaque(cells, index, sum = (ArrayUtil.getPlain(cells, index) + value));

        for (int i = 1; i < totalCells; ++i) {
            sum += ArrayUtil.getOpaque(cells, getIndexFor(i));
        }

        return sum;
    }

    public int sumOpaque() {
        int sum = 0;

        final int totalCells = this.totalCells;
        final int[] cells = this.cells;
        for (int i = 0; i < totalCells; ++i) {
            sum += ArrayUtil.getOpaque(cells, getIndexFor(i));
        }

        return sum;
    }

    public int sum() {
        int sum = 0;

        final int totalCells = this.totalCells;
        final int[] cells = this.cells;
        for (int i = 0; i < totalCells; ++i) {
            sum +=  ArrayUtil.getVolatile(cells, getIndexFor(i));
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