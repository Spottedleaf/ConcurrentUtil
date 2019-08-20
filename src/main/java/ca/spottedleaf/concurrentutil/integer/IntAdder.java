package ca.spottedleaf.concurrentutil.integer;

import ca.spottedleaf.concurrentutil.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.util.ArrayUtil;

import java.util.concurrent.ThreadLocalRandom;

public class IntAdder extends Number {

    protected final int[] cells;
    protected final int totalCells;

    protected static int getIndexFor(final int cellNumber) {
        return (cellNumber + 1) * (ConcurrentUtil.CACHE_LINE_SIZE / Integer.BYTES);
    }

    public IntAdder() {
        this(Runtime.getRuntime().availableProcessors(), 0);
    }

    public IntAdder(final int totalCells, final int initialValue) {
        if (totalCells <= 0) {
            throw new IllegalArgumentException("Invalid cells arg: " + totalCells);
        }

        final int[] cells = new int[getIndexFor(totalCells + 1)];

        ArrayUtil.setPlain(cells, getIndexFor(0), initialValue); // final guarantees publish

        this.totalCells = totalCells;
        this.cells = cells;
    }

    protected final int checkCell(final int cell) {
        if (cell < 0 || cell >= this.totalCells) {
            throw new IllegalArgumentException("Cell is out of bounds [0, " + this.totalCells + "): " + cell);
        }
        return cell;
    }

    public final int getTotalCells() {
        return this.totalCells;
    }

    public int getAndAddContended(final int value) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int[] cells = this.cells;
        final int totalCells = this.totalCells;
        final int cellIndex = random.nextInt(totalCells);

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
        final int[] cells = this.cells;
        final int totalCells = this.totalCells;

        int failures = 0;

        for (int cellIndex = random.nextInt(totalCells);; cellIndex = random.nextInt(totalCells)) {
            for (int i = 0; i < failures; ++i) {
                ConcurrentUtil.pause();
            }

            final int rawIndex = getIndexFor(cellIndex);
            final int curr = ArrayUtil.getVolatile(cells, rawIndex);
            if (curr == ArrayUtil.compareAndExchangeVolatile(cells, rawIndex, curr, curr + value)) {
                return;
            }
            ++failures;
        }
    }

    public void addContended(final int value, final int cell) {
        ArrayUtil.getAndAddVolatileContended(this.cells, getIndexFor(this.checkCell(cell)), value);
    }

    public int getAndAddUncontended(final int value) {
        return this.getAndAddUncontended(value, ThreadLocalRandom.current().nextInt(this.totalCells));
    }

    public int getAndAddUncontended(final int value, final int cell) {
        this.checkCell(cell);

        final int[] cells = this.cells;
        final int totalCells = this.totalCells;

        int sum = 0;

        for (int i = 0; i < cell; ++i) {
            sum += ArrayUtil.getOpaque(cells, getIndexFor(i));
        }

        sum += ArrayUtil.getAndAddVolatile(cells, getIndexFor(cell), value);

        for (int i = cell + 1; i < totalCells; ++i) {
            sum += ArrayUtil.getOpaque(cells, getIndexFor(i));
        }

        return sum;
    }

    public void addUncontended(final int value) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int cell = random.nextInt(this.totalCells);

        ArrayUtil.getAndAddVolatile(this.cells, getIndexFor(cell), value);
    }

    public void addUncontended(final int value, final int cell) {
        this.checkCell(cell);

        ArrayUtil.getAndAddVolatile(this.cells, getIndexFor(cell), value);
    }

    public void addOpaque(final int value) {
        this.addOpaque(value, 0);
    }

    public void addOpaque(final int value, final int cell) {
        final int[] cells = this.cells;
        final int rawIndex = getIndexFor(this.checkCell(cell));

        ArrayUtil.setOpaque(cells, rawIndex, ArrayUtil.getPlain(cells, rawIndex) + value);
    }

    public int getAndAddOpaque(final int value) {
        return this.getAndAddOpaque(value, 0);
    }

    public int getAndAddOpaque(final int value, final int cell) {
        this.checkCell(cell);

        final int[] cells = this.cells;
        final int totalCells = this.totalCells;
        final int rawIndex = getIndexFor(cell);

        int sum = 0;

        for (int i = 0; i < cell; ++i) {
            sum += ArrayUtil.getOpaque(cells, getIndexFor(i));
        }

        ArrayUtil.setOpaque(cells, rawIndex, sum += (ArrayUtil.getPlain(cells, rawIndex) + value));

        for (int i = cell + 1; i < totalCells; ++i) {
            sum += ArrayUtil.getOpaque(cells, getIndexFor(i));
        }

        return sum;
    }

    public int sumOpaque() {
        int sum = 0;

        final int[] cells = this.cells;
        for (int i = 0, totalCells = this.totalCells; i < totalCells; ++i) {
            sum += ArrayUtil.getOpaque(cells, getIndexFor(i));
        }

        return sum;
    }

    public long longSumOpaque() {
        long sum = 0;

        final int[] cells = this.cells;
        for (int i = 0, totalCells = this.totalCells; i < totalCells; ++i) {
            sum += ArrayUtil.getOpaque(cells, getIndexFor(i));
        }

        return sum;
    }

    public int sum() {
        int sum = 0;

        final int[] cells = this.cells;
        for (int i = 0, totalCells = this.totalCells; i < totalCells; ++i) {
            sum +=  ArrayUtil.getVolatile(cells, getIndexFor(i));
        }

        return sum;
    }

    public long longSum() {
        long sum = 0;

        final int[] cells = this.cells;
        for (int i = 0, totalCells = this.totalCells; i < totalCells; ++i) {
            sum += ArrayUtil.getVolatile(cells, getIndexFor(i));
        }

        return sum;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long longValue() {
        return this.longSum();
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