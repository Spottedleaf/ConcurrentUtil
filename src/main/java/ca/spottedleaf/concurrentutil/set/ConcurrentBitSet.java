package ca.spottedleaf.concurrentutil.set;

import ca.spottedleaf.concurrentutil.ConcurrentUtil;
import ca.spottedleaf.concurrentutil.util.ArrayUtil;

import java.lang.invoke.VarHandle;

public interface ConcurrentBitSet {

    /**
     * Constructs and returns a {@code ConcurrentBitSet} which is suitable for the number of bits specified.
     * @param bits The specified capacity of bits.
     * @return The {@code ConcurrentBitSet} with a capacity of the specified bits.
     */
    static ConcurrentBitSet of(final int bits) {
        if (bits <= 0) {
            throw new IllegalArgumentException("bit count must be > 0, not " + bits);
        }
        return bits <= Long.SIZE ? new SmallConcurrentBitSet(bits) : new LargeConcurrentBitset(bits);
    }

    // TODO Add contended versions
    // TODO Initial values/copying

    /**
     * Returns whether the specified bit is set to ON. The bit is read with volatile access.
     * <p>
     * This function is MT-Safe and is performed atomically.
     * </p>
     * @param bit The specified bit.
     * @return {@code true} if the bit is set, {@code false} otherwise.
     */
    boolean get(final int bit);

    /**
     * Sets the bit at the specified index to ON. The bit is written with volatile access.
     * <p>
     * This function is MT-Safe and is performed atomically.
     * </p>
     * @param bit The specified bit.
     */
    void setOn(final int bit);

    /**
     * Sets the bit at the specified index to OFF. The bit is written with volatile access.
     * <p>
     * This function is MT-Safe and is performed atomically.
     * </p>
     * @param bit The specified bit.
     */
    void setOff(final int bit);

    /**
     * Sets the specified bit to the specified value. The write to the specified bit is made with volatile access.
     * <p>
     * This function is MT-Safe and is performed atomically.
     * </p>
     * @param bit The specified bit.
     * @param on Whether to set the bit to ON.
     */
    void set(final int bit, final boolean on);

    /**
     * Sets the specified bit to ON and returns the previous state of the bit. The operation is made with volatile access.
     * <p>
     * This function is MT-Safe and is performed atomically.
     * </p>
     * @param bit The specified bit.
     * @return The previous value of the bit.
     */
    boolean getAndSetOn(final int bit);

    /**
     * Sets the specified bit to OFF and returns the previous state of the bit. The operation is made with volatile access.
     * <p>
     * This function is MT-Safe and is performed atomically.
     * </p>
     * @param bit The specified bit.
     * @return The previous value of the bit.
     */
    boolean getAndSetOff(final int bit);

    /**
     * Sets the specified bit to the specified value and returns the previous state of the bit. The operation is made with volatile access.
     * <p>
     * This function is MT-Safe and is performed atomically.
     * </p>
     * @param bit The specified bit.
     * @param on The specified value.
     * @return The previous value of the bit.
     */
    boolean getAndSet(final int bit, final boolean on);

    /**
     * Flips the specified bit. The write to the specified bit is made with volatile access, and the read is made with
     * volatile access. Both the read and write are performed atomically.
     * <p>
     * This function is MT-Safe.
     * </p>
     * @param bit The specified bit
     * @return The previous value of the specified bit
     */
    boolean flip(final int bit);

    /**
     * Returns the total number of bits stored in this bitset. This value cannot change through invocations.
     * @return The total number of bits stored in this bitset.
     */
    int totalBits();

    /**
     * Returns the total number of bits ON in this bitset. Each bit is read with volatile access.
     * <p>
     * This function is MT-Safe but not atomic. Concurrent changes may not be observed by this function.
     * </p>
     * @return Total number of ON bits.
     */
    int getOnBits();

    /**
     * {@link ConcurrentBitSet} implementation offering a maximum capacity of 64 bits. The bits are packed into a single
     * {@code long} field.
     * <p>
     * This implementation also modifies the specification of {@link ConcurrentBitSet#getOnBits()} to be atomic.
     * </p>
     * @see ConcurrentBitSet
     */
    class SmallConcurrentBitSet implements ConcurrentBitSet {

        protected final int maxBits;

        protected volatile long bitset;

        protected static final VarHandle BITSET_HANDLE = ConcurrentUtil.getVarHandle(SmallConcurrentBitSet.class, "bitset", long.class);

        protected final long getBitsetVolatile() {
            return (long)BITSET_HANDLE.getVolatile(this);
        }

        protected final void setBitsetRelease(final long value) {
            BITSET_HANDLE.setRelease(this, value);
        }

        protected final void setBitsetVolatile(final long value) {
            BITSET_HANDLE.setVolatile(this, value);
        }

        protected final long getAndBitwiseOrBitsetVolatile(final long value) {
            return (long)BITSET_HANDLE.getAndBitwiseOr(this, value);
        }

        protected final long getAndBitwiseAndBitseteVolatile(final long value) {
            return (long)BITSET_HANDLE.getAndBitwiseAnd(this, value);
        }

        protected final long getAndBitwiseXorBitsetVolatile(final long value) {
            return (long)BITSET_HANDLE.getAndBitwiseXor(this, value);
        }

        public SmallConcurrentBitSet(final int maxBits) {
            if (maxBits <= 0 || maxBits > 64) {
                throw new IllegalArgumentException("Max bits out of rage (0, 64]: " + maxBits);
            }
            this.maxBits = maxBits;
        }

        protected final void checkBit(final int bit) {
            if (bit < 0 || bit >= this.maxBits) {
                throw new IllegalArgumentException("Bit out of range [0," + this.maxBits + "): " + bit);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int totalBits() {
            return this.maxBits;
        }

        /**
         * Returns the total number of bits ON in this bitset. Each bit is read with volatile access.
         * <p>
         * This function is MT-Safe and atomic.
         * </p>
         * @return Total number of ON bits.
         */
        @Override
        public int getOnBits() {
            return Long.bitCount(this.getBitsetVolatile());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean get(final int bit) {
            this.checkBit(bit);

            final long curr = this.getBitsetVolatile();

            return (curr & (1L << bit)) != 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setOn(final int bit) {
            this.checkBit(bit);
            this.getAndBitwiseOrBitsetVolatile(1L << bit);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setOff(final int bit) {
            this.checkBit(bit);
            this.getAndBitwiseAndBitseteVolatile(~(1L << bit));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void set(final int bit, final boolean on) {
            if (on) {
                this.setOn(bit);
            } else {
                this.setOff(bit);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean getAndSetOn(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << bit;

            return (this.getAndBitwiseOrBitsetVolatile(bitfield) & bitfield) != 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean getAndSetOff(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << bit;

            return (this.getAndBitwiseAndBitseteVolatile(~bitfield) & bitfield) != 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean getAndSet(final int bit, final boolean on) {
            if (on) {
                return this.getAndSetOn(bit);
            } else {
                return this.getAndSetOff(bit);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean flip(final int bit) {
            final long bitfield = 1L << bit;
            final long prev = this.getAndBitwiseXorBitsetVolatile(bitfield);

            return (prev & bitfield) != 0;
        }
    }

    /**
     * {@link ConcurrentBitSet} implementation offering capacities up to {@link Integer#MAX_VALUE} bits. The bits are
     * packed into a {@code long} array.
     */
    class LargeConcurrentBitset implements ConcurrentBitSet {

        protected final int maxBits;

        protected final long[] bitset;

        public LargeConcurrentBitset(final int maxBits) {
            if (maxBits <= 0) {
                throw new IllegalArgumentException("Max bits must be positive");
            }
            final int size = maxBits >>> 6; // maxBits / Long.SIZE
            final int remainder = (~(maxBits & (Long.SIZE - 1))) >>> 31;
            this.bitset = new long[size + remainder];
            this.maxBits = maxBits;
        }

        protected final void checkBit(final int bit) {
            if (bit < 0 || bit >= this.maxBits) {
                throw new IllegalArgumentException("Bit out of range [0," + this.maxBits + "): " + bit);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int totalBits() {
            return this.maxBits;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getOnBits() {
            int ret = 0;
            for (int i = 0, len = this.bitset.length; i < len; ++i) {
                ret += Long.bitCount(ArrayUtil.getVolatile(this.bitset, i));
            }
            return ret;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean get(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << (bit & (Long.SIZE - 1));
            final long bitset = this.bitset[bit >>> 6]; // bit / Long.SIZE

            return (bitset & bitfield) != 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setOn(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << (bit & (Long.SIZE - 1));
            final int index = bit >>> 6; // bit / LONG.SIZE

            ArrayUtil.getAndOrVolatile(this.bitset, index, bitfield);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setOff(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << (bit & (Long.SIZE - 1));
            final int index = bit >>> 6; // bit / LONG.SIZE

            ArrayUtil.getAndAndVolatile(this.bitset, index, ~bitfield);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void set(final int bit, final boolean on) {
            if (on) {
                this.setOn(bit);
            } else {
                this.setOff(bit);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean getAndSetOn(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << (bit & (Long.SIZE - 1));
            final int index = bit >>> 6; // bit / LONG.SIZE

            return (ArrayUtil.getAndOrVolatile(this.bitset, index, bitfield) & bitfield) != 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean getAndSetOff(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << (bit & (Long.SIZE - 1));
            final int index = bit >>> 6; // bit / LONG.SIZE

            return (ArrayUtil.getAndAndVolatile(this.bitset, index, ~bitfield) & bitfield) != 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean getAndSet(final int bit, final boolean on) {
            if (on) {
                return this.getAndSetOn(bit);
            } else {
                return this.getAndSetOff(bit);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean flip(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << (bit & (Long.SIZE - 1));
            final int index = bit >>> 6; // bit / LONG.SIZE

            return (ArrayUtil.getAndXorVolatile(this.bitset, index, bitfield) & bitfield) != 0;
        }
    }

    /**
     * {@link ConcurrentBitSet} offering a cache-aware implementation for bitsets. Writes to one bit field will not
     * cause cache misses for reads/writes on other bit fields (aka, avoiding false sharing). However, this implementation
     * will use significantly more memory (expect at least two orders of magnitude) compared to {@link LargeConcurrentBitset}.
     */
    class FastLargeConcurrentBitset implements ConcurrentBitSet {

        protected final int maxBits;

        protected final boolean[] bitset;

        public FastLargeConcurrentBitset(final int bits) {
            if (bits <= 0) {
                throw new IllegalArgumentException("bit count must be > 0, not: " + bits);
            }

            // allocate a cache line at the start and end of the returned buffer
            this.bitset = new boolean[getIndexForBit(bits + 1)];
            this.maxBits = bits;
        }

        protected static int getIndexForBit(final int bit) {
            return ConcurrentUtil.CACHE_LINE_SIZE * (bit + 1);
        }

        protected final void checkBit(final int bit) {
            if (bit < 0 || bit >= this.maxBits) {
                throw new IllegalArgumentException("Bit out of range [0," + this.maxBits + "): " + bit);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int totalBits() {
            return this.maxBits;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getOnBits() {
            int ret = 0;
            for (int i = 0, len = this.maxBits; i < len; ++i) {
                ret += ArrayUtil.getVolatile(this.bitset, getIndexForBit(i)) ? 1 : 0;
            }
            return ret;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setOn(final int bit) {
            this.set(bit, true);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setOff(final int bit) {
            this.set(bit, false);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void set(final int bit, final boolean value) {
            this.checkBit(bit);

            ArrayUtil.setVolatile(this.bitset, getIndexForBit(bit), value);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean getAndSetOn(final int bit) {
            return this.getAndSet(bit, true);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean getAndSetOff(final int bit) {
            return this.getAndSet(bit, false);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean getAndSet(final int bit, final boolean on) {
            this.checkBit(bit);

            return ArrayUtil.getAndSetVolatile(this.bitset, getIndexForBit(bit), on);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean flip(final int bit) {
            this.checkBit(bit);

            return ArrayUtil.getAndXorVolatile(this.bitset, getIndexForBit(bit), true);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean get(final int bit) {
            return ArrayUtil.getVolatile(this.bitset, getIndexForBit(bit));
        }
    }
}
