package io.github.spottedleaf.concurrentutil.set;

import io.github.spottedleaf.concurrentutil.ConcurrentUtil;
import io.github.spottedleaf.concurrentutil.util.ArrayUtil;

import java.lang.invoke.VarHandle;

public abstract class ConcurrentBitSet {

    public static ConcurrentBitSet of(final int bits) {
        if (bits <= 0) {
            throw new IllegalArgumentException("bit count must be > 0, not" + bits);
        }
        return bits <= 64 ? new SmallConcurrentBitSet(bits) : new LargeConcurrentBitset(bits);
    }

    // TODO add contended versions

    public abstract boolean get(final int bit);
    public abstract boolean setOn(final int bit); //ret prev
    public abstract boolean setOff(final int bit); //ret prev
    public abstract boolean flip(final int bit); //ret prev

    public abstract int totalBits();
    public abstract int bitsOn();

    public boolean set(final int bit, final boolean value) {
        if (value) {
            return this.setOn(bit);
        } else {
            return this.setOff(bit);
        }
    }

    public static class SmallConcurrentBitSet extends ConcurrentBitSet {

        protected final int maxBits;

        protected volatile long bitset;

        protected static final VarHandle BITSET_HANDLE = ConcurrentUtil.getVarHandle(SmallConcurrentBitSet.class, "bitset", long.class);

        protected final long getBitsetVolatile() {
            return (long)BITSET_HANDLE.getVolatile(this);
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

        public SmallConcurrentBitSet(final int maxBits, final long initState) {
            this(maxBits);
            this.setBitsetVolatile(initState);
        }

        protected final void checkBit(final int bit) {
            if (bit < 0 || bit >= this.maxBits) {
                throw new IllegalArgumentException("Bit out of range [0," + this.maxBits + "): " + bit);
            }
        }

        @Override
        public int totalBits() {
            return this.maxBits;
        }

        @Override
        public int bitsOn() {
            return Long.bitCount(this.getBitsetVolatile());
        }

        @Override
        public boolean get(final int bit) {
            this.checkBit(bit);

            final long curr = this.getBitsetVolatile();

            return (curr & (1L << bit)) != 0;
        }

        @Override
        public boolean setOn(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << bit;

            return (this.getAndBitwiseOrBitsetVolatile(bitfield) & bitfield) != 0;
        }

        @Override
        public boolean setOff(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << bit;

            return (this.getAndBitwiseAndBitseteVolatile(~(bitfield)) & bitfield) != 0;
        }

        @Override
        public boolean flip(final int bit) {
            final long bitfield = 1L << bit;
            final long prev = this.getAndBitwiseXorBitsetVolatile(bitfield);

            return (prev & bitfield) != 0;
        }
    }

    public static class LargeConcurrentBitset extends ConcurrentBitSet {

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

        @Override
        public int totalBits() {
            return this.maxBits;
        }

        @Override
        public int bitsOn() {
            int ret = 0;
            for (int i = 0, len = this.bitset.length; i < len; ++i) {
                ret += Long.bitCount(ArrayUtil.getVolatile(this.bitset, i));
            }
            return ret;
        }

        @Override
        public boolean get(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << (bit & (Long.SIZE - 1));
            final long bitset = this.bitset[bit >>> 6]; // bit / Long.SIZE

            return (bitset & bitfield) != 0;
        }

        @Override
        public boolean setOn(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << (bit & (Long.SIZE - 1));
            final int index = bit >>> 6; // bit / LONG.SIZE

            return (ArrayUtil.getAndOrVolatile(this.bitset, index, bitfield) & bitfield) != 0;
        }

        @Override
        public boolean setOff(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << (bit & (Long.SIZE - 1));
            final int index = bit >>> 6; // bit / LONG.SIZE

            return (ArrayUtil.getAndAndVolatile(this.bitset, index, ~bitfield) & bitfield) != 0;
        }

        @Override
        public boolean flip(final int bit) {
            this.checkBit(bit);

            final long bitfield = 1L << (bit & (Long.SIZE - 1));
            final int index = bit >>> 6; // bit / LONG.SIZE

            return (ArrayUtil.getAndXorVolatile(this.bitset, index, bitfield) & bitfield) != 0;
        }
    }

    public static class FastLargeConcurrentBitset extends ConcurrentBitSet {

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

        @Override
        public int totalBits() {
            return this.maxBits;
        }

        @Override
        public int bitsOn() {
            int ret = 0;
            for (int i = 0, len = this.maxBits; i < len; ++i) {
                ret += ArrayUtil.getVolatile(this.bitset, getIndexForBit(i)) ? 1 : 0;
            }
            return ret;
        }

        @Override
        public boolean set(final int bit, final boolean value) {
            this.checkBit(bit);

            return ArrayUtil.getAndSetVolatile(this.bitset, getIndexForBit(bit), value);
        }

        @Override
        public boolean setOn(final int bit) {
            return this.set(bit, true);
        }

        @Override
        public boolean setOff(final int bit) {
            return this.set(bit, false);
        }

        @Override
        public boolean flip(final int bit) {
            this.checkBit(bit);

            return ArrayUtil.getAndXorVolatile(this.bitset, getIndexForBit(bit), true);
        }

        @Override
        public boolean get(final int bit) {
            return ArrayUtil.getVolatile(this.bitset, getIndexForBit(bit));
        }
    }
}
