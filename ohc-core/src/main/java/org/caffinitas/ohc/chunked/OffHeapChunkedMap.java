/*
 *      Copyright (C) 2014 Robert Stupp, Koeln, Germany, robert-stupp.de
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.caffinitas.ohc.chunked;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.AbstractIterator;

import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.OHCacheBuilder;
import org.caffinitas.ohc.histogram.EstimatedHistogram;

final class OffHeapChunkedMap
{
    // maximum hash table size
    private static final int TWO_POWER_30 = 1 << 30;

    private final int fixedKeySize;
    private final int fixedValueSize;

    private long size;

    private long hitCount;
    private long missCount;
    private long putAddCount;
    private long putReplaceCount;
    private long removeCount;

    private final float loadFactor;

    private long rehashes;
    private long evictedEntries;

    private final ReentrantLock lock = new ReentrantLock();

    private final boolean throwOOME;

    private Table table;

    private long threshold;

    private int capacity;
    private int freeCapacity;

    private final int chunkDataSize;
    private final int chunkFullSize;

    private final int chunkCount;

    private int chunksUsed;
    private int writeChunk;
    private int writeChunkOffset;
    private int writeChunkFree;

    private final ByteBuffer memory;

    OffHeapChunkedMap(OHCacheBuilder builder, long freeCapacity, int chunkSize)
    {
        this.throwOOME = builder.isThrowOOME();

        float lf = builder.getLoadFactor();
        if (lf <= .0d)
            lf = .75f;
        this.loadFactor = lf;

        if (freeCapacity > TWO_POWER_30)
            throw new IllegalArgumentException("Segment too big (max 2^30)");

        this.fixedKeySize = builder.getFixedKeySize();
        this.fixedValueSize = builder.getFixedValueSize();

        this.chunkDataSize = chunkSize;
        this.chunkFullSize = chunkSize + Util.CHUNK_OFF_DATA;
        this.chunkCount = (int) (freeCapacity / chunkSize);
        this.capacity = chunkCount * chunkSize;
        this.freeCapacity = this.capacity;
        int allocSize = chunkCount * chunkFullSize;
        memory = Uns.allocate(allocSize, throwOOME);

        for (int i = 0; i < chunkCount; i++)
            resetChunk(i);
        initWriteChunk(0);
        chunksUsed = 1;

        int hts = builder.getHashTableSize();
        if (hts <= 0)
            hts = 8192;
        if (hts < 256)
            hts = 256;
        table = createTable((int) Util.roundUpToPowerOf2(hts, TWO_POWER_30), throwOOME);
        if (table == null)
            throw new RuntimeException("unable to allocate off-heap memory for segment");

        threshold = (long) ((double) table.size() * loadFactor);
    }

    long size()
    {
        return size;
    }

    long hitCount()
    {
        return hitCount;
    }

    long missCount()
    {
        return missCount;
    }

    long putAddCount()
    {
        return putAddCount;
    }

    long putReplaceCount()
    {
        return putReplaceCount;
    }

    long removeCount()
    {
        return removeCount;
    }

    void resetStatistics()
    {
        rehashes = 0L;
        evictedEntries = 0L;
        hitCount = 0L;
        missCount = 0L;
        putAddCount = 0L;
        putReplaceCount = 0L;
        removeCount = 0L;
    }

    long rehashes()
    {
        return rehashes;
    }

    long evictedEntries()
    {
        return evictedEntries;
    }

    float loadFactor()
    {
        return loadFactor;
    }

    void release()
    {
        lock.lock();
        try
        {
            Uns.free(memory);

            table.release();
            table = null;
        }
        finally
        {
            lock.unlock();
        }
    }

    long freeCapacity()
    {
        return freeCapacity;
    }

    Object getEntry(KeyBuffer key, CacheSerializer<?> valueSerializer, SerializationBufferProvider serializationBufferProvider)
    {
        int hashEntryOffset;
        ByteBuffer serBuffer = null;

        lock.lock();
        try
        {
            for (hashEntryOffset = table.getFirst(key.hash());
                 hashEntryOffset != 0L;
                 hashEntryOffset = getNext(hashEntryOffset))
            {
                if (notSameKey(key, hashEntryOffset))
                    continue;

                // return existing entry

                hitCount++;

                if (valueSerializer == null)
                    return Boolean.TRUE;

                if (isEntryRemoved(hashEntryOffset))
                    return Boolean.FALSE;

                touch(hashEntryOffset);

                int keyLen = getKeyLen(hashEntryOffset);
                int valueLen = getValueLen(hashEntryOffset);
                int hashEntryValueOffset = hashEntryOffset + Util.entryOffData(isFixedSize()) + keyLen;
                serBuffer = serializationBufferProvider.get(valueLen);
                memory.position(hashEntryValueOffset);
                memory.limit(hashEntryValueOffset + valueLen);
                serBuffer.put(memory);
                serBuffer.flip();
                memory.clear();

                break;
            }

            if (hashEntryOffset == 0)
            {
                // not found
                missCount++;
                return null;
            }
        }
        finally
        {
            lock.unlock();
        }

        return valueSerializer.deserialize(serBuffer);
    }

    boolean putEntry(ByteBuffer newHashEntry, long hash, int keyLen, int entryBytes, boolean ifAbsent, int oldValueLen)
    {
        lock.lock();
        try
        {
            int hashEntryOffset;
            int prevEntryOffset = 0;
            for (hashEntryOffset = table.getFirst(hash);
                 hashEntryOffset != 0L;
                 prevEntryOffset = hashEntryOffset, hashEntryOffset = getNext(hashEntryOffset))
            {
                if (notSameKey(newHashEntry, hash, keyLen, hashEntryOffset))
                    continue;

                // replace existing entry

                if (ifAbsent)
                    return false;

                int valueOffset = Util.entryOffData(isFixedSize()) + keyLen;
                int hashEntryValueOffset = hashEntryOffset + valueOffset;
                if (oldValueLen != 0)
                {
                    // code for replace() operation
                    int valueLen = getValueLen(hashEntryOffset);
                    if (valueLen != oldValueLen ||
                        !compare(hashEntryValueOffset, newHashEntry, entryBytes, oldValueLen))
                        return false;
                }

                if (getValueReservedLen(hashEntryOffset) >= getValueLen(newHashEntry))
                {
                    // just overwrite the old value if it fits

                    if (!isFixedSize())
                        setValueLen(hashEntryOffset, getValueLen(newHashEntry));
                    memory.position(hashEntryValueOffset);
                    newHashEntry.position(valueOffset);
                    newHashEntry.limit(entryBytes);
                    memory.put(newHashEntry);

                    putReplaceCount++;

                    return true;
                }

                removeInternal(hashEntryOffset, prevEntryOffset);

                break;
            }

            if (writeChunkFree < entryBytes)
            {
                if (chunksUsed >= chunkCount)
                {
                    // find oldest chunk
                    long minTS = Long.MAX_VALUE;
                    int eldestChunk = 0;
                    for (int i = 0; i < chunkCount; i++)
                    {
                        long ts = lastUsed(i);
                        if (ts < minTS)
                        {
                            eldestChunk = i;
                            minTS = ts;
                        }
                    }

                    int entries = entriesInChunk(eldestChunk);
                    for (int nextOff, i = 0, off = chunkOffset(eldestChunk) + Util.CHUNK_OFF_DATA;
                         i < entries; i++, off = nextOff)
                    {
                        nextOff = nextHashEntryOffset(off);
                        if (!isEntryRemoved(off))
                        {
                            // removed elements have a value length of -1
                            removeInternal(off, -1);
                        }
                    }

                    // record statistics
                    evictedEntries += entries;
                    size += entries;
                    freeCapacity += bytesInChunk(eldestChunk);
                    initWriteChunk(eldestChunk);
                }
                else
                {
                    // Initially not all chunks have been used.
                    // So use all "virgin" chunks first before starting eviction.
                    initWriteChunk(writeChunk + 1);
                    chunksUsed++;
                }
            }

            if (hashEntryOffset == 0L)
            {
                if (size >= threshold)
                    rehash();

                size++;
            }

            if (hashEntryOffset == 0L)
                putAddCount++;
            else
                putReplaceCount++;

            // just overwrite the old value if it fits
            hashEntryOffset = chunkOffset(writeChunk) + writeChunkOffset;
            memory.position(hashEntryOffset);
            newHashEntry.limit(entryBytes);
            memory.put(newHashEntry);

            writeChunkOffset += entryBytes;
            writeChunkFree -= entryBytes;

            freeCapacity -= entryBytes;

            entryAdded(writeChunk, entryBytes);

            table.addAsHead(hash, hashEntryOffset);

            return true;
        }
        finally
        {
            lock.unlock();
        }
    }

    void clear()
    {
        lock.lock();
        try
        {
            size = 0L;
            freeCapacity = capacity;

            table.clear();

            initWriteChunk(0);
            chunksUsed = 1;
        }
        finally
        {
            lock.unlock();
        }
    }

    private void initWriteChunk(int newWriteChunk)
    {
        writeChunk = newWriteChunk;
        writeChunkFree = chunkDataSize;
        writeChunkOffset = Util.CHUNK_OFF_DATA;
        resetChunk(newWriteChunk);
    }

    void removeEntry(KeyBuffer key)
    {
        lock.lock();
        try
        {
            int prevEntryOffset = 0;
            for (int hashEntryOffset = table.getFirst(key.hash());
                 hashEntryOffset != 0;
                 prevEntryOffset = hashEntryOffset, hashEntryOffset = getNext(hashEntryOffset))
            {
                if (notSameKey(key, hashEntryOffset))
                    continue;

                if (isEntryRemoved(hashEntryOffset))
                    return;

                // remove existing entry

                removeInternal(hashEntryOffset, prevEntryOffset);

                size--;
                removeCount++;

                return;
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    private boolean notSameKey(KeyBuffer key, int hashEntryOffset)
    {
        if (getHash(hashEntryOffset) != key.hash()) return true;

        int serKeyLen = getKeyLen(hashEntryOffset);
        return serKeyLen != key.size()
               || !compareKey(hashEntryOffset, key, serKeyLen);
    }

    private boolean notSameKey(ByteBuffer newHashEntry, long newHash, int newKeyLen, int hashEntryOffset)
    {
        if (getHash(newHashEntry) != newHash) return true;

        int serKeyLen = getKeyLen(hashEntryOffset);
        return serKeyLen != newKeyLen
               || !compare(hashEntryOffset + Util.entryOffData(isFixedSize()),
                           newHashEntry, Util.entryOffData(isFixedSize()), serKeyLen);
    }

    private void rehash()
    {
        Table tab = table;
        int tableSize = tab.size();
        if (tableSize > TWO_POWER_30)
        {
            // already at max hash table size
            return;
        }

        Table newTable = createTable(tableSize * 2, throwOOME);
        if (newTable == null)
            return;
        int next;

        for (int part = 0; part < tableSize; part++)
            for (int hashEntryOffset = tab.getFirst(part);
                 hashEntryOffset != 0L;
                 hashEntryOffset = next)
            {
                next = getNext(hashEntryOffset);

                setNext(hashEntryOffset, 0);

                newTable.addAsHead(getHash(hashEntryOffset), hashEntryOffset);
            }

        threshold = (long) ((float) newTable.size() * loadFactor);
        table.release();
        table = newTable;
        rehashes++;
    }

    int hashTableSize()
    {
        return table.size();
    }

    void updateBucketHistogram(EstimatedHistogram hist)
    {
        lock.lock();
        try
        {
            table.updateBucketHistogram(hist);
        }
        finally
        {
            lock.unlock();
        }
    }

    Table createTable(int hashTableSize, boolean throwOOME)
    {
        int msz = Table.BUCKET_ENTRY_LEN * hashTableSize;
        ByteBuffer table = Uns.allocate(msz, throwOOME);
        return table == null ? null : new Table(table, hashTableSize);
    }

    final class Table
    {
        final int mask;
        final ByteBuffer table;
        private boolean released;

        static final int BUCKET_ENTRY_LEN = 4;

        private Table(ByteBuffer table, int hashTableSize)
        {
            this.table = table;
            this.mask = hashTableSize - 1;
            clear();
        }

        void clear()
        {
            // It's important to initialize the hash table memory.
            // (uninitialized memory will cause problems - endless loops, JVM crashes, damaged data, etc)
            table.clear();
            while (table.remaining() > 8)
                table.putLong(0L);
            while (table.remaining() > 0)
                table.put((byte) 0);
        }

        void release()
        {
            Uns.free(table);
            released = true;
        }

        protected void finalize() throws Throwable
        {
            if (!released)
                Uns.free(table);
            super.finalize();
        }

        int getFirst(long hash)
        {
            return table.getInt(bucketOffset(hash));
        }

        void setFirst(long hash, int hashEntryOffset)
        {
            table.putInt(bucketOffset(hash), hashEntryOffset);
        }

        private int bucketOffset(long hash)
        {
            return bucketIndexForHash(hash) * BUCKET_ENTRY_LEN;
        }

        private int bucketIndexForHash(long hash)
        {
            return (int) (hash & mask);
        }

        void removeLink(long hash, int hashEntryOffset, int prevEntryOffset)
        {
            int next = getNext(hashEntryOffset);

            int head = getFirst(hash);
            if (head == hashEntryOffset)
            {
                setFirst(hash, next);
            }
            else if (prevEntryOffset != 0)
            {
                if (prevEntryOffset == -1)
                {
                    for (int offset = head;
                         offset != 0L;
                         prevEntryOffset = offset, offset = getNext(offset))
                    {
                        if (offset == hashEntryOffset)
                            break;
                    }
                }
                setNext(prevEntryOffset, next);
            }
        }

        void addAsHead(long hash, int hashEntryOffset)
        {
            int head = getFirst(hash);
            setNext(hashEntryOffset, head);
            setFirst(hash, hashEntryOffset);
        }

        int size()
        {
            return mask + 1;
        }

        void updateBucketHistogram(EstimatedHistogram h)
        {
            for (int i = 0; i < size(); i++)
            {
                int len = 0;
                for (int off = getFirst(i); off != 0L; off = getNext(off))
                    len++;
                h.add(len + 1);
            }
        }
    }

    private void removeInternal(int hashEntryOffset, int prevEntryOffset)
    {
        long hash = getHash(hashEntryOffset);

        table.removeLink(hash, hashEntryOffset, prevEntryOffset);

        setEntryRemoved(hashEntryOffset);
    }

    @Override
    public String toString()
    {
        return String.valueOf(size);
    }

    //
    // snapshot iterator
    //

    private final class ChunkSnapshotIterator extends AbstractIterator<ByteBuffer>
    {
        private final int entries;
        private final int limit;
        private final ByteBuffer snaphotBuffer;
        private int n;
        private int pos;

        ChunkSnapshotIterator(int limit, ByteBuffer snaphotBuffer)
        {
            this.snaphotBuffer = snaphotBuffer;
            this.limit = limit;
            this.entries = snaphotBuffer.getInt(Util.CHUNK_OFF_ENTRIES);
            this.pos = Util.CHUNK_OFF_DATA;
        }

        protected ByteBuffer computeNext()
        {
            while (true)
            {
                if (n == entries || n == limit)
                    return endOfData();

                snaphotBuffer.clear();
                int keyLen;
                int valueLen;
                int reservedLen;
                if (isFixedSize())
                {
                    keyLen = fixedKeySize;
                    valueLen = reservedLen = fixedValueSize;
                }
                else
                {
                    keyLen = snaphotBuffer.getInt(pos + Util.ENTRY_OFF_KEY_LENGTH);
                    valueLen = snaphotBuffer.getInt(pos + Util.ENTRY_OFF_VALUE_LENGTH);
                    reservedLen = snaphotBuffer.getInt(pos + Util.ENTRY_OFF_RESERVED_LENGTH);
                }

                int keyOff = pos + Util.entryOffData(isFixedSize());

                pos += Util.allocLen(keyLen, reservedLen, isFixedSize());

                // skip removed entries
                if (valueLen != -1)
                {
                    n++;

                    snaphotBuffer.position(keyOff);
                    snaphotBuffer.limit(keyOff + keyLen);
                    return snaphotBuffer;
                }
            }
        }
    }

    Iterator<ByteBuffer> snapshotIterator(final int keysPerChunk, final ByteBuffer snaphotBuffer)
    {
        return new AbstractIterator<ByteBuffer>()
        {
            private int chunk;

            private Iterator<ByteBuffer> snapshotIterator;

            protected ByteBuffer computeNext()
            {
                while (true)
                {
                    if (snapshotIterator != null && snapshotIterator.hasNext())
                        return snapshotIterator.next();

                    if (chunk == chunkCount)
                        return endOfData();

                    lock.lock();
                    try
                    {
                        memory.position(chunkOffset(chunk));
                        memory.limit(memory.position() + chunkFullSize);
                        snaphotBuffer.clear();
                        snaphotBuffer.put(memory);
                        memory.clear();

                        snapshotIterator = new ChunkSnapshotIterator(keysPerChunk, snaphotBuffer);
                    }
                    finally
                    {
                        lock.unlock();
                        chunk++;
                    }
                }
            }
        };
    }

    //
    // memory access utils
    //

    private long getHash(int hashEntryOffset)
    {
        return memory.getLong(hashEntryOffset + Util.ENTRY_OFF_HASH);
    }

    private long getHash(ByteBuffer hashEntry)
    {
        return hashEntry.getLong(Util.ENTRY_OFF_HASH);
    }

    private int getNext(int hashEntryOffset)
    {
        return hashEntryOffset != 0 ? memory.getInt(hashEntryOffset + Util.ENTRY_OFF_NEXT) : 0;
    }

    private void setNext(int hashEntryOffset, int nextEntryOffset)
    {
        memory.putInt(hashEntryOffset + Util.ENTRY_OFF_NEXT, nextEntryOffset);
    }

    private boolean isEntryRemoved(int hashEntryOffset)
    {
        return memory.getInt(hashEntryOffset + (isFixedSize() ? Util.ENTRY_OFF_REMOVED : Util.ENTRY_OFF_VALUE_LENGTH)) == -1;
    }

    private void setEntryRemoved(int hashEntryOffset)
    {
        memory.putInt(hashEntryOffset + (isFixedSize() ? Util.ENTRY_OFF_REMOVED : Util.ENTRY_OFF_VALUE_LENGTH), -1);
    }

    private int getKeyLen(int hashEntryOffset)
    {
        if (fixedKeySize > 0)
            return fixedKeySize;
        return memory.getInt(hashEntryOffset + Util.ENTRY_OFF_KEY_LENGTH);
    }

    private int getValueLen(int hashEntryOffset)
    {
        if (fixedKeySize > 0)
            return fixedValueSize;
        return memory.getInt(hashEntryOffset + Util.ENTRY_OFF_VALUE_LENGTH);
    }

    private void setValueLen(int hashEntryOffset, int valueLen)
    {
        if (fixedKeySize == 0)
            memory.putInt(hashEntryOffset + Util.ENTRY_OFF_VALUE_LENGTH, valueLen);
    }

    private int getValueLen(ByteBuffer hashEntry)
    {
        if (fixedValueSize > 0)
            return fixedValueSize;
        return hashEntry.getInt(Util.ENTRY_OFF_VALUE_LENGTH);
    }

    private int getValueReservedLen(int hashEntryOffset)
    {
        if (fixedValueSize > 0)
            return fixedValueSize;
        return memory.getInt(hashEntryOffset + Util.ENTRY_OFF_RESERVED_LENGTH);
    }

    private boolean compare(int memoryOffset, ByteBuffer otherHashEntry, int otherOffset, int len)
    {
        int p = 0;
        for (; p <= len - 8; p += 8, memoryOffset += 8, otherOffset += 8)
            if (memory.getLong(memoryOffset) != otherHashEntry.getLong(otherOffset))
                return false;
        for (; p <= len - 4; p += 4, memoryOffset += 4, otherOffset += 4)
            if (memory.getInt(memoryOffset) != otherHashEntry.getInt(otherOffset))
                return false;
        for (; p <= len - 2; p += 2, memoryOffset += 2, otherOffset += 2)
            if (memory.getShort(memoryOffset) != otherHashEntry.getShort(otherOffset))
                return false;
        for (; p < len; p++, memoryOffset++, otherOffset++)
            if (memory.get(memoryOffset) != otherHashEntry.get(otherOffset))
                return false;

        return true;
    }

    private boolean compareKey(int hashEntryOffset, KeyBuffer key, long serKeyLen)
    {
        int blkOff = Util.entryOffData(isFixedSize());
        int p = 0;
        ByteBuffer buf = key.buffer();
        for (; p <= serKeyLen - 8; p += 8, blkOff += 8)
            if (memory.getLong(hashEntryOffset + blkOff) != buf.getLong(p))
                return false;
        for (; p <= serKeyLen - 4; p += 4, blkOff += 4)
            if (memory.getInt(hashEntryOffset + blkOff) != buf.getInt(p))
                return false;
        for (; p <= serKeyLen - 2; p += 2, blkOff += 2)
            if (memory.getShort(hashEntryOffset + blkOff) != buf.getShort(p))
                return false;
        for (; p < serKeyLen; p++, blkOff++)
            if (memory.get(hashEntryOffset + blkOff) != buf.get(p))
                return false;

        return true;
    }

    private int chunkOffset(int chunkNum)
    {
        return chunkNum * chunkFullSize;
    }

    private void resetChunk(int chunkNum)
    {
        int offset = chunkOffset(chunkNum);
        memory.putLong(offset + Util.CHUNK_OFF_TIMESTAMP, System.nanoTime());
        memory.putInt(offset + Util.CHUNK_OFF_ENTRIES, 0);
        memory.putInt(offset + Util.CHUNK_OFF_BYTES, 0);
    }

    private void touch(int hashEntryOffset)
    {
        touchChunk(chunkNum(hashEntryOffset));
    }

    private int chunkNum(int hashEntryOffset)
    {
        return hashEntryOffset / chunkFullSize;
    }

    private void touchChunk(int chunkNum)
    {
        int offset = chunkOffset(chunkNum);
        memory.putLong(offset + Util.CHUNK_OFF_TIMESTAMP, System.nanoTime());
    }

    private long lastUsed(int chunkNum)
    {
        int offset = chunkOffset(chunkNum);
        return memory.getLong(offset + Util.CHUNK_OFF_TIMESTAMP);
    }

    private void entryAdded(int chunkNum, int bytes)
    {
        int offset = chunkOffset(chunkNum);
        memory.putInt(offset + Util.CHUNK_OFF_ENTRIES, memory.getInt(offset + Util.CHUNK_OFF_ENTRIES) + 1);
        memory.putInt(offset + Util.CHUNK_OFF_BYTES, memory.getInt(offset + Util.CHUNK_OFF_BYTES) + bytes);
    }

    private int entriesInChunk(int chunkNum)
    {
        int offset = chunkOffset(chunkNum);
        return memory.getInt(offset + Util.CHUNK_OFF_ENTRIES);
    }

    private int bytesInChunk(int chunkNum)
    {
        int offset = chunkOffset(chunkNum);
        return memory.getInt(offset + Util.CHUNK_OFF_BYTES);
    }

    private int nextHashEntryOffset(int off)
    {
        int allocLen = Util.allocLen(getKeyLen(off), getValueReservedLen(off), isFixedSize());
        return off + allocLen;
    }

    private boolean isFixedSize()
    {
        return fixedKeySize > 0;
    }
}
