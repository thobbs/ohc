/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.caffinitas.ohc.chunked;

import java.nio.ByteBuffer;

final class SerializationBufferProvider
{
    public static final int RESIZE_AMOUNT = 4096;
    private final ThreadLocal<ByteBuffer> serializationBuffer = new ThreadLocal<ByteBuffer>()
    {
        protected ByteBuffer initialValue()
        {
            return ByteBuffer.allocate(initialSize);
        }
    };
    private final int initialSize;

    private static int roundUpTo4K(int val)
    {
        int rem = val & 0xfff; // 4kB
        if (rem != 0)
            val += 4096L - rem;
        return val;
    }

    SerializationBufferProvider(int initialSize)
    {
        this.initialSize = roundUpTo4K(Math.min(RESIZE_AMOUNT, initialSize));
    }

    ByteBuffer get(int bytes)
    {
        ByteBuffer buffer = serializationBuffer.get();
        if (buffer.capacity() < bytes)
            serializationBuffer.set(buffer = ByteBuffer.allocate(roundUpTo4K(bytes)));
        buffer.clear();
        return buffer;
    }

    ByteBuffer resize()
    {
        ByteBuffer buffer = serializationBuffer.get();
        ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() + RESIZE_AMOUNT);
        serializationBuffer.set(newBuffer);
        return newBuffer;
    }
}
