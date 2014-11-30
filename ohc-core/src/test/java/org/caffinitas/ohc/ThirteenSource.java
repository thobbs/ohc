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
package org.caffinitas.ohc;

public class ThirteenSource extends BytesSource.AbstractSource
{

    private final int len;

    public ThirteenSource(int len)
    {
        this.len = len;
    }

    public int size()
    {
        return len;
    }

    public byte getByte(int pos)
    {
        return (byte) (pos % 13);
    }

    public long hash()
    {
        return len;
    }
}
