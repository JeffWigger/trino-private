/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.spi.block;

public class UpdatableUtils
{
    public static byte NULL = 1;
    public static byte DEL = 2;

    static int getNullCount(boolean[] nulls)
    {
        if (nulls == null) {
            return 0;
        }
        int count = 0;
        for (boolean b : nulls) {
            if (b) {
                count++;
            }
        }
        return count;
    }

    static byte[] toBytes(boolean[] nulls)
    {
        if (nulls == null) {
            return null;
        }
        byte[] bytes = new byte[nulls.length];
        for (int i = 0; i < nulls.length; i++) {
            boolean b = nulls[i];
            if (b) {
                bytes[i] = 1;
            }
        }
        return bytes;
    }

    static boolean[] toBoolean(byte[] types)
    {
        boolean[] nulls = new boolean[types.length];
        for (int i = 0; i < nulls.length; i++) {
            boolean b = types[i] == NULL;
            nulls[i] = b;
        }
        return nulls;
    }
}
