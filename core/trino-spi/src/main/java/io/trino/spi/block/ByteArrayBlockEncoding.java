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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static io.trino.spi.block.EncoderUtil.decodeNullBits;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBits;
import static io.trino.spi.block.EncoderUtil.retrieveNullBits;
import static java.lang.System.arraycopy;

public class ByteArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "BYTE_ARRAY";

    private static final byte STATIC = 1;
    private static final byte UPDATABLE = 2;

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        int positionCount = block.getPositionCount();
        if (block instanceof UpdatableByteArrayBlock) {
            UpdatableByteArrayBlock uBlock = (UpdatableByteArrayBlock) block;
            sliceOutput.writeByte(UPDATABLE);
            sliceOutput.appendInt(positionCount);
            sliceOutput.writeInt(uBlock.getNullCounter());
            sliceOutput.writeInt(uBlock.getDeleteCounter());
            // TODO: for further encoding we would need to write code that filters out the nulls and deleted columns
            sliceOutput.writeBytes(uBlock.getValueMarkerSlice());
            sliceOutput.writeBytes(getValuesSlice(block));
        }
        else {
            sliceOutput.writeByte(STATIC);
            sliceOutput.appendInt(positionCount);

            encodeNullsAsBits(sliceOutput, block);

            if (!block.mayHaveNull()) {
                sliceOutput.writeBytes(getValuesSlice(block));
            }
            else {
                byte[] valuesWithoutNull = new byte[positionCount];
                int nonNullPositionCount = 0;
                for (int i = 0; i < positionCount; i++) {
                    valuesWithoutNull[nonNullPositionCount] = block.getByte(i, 0);
                    if (!block.isNull(i)) {
                        nonNullPositionCount++;
                    }
                }

                sliceOutput.writeInt(nonNullPositionCount);
                sliceOutput.writeBytes(Slices.wrappedBuffer(valuesWithoutNull, 0, nonNullPositionCount));
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        byte type = sliceInput.readByte();
        int positionCount = sliceInput.readInt();
        if (type == STATIC) {
            byte[] valueIsNullPacked = retrieveNullBits(sliceInput, positionCount);
            byte[] values = new byte[positionCount];

            if (valueIsNullPacked == null) {
                sliceInput.readBytes(Slices.wrappedBuffer(values));
                return new ByteArrayBlock(0, positionCount, null, values);
            }
            boolean[] valueIsNull = decodeNullBits(valueIsNullPacked, positionCount);

            int nonNullPositionCount = sliceInput.readInt();
            sliceInput.readBytes(Slices.wrappedBuffer(values, 0, nonNullPositionCount));
            int position = nonNullPositionCount - 1;

            // Handle Last (positionCount % 8) values
            for (int i = positionCount - 1; i >= (positionCount & ~0b111) && position >= 0; i--) {
                values[i] = values[position];
                if (!valueIsNull[i]) {
                    position--;
                }
            }

            // Handle the remaining positions.
            for (int i = (positionCount & ~0b111) - 8; i >= 0 && position >= 0; i -= 8) {
                byte packed = valueIsNullPacked[i >>> 3];
                if (packed == 0) { // Only values
                    arraycopy(values, position - 7, values, i, 8);
                    position -= 8;
                }
                else if (packed != -1) { // At least one non-null
                    for (int j = i + 7; j >= i && position >= 0; j--) {
                        values[j] = values[position];
                        if (!valueIsNull[j]) {
                            position--;
                        }
                    }
                }
                // Do nothing if there are only nulls
            }
            return new ByteArrayBlock(0, positionCount, valueIsNull, values);
        }
        else {
            int nullCounter = sliceInput.readInt();
            int deleteCounter = sliceInput.readInt();
            byte[] valueMarker = new byte[positionCount];
            byte[] values = new byte[positionCount];
            sliceInput.readBytes(Slices.wrappedBuffer(valueMarker));
            sliceInput.readBytes(Slices.wrappedBuffer(values));
            return new UpdatableByteArrayBlock(null, positionCount, valueMarker, values, nullCounter, deleteCounter);
        }
    }

    private Slice getValuesSlice(Block block)
    {
        if (block instanceof ByteArrayBlock) {
            return ((ByteArrayBlock) block).getValuesSlice();
        }
        else if (block instanceof ByteArrayBlockBuilder) {
            return ((ByteArrayBlockBuilder) block).getValuesSlice();
        }
        else if (block instanceof UpdatableByteArrayBlock) {
            return ((UpdatableByteArrayBlock) block).getValuesSlice();
        }

        throw new IllegalArgumentException("Unexpected block type " + block.getClass().getSimpleName());
    }
}
