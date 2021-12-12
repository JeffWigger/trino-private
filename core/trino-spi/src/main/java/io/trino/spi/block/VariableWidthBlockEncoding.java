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

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.trino.spi.block.EncoderUtil.decodeNullBits;
import static io.trino.spi.block.EncoderUtil.encodeNullsAsBits;

public class VariableWidthBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "VARIABLE_WIDTH";

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
        if (block instanceof UpdatableVariableWidthBlock) {
            // The down casts here are safe because it is the block itself the provides this encoding implementation.
            UpdatableVariableWidthBlock variableWidthBlock = (UpdatableVariableWidthBlock) block;

            int positionCount = variableWidthBlock.getPositionCount();
            sliceOutput.writeByte(UPDATABLE);
            sliceOutput.appendInt(positionCount);
            // offsets
            int totalLength = 0;
            for (int position = 0; position < positionCount; position++) {
                int length = variableWidthBlock.getSliceLength(position);
                totalLength += length;
                sliceOutput.appendInt(totalLength);
            }
            sliceOutput.writeInt(variableWidthBlock.getNullCounter());
            sliceOutput.writeInt(variableWidthBlock.getDeleteCounter());
            sliceOutput.writeInt(totalLength);
            // TODO: for further encoding we would need to write code that filters out the nulls and deleted columns
            sliceOutput.writeBytes(variableWidthBlock.getValueMarkerSlice());
            sliceOutput.writeBytes(variableWidthBlock.getRawSlice(0), variableWidthBlock.getPositionOffset(0), totalLength);
        }
        else {
            // The down casts here are safe because it is the block itself the provides this encoding implementation.
            AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) block;

            sliceOutput.writeByte(STATIC);

            int positionCount = variableWidthBlock.getPositionCount();
            sliceOutput.appendInt(positionCount);

            // offsets
            int totalLength = 0;
            for (int position = 0; position < positionCount; position++) {
                int length = variableWidthBlock.getSliceLength(position);
                totalLength += length;
                sliceOutput.appendInt(totalLength);
            }

            encodeNullsAsBits(sliceOutput, variableWidthBlock);

            sliceOutput
                    .appendInt(totalLength)
                    .writeBytes(variableWidthBlock.getRawSlice(0), variableWidthBlock.getPositionOffset(0), totalLength);
        }
    }
    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {

        byte type = sliceInput.readByte();
        if (type == UPDATABLE) {
            int positionCount = sliceInput.readInt();

            int[] offsets = new int[positionCount + 1];
            sliceInput.readBytes(Slices.wrappedIntArray(offsets), SIZE_OF_INT, positionCount * SIZE_OF_INT);

            int nullCounter = sliceInput.readInt();
            int deleteCounter = sliceInput.readInt();

            int blockSize = sliceInput.readInt();
            byte[] valueMarker = new byte[positionCount];
            sliceInput.readBytes(Slices.wrappedBuffer(valueMarker));
            Slice slice = sliceInput.readSlice(blockSize);

            return new UpdatableVariableWidthBlock(null, positionCount, valueMarker, new VariableSliceOutput(slice, blockSize), nullCounter, deleteCounter, offsets);
        }
        else {
            int positionCount = sliceInput.readInt();

            int[] offsets = new int[positionCount + 1];
            sliceInput.readBytes(Slices.wrappedIntArray(offsets), SIZE_OF_INT, positionCount * SIZE_OF_INT);

            boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

            int blockSize = sliceInput.readInt();
            Slice slice = sliceInput.readSlice(blockSize);

            return new VariableWidthBlock(0, positionCount, slice, offsets, valueIsNull);
        }
    }
}
