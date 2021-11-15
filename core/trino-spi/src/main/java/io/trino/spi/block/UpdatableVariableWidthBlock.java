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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.BlockUtil.MAX_ARRAY_SIZE;
import static io.trino.spi.block.BlockUtil.calculateBlockResetBytes;
import static io.trino.spi.block.BlockUtil.calculateBlockResetSize;
import static io.trino.spi.block.BlockUtil.checkArrayRange;
import static io.trino.spi.block.BlockUtil.checkValidPosition;
import static io.trino.spi.block.BlockUtil.checkValidPositions;
import static io.trino.spi.block.BlockUtil.checkValidRegion;
import static io.trino.spi.block.BlockUtil.compactArray;
import static io.trino.spi.block.BlockUtil.compactOffsets;
import static io.trino.spi.block.BlockUtil.compactSlice;
import static io.trino.spi.block.UpdatableUtils.toBoolean;
import static java.lang.Math.min;

public class UpdatableVariableWidthBlock
        extends AbstractVariableWidthBlock
        implements UpdatableBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(UpdatableVariableWidthBlock.class).instanceSize();

    @Nullable
    private final BlockBuilderStatus blockBuilderStatus;
    private boolean initialized;
    private final int initialEntryCount;
    private int initialSliceOutputSize;

    private VariableSliceOutput sliceOutput = new VariableSliceOutput(0);
    private int nullCounter = 0;
    private int deleteCounter = 0;


    // it is assumed that the offsets array is one position longer than the valueIsNull array
    private byte[] valueMarker = new byte[0];
    private int[] offsets = new int[1]; //TODO

    private int positions;
    private int currentEntrySize;

    private long arraysRetainedSizeInBytes;

    public static byte NULL = 1;
    public static byte DEL = 2;

    public UpdatableVariableWidthBlock(@Nullable BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytes)
    {
        this.blockBuilderStatus = blockBuilderStatus;

        initialEntryCount = expectedEntries;
        initialSliceOutputSize = min(expectedBytes, MAX_ARRAY_SIZE);

        updateArraysDataSize();
    }

    public UpdatableVariableWidthBlock(@Nullable BlockBuilderStatus blockBuilderStatus, int positionCount, boolean[] valueMarker, VariableSliceOutput sliceOutput, int[] offsets){
        this(blockBuilderStatus, positionCount, UpdatableUtils.toBytes(valueMarker), sliceOutput, UpdatableUtils.getNullCount(valueMarker), 0, offsets);
    }

    public UpdatableVariableWidthBlock(@Nullable BlockBuilderStatus blockBuilderStatus, int positionCount, byte[] valueMarker, VariableSliceOutput sliceOutput, int nullCounter, int deleteCounter, int[] offsets)
    {
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positions = positionCount;

        // it is assumed that nullCounter matches the number of nulls in valueMarker
        if (nullCounter > positionCount || nullCounter < 0) {
            throw new IllegalArgumentException("nullCounter is not valid");
        }
        this.nullCounter = nullCounter;

        // it is assumed that deleteCounter matches the number of nulls in valueMarker
        if (nullCounter > positionCount || nullCounter < 0) {
            throw new IllegalArgumentException("deleteCounter is not valid");
        }
        this.deleteCounter = deleteCounter;

        this.sliceOutput = sliceOutput;

        this.valueMarker = Objects.requireNonNullElseGet(valueMarker, () -> new byte[positionCount]);

        this.offsets = offsets;

        updateArraysDataSize();

        this.blockBuilderStatus = blockBuilderStatus;

        this.initialEntryCount = this.valueMarker.length;
        this.initialSliceOutputSize = sliceOutput.size();
    }

    public int getNullCounter()
    {
        return nullCounter;
    }

    public int getDeleteCounter()
    {
        return deleteCounter;
    }

    @Override
    protected int getPositionOffset(int position)
    {
        checkValidPosition(position, positions);
        return getOffset(position);
    }

    @Override
    public int getSliceLength(int position)
    {
        checkValidPosition(position, positions);
        return getOffset((position + 1)) - getOffset(position);
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        return sliceOutput.getUnderlyingSlice();
    }

    @Override
    public int getPositionCount()
    {
        return positions;
    }

    @Override
    public long getSizeInBytes()
    {
        long arraysSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) positions;
        return sliceOutput.size() + arraysSizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int positionOffset, int length)
    {
        // TODO: should this count the deleted ones?
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, positionOffset, length);
        long arraysSizeInBytes = (Integer.BYTES + Byte.BYTES) * (long) length;
        return getOffset(positionOffset + length) - getOffset(positionOffset) + arraysSizeInBytes;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        // TODO: should this count the deleted ones?
        checkValidPositions(positions, getPositionCount());
        long sizeInBytes = 0;
        int usedPositionCount = 0;
        for (int i = 0; i < positions.length; ++i) {
            if (positions[i]) {
                usedPositionCount++;
                sizeInBytes += getOffset(i + 1) - getOffset(i);
            }
        }
        return sizeInBytes + (Integer.BYTES + Byte.BYTES) * (long) usedPositionCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long size = INSTANCE_SIZE + sliceOutput.getRetainedSize() + arraysRetainedSizeInBytes;
        if (blockBuilderStatus != null) {
            size += BlockBuilderStatus.INSTANCE_SIZE;
        }
        return size;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(sliceOutput, sliceOutput.getRetainedSize());
        consumer.accept(offsets, sizeOf(offsets));
        consumer.accept(valueMarker, sizeOf(valueMarker));
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int finalLength = 0;
        int newlength = 0;
        for (int i = offset; i < offset + length; i++) {
            if (valueMarker[i] == DEL){
                continue;
            }
            finalLength += getSliceLength(positions[i]);
            length++;
        }
        SliceOutput newSlice = Slices.allocate(finalLength).getOutput();
        int[] newOffsets = new int[newlength + 1];
        boolean[] newValueMarker = null;
        newValueMarker = new boolean[length];

        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (valueMarker[position] == DEL){
                continue;
            } else if (valueMarker[position] == NULL) {
                newValueMarker[i] = true;
            }
            else {
                newSlice.writeBytes(sliceOutput.getUnderlyingSlice(), getPositionOffset(position), getSliceLength(position));
            }
            newOffsets[i + 1] = newSlice.size();
        }
        return new VariableWidthBlock(0, length, newSlice.slice(), newOffsets, newValueMarker);
    }

    @Override
    public UpdatableBlock writeByte(int value)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeByte(value);
        currentEntrySize += SIZE_OF_BYTE;
        return this;
    }

    @Override
    public UpdatableBlock writeShort(int value)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeShort(value);
        currentEntrySize += SIZE_OF_SHORT;
        return this;
    }

    @Override
    public UpdatableBlock writeInt(int value)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeInt(value);
        currentEntrySize += SIZE_OF_INT;
        return this;
    }

    @Override
    public UpdatableBlock writeLong(long value)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeLong(value);
        currentEntrySize += SIZE_OF_LONG;
        return this;
    }

    @Override
    public UpdatableBlock writeBytes(Slice source, int sourceIndex, int length)
    {
        if (!initialized) {
            initializeCapacity();
        }
        sliceOutput.writeBytes(source, sourceIndex, length);
        currentEntrySize += length;
        return this;
    }

    @Override
    public UpdatableBlock closeEntry()
    {
        entryAdded(currentEntrySize, false);
        currentEntrySize = 0;
        return this;
    }

    @Override
    public UpdatableBlock appendNull()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before a null can be written");
        }

        nullCounter++;
        entryAdded(0, true);
        return this;
    }

    private void entryAdded(int bytesWritten, boolean isNull)
    {
        if (!initialized) {
            initializeCapacity();
        }
        if (valueMarker.length <= positions) {
            growCapacity();
        }
        if (isNull) {
            valueMarker[positions] = NULL;
        }
        offsets[positions + 1] = sliceOutput.size();

        positions++;

        if (blockBuilderStatus != null) {
            blockBuilderStatus.addBytes(SIZE_OF_BYTE + SIZE_OF_INT + bytesWritten);
        }
    }

    @Override
    public UpdatableBlock updateBytes(Slice source, int sourceIndex, int length, int position, int offset){
        // currently, we cannot handle updates that do not exactly match the size
        // we would need another array encoding the size of each entry
        if(length != this.getSliceLength(position)){
            // deletes the current entry, decreases null counter if necessary and increases delete counter
            deleteBytes(position, offset, length);
            if (source != null) {
                this.writeBytes(source, sourceIndex, length);
                this.closeEntry();
            }else{
                this.appendNull();
            }
            return this;
        }else{
            if (source != null) {
                if (valueMarker[position] == NULL){
                    nullCounter--;
                    valueMarker[position] = 0;
                }else if (valueMarker[position] == DEL){
                    deleteCounter--;
                    valueMarker[position] = 0;
                }
                System.out.println("sliceOutput size: "+ this.sliceOutput.size()+ "slice output: "+ this.sliceOutput);
                this.sliceOutput.slice().setBytes(offsets[position], source, sourceIndex, length);
                return this;
            }else{
                if (valueMarker[position] == NULL){
                    // no-op
                    return this;
                }else if (valueMarker[position] == DEL){
                    deleteCounter--;
                    valueMarker[position] = NULL;
                    nullCounter++;
                    return this;
                }else{
                    valueMarker[position] = NULL;
                    nullCounter++;
                    return this;
                }
            }
        }
    }

    @Override
    public UpdatableBlock deleteBytes(int position, int offset, int length){
        if (valueMarker[position] == NULL){
            nullCounter--;
        }
        this.valueMarker[position] = DEL;
        deleteCounter++;
        return this;
    }

    private void growCapacity()
    {
        int newSize = BlockUtil.calculateNewArraySize(valueMarker.length);
        valueMarker = Arrays.copyOf(valueMarker, newSize);
        offsets = Arrays.copyOf(offsets, newSize + 1);
        updateArraysDataSize();
    }

    private void initializeCapacity()
    {
        if (positions != 0 || currentEntrySize != 0) {
            throw new IllegalStateException(getClass().getSimpleName() + " was used before initialization");
        }
        initialized = true;
        valueMarker = new byte[initialEntryCount];
        offsets = new int[initialEntryCount + 1];
        sliceOutput = new VariableSliceOutput(initialSliceOutputSize);
        updateArraysDataSize();
    }

    private void updateArraysDataSize()
    {
        arraysRetainedSizeInBytes = sizeOf(valueMarker) + sizeOf(offsets);
    }

    @Override
    public boolean mayHaveNull()
    {
        return nullCounter > 0;
    }

    @Override
    public UpdatableBlock makeUpdatable()
    {
        return this;
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueMarker[position] == NULL;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, positionOffset, length);

        CoreBool cb = compactValuesBool(positionOffset, positionOffset+length);
        // TODO does not check if the region has nulls, it looks at the entire array
        return new VariableWidthBlock(positionOffset, cb.offsets.length, cb.sliceOutput, cb.offsets, mayHaveNull() ? cb.markers : null);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        int positionCount = getPositionCount();
        checkValidRegion(positionCount, positionOffset, length);

        CoreBool cb = compactValuesBool(positionOffset, positionOffset+length);
        // TODO does not check if the region has nulls, it looks at the entire array
        return new VariableWidthBlock(positionOffset, cb.offsets.length, cb.sliceOutput, cb.offsets, mayHaveNull() ? cb.markers : null);
    }

    @Override
    public Block build()
    {
        if (currentEntrySize > 0) {
            throw new IllegalStateException("Current entry must be closed before the block can be built");
        }
        compact();
        return new VariableWidthBlock(0, positions, sliceOutput.slice(), offsets, mayHaveNull() ? toBoolean(valueMarker) : null);
    }

    @Override
    public UpdatableBlock newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus)
    {
        int currentSizeInBytes = positions == 0 ? positions : (getOffset(positions) - getOffset(0));
        return new UpdatableVariableWidthBlock(blockBuilderStatus, calculateBlockResetSize(positions), calculateBlockResetBytes(currentSizeInBytes));
    }

    @Override
    public boolean mayHaveDel()
    {
        return deleteCounter > 0;
    }

    private int getOffset(int position)
    {
        return offsets[position];
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("UpdatableVariableWidthBlock{");
        sb.append("positionCount=").append(positions);
        sb.append(", size=").append(sliceOutput.size());
        sb.append('}');
        return sb.toString();
    }

    private void compact(){
        if (deleteCounter != 0) {
            Core c = compactValues();
            long copy = getSizeInBytes();
            this.valueMarker = c.markers;
            this.sliceOutput = c.sliceOutput;
            this.offsets = c.offsets;
            this.positions = c.markers.length;
            this.deleteCounter = 0;
            updateArraysDataSize();
            if (blockBuilderStatus!=null) {
                blockBuilderStatus.addBytes((int) (getSizeInBytes() - copy));
            }
        }
    }

    private Core compactValues(){
        int start = 0;
        int end = positions;
        VariableSliceOutput sliceOutput = new VariableSliceOutput(offsets[end-1] - offsets[start] );
        byte markers[] = new byte[positions];
        int[] newoffsets = new int[positions];
        int cindex = 0;
        // TODO: not sure if inclusive is correct
        for (int i=start; i < end; i++){
            if(valueMarker[i] != DEL){
                sliceOutput.writeBytes(this.sliceOutput.slice().getBytes(offsets[i], this.getSliceLength(i)));
                markers[cindex] =  NULL;
                newoffsets[cindex] = sliceOutput.size();
                cindex++;
            }
        }
        return new Core(sliceOutput, markers, newoffsets);
    }

    private class Core{
        private VariableSliceOutput sliceOutput;
        private byte markers[];
        private int[] offsets;

        public Core(VariableSliceOutput sliceOutput, byte markers[], int[] offsets){
            this.sliceOutput = sliceOutput;
            this.markers = markers;
            this.offsets = offsets;
        }
    }

    private CoreBool compactValuesBool(int start, int end){
        VariableSliceOutput sliceOutput = new VariableSliceOutput(offsets[end] - offsets[start] );
        boolean markers[] = new boolean[end - start];
        int[] newoffsets = new int[end-start];
        int cindex = 0;
        // TODO: not sure if inclusive is correct
        for (int i=start; i < end; i++){
            if(valueMarker[i] != DEL){
                sliceOutput.writeBytes(this.sliceOutput.slice().getBytes(offsets[i], this.getSliceLength(i)));
                markers[cindex] = valueMarker[i] == NULL;
                newoffsets[cindex] = sliceOutput.size();
                cindex++;
            }
        }
        return new CoreBool( compactSlice(sliceOutput.getUnderlyingSlice(),0, offsets[cindex-1]), compactArray(markers, 0, cindex), compactOffsets(offsets, 0, cindex));
    }


    private class CoreBool{
        private Slice sliceOutput;
        private boolean markers[];
        private int[] offsets;

        public CoreBool(Slice sliceOutput, boolean markers[], int[] offsets){
            this.sliceOutput = sliceOutput;
            this.markers = markers;
            this.offsets = offsets;
        }
    }

    Slice getValueMarkerSlice()
    {
        return Slices.wrappedBuffer(valueMarker, 0, positions);
    }
}
