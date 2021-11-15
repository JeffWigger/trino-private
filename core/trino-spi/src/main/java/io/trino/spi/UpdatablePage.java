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
package io.trino.spi;

import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.DictionaryId;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.UpdatableBlock;
import io.trino.spi.block.UpdatableByteArrayBlock;
import io.trino.spi.block.UpdatableLongArrayBlock;
import io.trino.spi.block.UpdatableVariableWidthBlock;
import io.trino.spi.block.VariableWidthBlock;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class UpdatablePage
    extends Page
{
    public static final int INSTANCE_SIZE = ClassLayout.parseClass(UpdatablePage.class).instanceSize();
    private static final int DIFFERENCE_SIZE = INSTANCE_SIZE -  Page.INSTANCE_SIZE;
    private static final UpdatableBlock[] EMPTY_BLOCKS = new UpdatableBlock[0];

    /**
     * Visible to give trusted classes like {@link PageBuilder} access to a constructor that doesn't
     * defensively copy the blocks
     */
    static UpdatablePage wrapBlocksWithoutCopy(int positionCount, UpdatableBlock[] blocks)
    {
        return new UpdatablePage(false, positionCount, blocks);
    }

    public UpdatablePage(Block... blocks)
    {
        this(true, determinePositionCount(blocks), makeUpdatable(blocks));
    }

    public UpdatablePage(int positionCount)
    {
        this(false, positionCount, EMPTY_BLOCKS);
    }

    public UpdatablePage(int positionCount, Block... blocks)
    {
        this(true, positionCount, makeUpdatable(blocks));
    }

    public UpdatablePage(boolean blocksCopyRequired, int positionCount, UpdatableBlock[] blocks)
    {
        super(blocksCopyRequired, positionCount, blocks);
    }

    @Override
    public long getSizeInBytes()
    {
        return super.getSizeInBytes();
    }

    @Override
    public long getLogicalSizeInBytes()
    {
        return super.getLogicalSizeInBytes();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return super.getRetainedSizeInBytes() + DIFFERENCE_SIZE;
    }

    @Override
    public UpdatableBlock getBlock(int channel)
    {
        return (UpdatableBlock) super.blocks[channel];
    }

    public UpdatablePage appendColumn(UpdatableBlock block)
    {
        requireNonNull(block, "block is null");
        if (positionCount != block.getPositionCount()) {
            throw new IllegalArgumentException("Block does not have same position count");
        }

        UpdatableBlock[] newBlocks = new UpdatableBlock[blocks.length + 1];
        for (int i = 0; i < blocks.length; i++){
            newBlocks[i] = (UpdatableBlock) blocks[i];
        }
        newBlocks[blocks.length] = block;
        return wrapBlocksWithoutCopy(positionCount, newBlocks);
    }

    @Override
    public void compact()
    {
        // currently not supported
    }

    /**
     * Returns a page that assures all data is in memory.
     * May return the same page if all page data is already in memory.
     * <p>
     * This allows streaming data sources to skip sections that are not
     * accessed in a query.
     */
    @Override
    public UpdatablePage getLoadedPage()
    {
        // No newly loaded blocks
        return (UpdatablePage) super.getLoadedPage();
    }

    @Override
    public UpdatablePage getLoadedPage(int column)
    {
        return wrapBlocksWithoutCopy(positionCount, new UpdatableBlock[]{(UpdatableBlock) super.blocks[column].getLoadedBlock()});
    }

    @Override
    public UpdatablePage getLoadedPage(int... columns)
    {
        requireNonNull(columns, "columns is null");

        UpdatableBlock[] blocks = new UpdatableBlock[columns.length];
        for (int i = 0; i < columns.length; i++) {
            blocks[i] = (UpdatableBlock) this.blocks[columns[i]].getLoadedBlock();
        }
        return wrapBlocksWithoutCopy(positionCount, blocks);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("UpdatablePage{");
        builder.append("positions=").append(positionCount);
        builder.append(", channels=").append(getChannelCount());
        builder.append('}');
        builder.append("@").append(Integer.toHexString(System.identityHashCode(this)));
        return builder.toString();
    }

    public static int determinePositionCount(Block... blocks)
    {
        requireNonNull(blocks, "blocks is null");
        if (blocks.length == 0) {
            throw new IllegalArgumentException("blocks is empty");
        }

        return blocks[0].getPositionCount();
    }

    @Override
    public UpdatablePage getColumns(int column)
    {
        return wrapBlocksWithoutCopy(positionCount, new UpdatableBlock[] {(UpdatableBlock) this.blocks[column]});
    }

    @Override
    public UpdatablePage getColumns(int... columns)
    {
        requireNonNull(columns, "columns is null");

        UpdatableBlock[] blocks = new UpdatableBlock[columns.length];
        for (int i = 0; i < columns.length; i++) {
            super.blocks[i] = super.blocks[columns[i]];
        }
        return wrapBlocksWithoutCopy(positionCount, blocks);
    }

    public UpdatablePage prependColumn(UpdatableBlock column)
    {
        if (column.getPositionCount() != positionCount) {
            throw new IllegalArgumentException(format("Column does not have same position count (%s) as page (%s)", column.getPositionCount(), positionCount));
        }

        UpdatableBlock[] result = new UpdatableBlock[blocks.length + 1];
        result[0] = column;
        System.arraycopy(blocks, 0, result, 1, blocks.length);

        return wrapBlocksWithoutCopy(positionCount, result);
    }

    public static class DictionaryBlockIndexes
    {
        private final List<DictionaryBlock> blocks = new ArrayList<>();
        private final List<Integer> indexes = new ArrayList<>();

        public void addBlock(DictionaryBlock block, int index)
        {
            blocks.add(block);
            indexes.add(index);
        }

        public List<DictionaryBlock> getBlocks()
        {
            return blocks;
        }

        public List<Integer> getIndexes()
        {
            return indexes;
        }
    }

    public static UpdatableBlock[]  makeUpdatable(Block[] blocks){
        UpdatableBlock[] uBlocks = new UpdatableBlock[blocks.length];
        for (int i = 0; i < blocks.length; i++){
            uBlocks[i] = blocks[i].makeUpdatable();
        }
        return uBlocks;
    }

    public void updateRow(Page row, int position){
        int cols = getChannelCount();
        for (int c = 0; c < cols; c++) {
            updateBlock(this.getBlock(c), row.getBlock(c), position);
        }
    }

    public void deleteRow(int position){
        int cols = getChannelCount();
        for (int c = 0; c < cols; c++) {
            deleteBlock(this.getBlock(c), position);
        }
    }



    private void updateBlock(UpdatableBlock target, Block sourceRow, int position){
        // target and source should only differ in being updatable and not
        if (target instanceof UpdatableLongArrayBlock){
            target.updateLong(((LongArrayBlock) sourceRow).getLong(0, 0), position, 0);
        } else if (target instanceof UpdatableByteArrayBlock){
            target.updateByte((int)((ByteArrayBlock) sourceRow).getByte(0, 0), position, 0);
        } else if (target instanceof UpdatableVariableWidthBlock){
            VariableWidthBlock variableWidthBlock = (VariableWidthBlock) sourceRow;
            int length = variableWidthBlock.getSliceLength(0);
            target.updateBytes(variableWidthBlock.getSlice(0, 0,length ), 0, length, position, 0);
        }
    }

    private void deleteBlock(UpdatableBlock target, int position){
        if (target instanceof UpdatableLongArrayBlock){
            target.deleteLong(position, 0);
        }
    }

    public Page wrapBlocksWithoutCopyToPage(){
        return Page.wrapBlocksWithoutCopy(getPositionCount(), blocks);
    }
}
