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
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class DeltaPage
    extends Page
{
    public static final int INSTANCE_SIZE = ClassLayout.parseClass(DeltaPage.class).instanceSize();
    private static final int DIFFERENCE_SIZE = INSTANCE_SIZE -  Page.INSTANCE_SIZE;
    private static final Block[] EMPTY_BLOCKS = new Block[0];

    /**
     * Visible to give trusted classes like {@link PageBuilder} access to a constructor that doesn't
     * defensively copy the blocks
     */
    public static DeltaPage wrapBlocksWithoutCopy(int positionCount, Block[] blocks, Block updateType)
    {
        return new DeltaPage(false, positionCount, blocks, updateType);
    }


    private final Block updateType;



    public DeltaPage(int positionCount)
    {
        super(positionCount);
        // TODO figure out what to best set here
        this.updateType = new ByteArrayBlock(positionCount, Optional.empty(),new byte[positionCount]);
    }
    public DeltaPage(int positionCount, Block[] blocks, Block updateType) {
        this(true, positionCount, blocks, updateType);
    }
    public DeltaPage(Block[] blocks, Block updateType) {
        this(true, determinePositionCount(blocks), blocks, updateType);
    }
    private DeltaPage(boolean blocksCopyRequired, int positionCount, Block[] blocks, Block updateType)
    {
        super(blocksCopyRequired, positionCount, blocks);
        this.updateType = updateType;

    }

    public Block getUpdateType()
    {
        return updateType;
    }

    @Override
    public long getSizeInBytes()
    {
        return super.getSizeInBytes() + updateType.getLoadedBlock().getSizeInBytes();
    }

    @Override
    public long getLogicalSizeInBytes()
    {
        return super.getLogicalSizeInBytes() + updateType.getLogicalSizeInBytes();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        // TODO: Does the size of this class include the size of the super calss?
        return getSizeInBytes() + DIFFERENCE_SIZE + updateType.getRetainedSizeInBytes();
    }

    /**
     * Gets the values at the specified position as a single element page.  The method creates independent
     * copy of the data.
     */
    @Override
    public DeltaPage getSingleValuePage(int position)
    {
        int length = super.getChannelCount();
        Block[] singleValueBlocks = new Block[length];
        for (int i = 0; i < length; i++) {
            singleValueBlocks[i] = super.getBlock(i).getSingleValueBlock(position);
        }
        Block singleValueUpdate = updateType.getSingleValueBlock(position);
        return wrapBlocksWithoutCopy(1, singleValueBlocks, singleValueUpdate);
    }

    @Override
    public DeltaPage getRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || length < 0 || positionOffset + length > positionCount) {
            throw new IndexOutOfBoundsException(format("Invalid position %s and length %s in page with %s positions", positionOffset, length, positionCount));
        }

        int channelCount = getChannelCount();
        Block[] slicedBlocks = new Block[channelCount];
        for (int i = 0; i < channelCount; i++) {
            slicedBlocks[i] = super.getBlock(i).getRegion(positionOffset, length);
        }
        Block sliceUpdateType = updateType.getRegion(positionOffset, length);
        return wrapBlocksWithoutCopy(length, slicedBlocks, sliceUpdateType);
    }

    @Override
    public DeltaPage appendColumn(Block block)
    {
        requireNonNull(block, "block is null");
        if (positionCount != block.getPositionCount()) {
            throw new IllegalArgumentException("Block does not have same position count");
        }
        int channelCount = getChannelCount();
        Block[] newBlocks = new Block[channelCount+1];
        for (int i = 0; i < channelCount; i++) {
            newBlocks[i] = super.getBlock(i);
        }
        newBlocks[channelCount] = block;
        return wrapBlocksWithoutCopy(positionCount, newBlocks, updateType);
    }

    @Override
    public void compact()
    {
        // TODO: compact updateType
        super.compact();
    }

    private Map<DictionaryId, DictionaryBlockIndexes> getRelatedDictionaryBlocks()
    {
        Map<DictionaryId, DictionaryBlockIndexes> relatedDictionaryBlocks = new HashMap<>();

        int channelCount = getChannelCount();
        for (int i = 0; i < channelCount; i++) {
            Block block =super.getBlock(i);
            if (block instanceof DictionaryBlock) {
                DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
                relatedDictionaryBlocks.computeIfAbsent(dictionaryBlock.getDictionarySourceId(), id -> new DictionaryBlockIndexes())
                        .addBlock(dictionaryBlock, i);
            }
        }
        return relatedDictionaryBlocks;
    }


    /**
     * Returns a page that assures all data is in memory.
     * May return the same page if all page data is already in memory.
     * <p>
     * This allows streaming data sources to skip sections that are not
     * accessed in a query.
     */
    @Override
    public DeltaPage getLoadedPage()
    {
        int channelCount = getChannelCount();
        for (int i = 0; i < channelCount; i++) {
            Block loaded = super.getBlock(i).getLoadedBlock();
            if (loaded != super.getBlock(i)) {
                // Transition to new block creation mode after the first newly loaded block is encountered
                Block[] loadedBlocks = new Block[channelCount+1];
                for (int j = 0; j < channelCount; j++) {
                    loadedBlocks[j] = super.getBlock(j);
                }
                loadedBlocks[i++] = loaded;
                for (; i < channelCount; i++) {
                    loadedBlocks[i] = super.getBlock(i).getLoadedBlock();
                }
                return wrapBlocksWithoutCopy(positionCount, loadedBlocks, updateType);
            }
        }
        // No newly loaded blocks
        return this;
    }

    @Override
    public DeltaPage getLoadedPage(int column)
    {
        return wrapBlocksWithoutCopy(positionCount, new Block[]{super.getBlock(column).getLoadedBlock()}, updateType);
    }

    @Override
    public DeltaPage getLoadedPage(int... columns)
    {
        requireNonNull(columns, "columns is null");

        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            blocks[i] = super.getBlock(columns[i]).getLoadedBlock();
        }
        return wrapBlocksWithoutCopy(positionCount, blocks, updateType);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("Page{");
        builder.append("positions=").append(positionCount);
        builder.append(", channels=").append(getChannelCount());
        builder.append('}');
        builder.append("@").append(Integer.toHexString(System.identityHashCode(this)));
        return builder.toString();
    }

    @Override
    public DeltaPage getPositions(int[] retainedPositions, int offset, int length)
    {
        requireNonNull(retainedPositions, "retainedPositions is null");
        int channelCount = getChannelCount();
        Block[] blocks = new Block[channelCount];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = super.getBlock(i).getPositions(retainedPositions, offset, length);
        }
        return wrapBlocksWithoutCopy(length, blocks, updateType);
    }

    @Override
    public DeltaPage copyPositions(int[] retainedPositions, int offset, int length)
    {
        requireNonNull(retainedPositions, "retainedPositions is null");

        int channelCount = getChannelCount();
        Block[] blocks = new Block[channelCount];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = super.getBlock(i).copyPositions(retainedPositions, offset, length);
        }
        return wrapBlocksWithoutCopy(length, blocks, updateType);
    }

    @Override
    public DeltaPage prependColumn(Block column)
    {
        if (column.getPositionCount() != positionCount) {
            throw new IllegalArgumentException(format("Column does not have same position count (%s) as page (%s)", column.getPositionCount(), positionCount));
        }
        int channelCount = getChannelCount();
        Block[] result = new Block[channelCount + 1];
        result[0] = column;
        for (int i = 1; i < channelCount+1; i++) {
            result[i] = super.getBlock(i);
        }

        return wrapBlocksWithoutCopy(positionCount, result, updateType);
    }
}
