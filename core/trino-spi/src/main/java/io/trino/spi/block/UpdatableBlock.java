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

public interface UpdatableBlock
        extends BlockBuilder
{
    /**
     * Write a byte to the current entry;
     */
    default UpdatableBlock writeByte(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a short to the current entry;
     */
    default UpdatableBlock writeShort(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a int to the current entry;
     */
    default UpdatableBlock writeInt(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a long to the current entry;
     */
    default UpdatableBlock writeLong(long value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a byte sequences to the current entry;
     */
    default UpdatableBlock writeBytes(Slice source, int sourceIndex, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Update a byte to the current entry;
     */
    default UpdatableBlock updateByte(Integer value, int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Update a short to the current entry;
     */
    default UpdatableBlock updateShort(Integer value, int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Update a int to the current entry;
     */
    default UpdatableBlock updateInt(Integer value, int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Update a long to the current entry;
     */
    default UpdatableBlock updateLong(Long value, int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Update a byte sequences to the current entry;
     * sourceIndex is the position inside the source slice
     */
    default UpdatableBlock updateBytes(Slice source, int sourceIndex, int length, int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Delete a byte to the current entry;
     */
    default UpdatableBlock deleteByte(int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Delete a short to the current entry;
     */
    default UpdatableBlock deleteShort(int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Delete a int to the current entry;
     */
    default UpdatableBlock deleteInt(int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Delete a long to the current entry;
     */
    default UpdatableBlock deleteLong(int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Delete a byte sequences to the current entry;
     */
    default UpdatableBlock deleteBytes(int position, int offset, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Return a writer to the current entry. The caller can operate on the returned caller to incrementally build the object. This is generally more efficient than
     * building the object elsewhere and call writeObject afterwards because a large chunk of memory could potentially be unnecessarily copied in this process.
     */
    default UpdatableBlock beginBlockEntry()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Create a new block from the current materialized block by keeping the same elements
     * only with respect to {@code visiblePositions}.
     */
    @Override
    default Block getPositions(int[] visiblePositions, int offset, int length)
    {
        return build().getPositions(visiblePositions, offset, length);
    }

    /**
     * Write a byte to the current entry;
     */
    UpdatableBlock closeEntry();

    /**
     * Appends a null value to the block.
     */
    UpdatableBlock appendNull();

    /**
     * Append a struct to the block and close the entry.
     */
    default UpdatableBlock appendStructure(Block value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Do not use this interface outside block package.
     * Instead, use Block.writePositionTo(BlockBuilder, position)
     */
    default UpdatableBlock appendStructureInternal(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Builds the block. This method can be called multiple times.
     */
    Block build();

    /**
     * Creates a new block builder of the same type based on the current usage statistics of this block builder.
     */
    UpdatableBlock newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus);

    /**
     * Returns true if there are deleted values in the block.
     */
    boolean mayHaveDel();

    default UpdatableBlock getLoadedBlock()
    {
        return this;
    }

    @Override
    default Block getRegion(int positionOffset, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    default Block copyRegion(int positionOffset, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Returns length number of rows that start from positionOffset.
     * If there are less then length rows it will return all available ones
     * It does not return deleted rows
     */
    Block getEntriesFrom(int positionOffset, int length);

    /**
     * returns a new instance like this updatabeBlock. The sizes of the fields are retained, but the values are not copied!. blockBuilderStatus is set to null.
     */
    UpdatableBlock newLike();


}
