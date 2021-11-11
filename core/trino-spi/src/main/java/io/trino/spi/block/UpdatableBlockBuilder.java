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

public interface UpdatableBlockBuilder
        extends BlockBuilder
{
    /**
     * Write a byte to the current entry;
     */
    default UpdatableBlockBuilder writeByte(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a short to the current entry;
     */
    default UpdatableBlockBuilder writeShort(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a int to the current entry;
     */
    default UpdatableBlockBuilder writeInt(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a long to the current entry;
     */
    default UpdatableBlockBuilder writeLong(long value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a byte sequences to the current entry;
     */
    default UpdatableBlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Update a byte to the current entry;
     */
    default UpdatableBlockBuilder updateByte(Integer value, int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Update a short to the current entry;
     */
    default UpdatableBlockBuilder updateShort(Integer value, int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Update a int to the current entry;
     */
    default UpdatableBlockBuilder updateInt(Integer value, int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Update a long to the current entry;
     */
    default UpdatableBlockBuilder updateLong(Long value, int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Update a byte sequences to the current entry;
     * sourceIndex is the position inside the source slice
     */
    default UpdatableBlockBuilder updateBytes(Slice source, int sourceIndex, int length,  int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Delete a byte to the current entry;
     */
    default UpdatableBlockBuilder deleteByte(int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Delete a short to the current entry;
     */
    default UpdatableBlockBuilder deleteShort(int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Delete a int to the current entry;
     */
    default UpdatableBlockBuilder deleteInt(int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Delete a long to the current entry;
     */
    default UpdatableBlockBuilder deleteLong(int position, int offset)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Delete a byte sequences to the current entry;
     */
    default UpdatableBlockBuilder deleteBytes(int position, int offset, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Return a writer to the current entry. The caller can operate on the returned caller to incrementally build the object. This is generally more efficient than
     * building the object elsewhere and call writeObject afterwards because a large chunk of memory could potentially be unnecessarily copied in this process.
     */
    default UpdatableBlockBuilder beginBlockEntry()
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
    UpdatableBlockBuilder closeEntry();

    /**
     * Appends a null value to the block.
     */
    UpdatableBlockBuilder appendNull();

    /**
     * Append a struct to the block and close the entry.
     */
    default UpdatableBlockBuilder appendStructure(Block value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Do not use this interface outside block package.
     * Instead, use Block.writePositionTo(BlockBuilder, position)
     */
    default UpdatableBlockBuilder appendStructureInternal(Block block, int position)
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
    UpdatableBlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus);

    /**
     * Returns true if there are deleted values in the block.
     */
    public boolean mayHaveDel();
}
