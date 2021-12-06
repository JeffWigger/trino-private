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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.FunctionArgumentDefinition;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.Signature;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DuplicateMapKeyException;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.metadata.FunctionKind.SCALAR;
import static io.trino.metadata.Signature.comparableTypeParameter;
import static io.trino.metadata.Signature.typeVariable;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.Failures.internalError;
import static io.trino.util.Reflection.constructorMethodHandle;
import static io.trino.util.Reflection.methodHandle;

public final class MapConstructor
        extends SqlScalarFunction
{
    public static final MapConstructor MAP_CONSTRUCTOR = new MapConstructor();

    private static final MethodHandle METHOD_HANDLE = methodHandle(
            MapConstructor.class,
            "createMap",
            MapType.class,
            MethodHandle.class,
            State.class,
            ConnectorSession.class,
            Block.class,
            Block.class);

    private static final String DESCRIPTION = "Constructs a map from the given key/value arrays";

    public MapConstructor()
    {
        super(new FunctionMetadata(
                new Signature(
                        "map",
                        ImmutableList.of(comparableTypeParameter("K"), typeVariable("V")),
                        ImmutableList.of(),
                        TypeSignature.mapType(new TypeSignature("K"), new TypeSignature("V")),
                        ImmutableList.of(arrayType(new TypeSignature("K")), arrayType(new TypeSignature("V"))),
                        false),
                false,
                ImmutableList.of(
                        new FunctionArgumentDefinition(false),
                        new FunctionArgumentDefinition(false)),
                false,
                true,
                DESCRIPTION,
                SCALAR));
    }

    @UsedByGeneratedCode
    public static Block createMap(
            MapType mapType,
            MethodHandle keyIndeterminate,
            State state,
            ConnectorSession session,
            Block keyBlock,
            Block valueBlock)
    {
        checkCondition(keyBlock.getPositionCount() == valueBlock.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "Key and value arrays must be the same length");
        PageBuilder pageBuilder = state.getPageBuilder();
        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) pageBuilder.getBlockBuilder(0);
        mapBlockBuilder.strict();
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();
        for (int i = 0; i < keyBlock.getPositionCount(); i++) {
            if (keyBlock.isNull(i)) {
                // close block builder before throwing as we may be in a TRY() call
                // so that subsequent calls do not find it in an inconsistent state
                mapBlockBuilder.closeEntry();
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
            }
            Object keyObject = readNativeValue(mapType.getKeyType(), keyBlock, i);
            try {
                if ((boolean) keyIndeterminate.invoke(keyObject)) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be indeterminate: " + mapType.getKeyType().getObjectValue(session, keyBlock, i));
                }
            }
            catch (Throwable t) {
                mapBlockBuilder.closeEntry();
                throw internalError(t);
            }
            mapType.getKeyType().appendTo(keyBlock, i, blockBuilder);
            mapType.getValueType().appendTo(valueBlock, i, blockBuilder);
        }
        try {
            mapBlockBuilder.closeEntry();
        }
        catch (DuplicateMapKeyException e) {
            throw e.withDetailedMessage(mapType.getKeyType(), session);
        }
        finally {
            pageBuilder.declarePosition();
        }

        return mapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addOperatorSignature(HASH_CODE, ImmutableList.of(new TypeSignature("K")))
                .addOperatorSignature(EQUAL, ImmutableList.of(new TypeSignature("K"), new TypeSignature("K")))
                .addOperatorSignature(INDETERMINATE, ImmutableList.of(new TypeSignature("K")))
                .build();
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        Type keyType = functionBinding.getTypeVariable("K");

        MethodHandle keyIndeterminate = functionDependencies.getOperatorInvoker(INDETERMINATE, ImmutableList.of(keyType), simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle();

        Type mapType = functionBinding.getBoundSignature().getReturnType();
        MethodHandle instanceFactory = constructorMethodHandle(State.class, MapType.class).bindTo(mapType);

        return new ChoicesScalarFunctionImplementation(
                functionBinding,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                METHOD_HANDLE.bindTo(mapType).bindTo(keyIndeterminate),
                Optional.of(instanceFactory));
    }

    public static final class State
    {
        private final PageBuilder pageBuilder;

        public State(MapType mapType)
        {
            pageBuilder = new PageBuilder(ImmutableList.of(mapType));
        }

        public PageBuilder getPageBuilder()
        {
            return pageBuilder;
        }
    }
}
