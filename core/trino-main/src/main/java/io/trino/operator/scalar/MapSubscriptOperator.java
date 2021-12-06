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
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.FunctionDependencyDeclaration;
import io.trino.metadata.FunctionInvoker;
import io.trino.metadata.FunctionMetadata;
import io.trino.metadata.SqlOperator;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SingleMapBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.sql.InterpretedFunctionInvoker;

import java.lang.invoke.MethodHandle;

import static io.trino.metadata.Signature.typeVariable;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.String.format;

public class MapSubscriptOperator
        extends SqlOperator
{
    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(MapSubscriptOperator.class, "subscript", MissingKeyExceptionFactory.class, Type.class, Type.class, ConnectorSession.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(MapSubscriptOperator.class, "subscript", MissingKeyExceptionFactory.class, Type.class, Type.class, ConnectorSession.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(MapSubscriptOperator.class, "subscript", MissingKeyExceptionFactory.class, Type.class, Type.class, ConnectorSession.class, Block.class, double.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(MapSubscriptOperator.class, "subscript", MissingKeyExceptionFactory.class, Type.class, Type.class, ConnectorSession.class, Block.class, Object.class);

    public MapSubscriptOperator()
    {
        super(SUBSCRIPT,
                ImmutableList.of(typeVariable("K"), typeVariable("V")),
                ImmutableList.of(),
                new TypeSignature("V"),
                ImmutableList.of(mapType(new TypeSignature("K"), new TypeSignature("V")), new TypeSignature("K")),
                true);
    }

    @UsedByGeneratedCode
    public static Object subscript(MissingKeyExceptionFactory missingKeyExceptionFactory, Type keyType, Type valueType, ConnectorSession session, Block map, boolean key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(MissingKeyExceptionFactory missingKeyExceptionFactory, Type keyType, Type valueType, ConnectorSession session, Block map, long key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(MissingKeyExceptionFactory missingKeyExceptionFactory, Type keyType, Type valueType, ConnectorSession session, Block map, double key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(MissingKeyExceptionFactory missingKeyExceptionFactory, Type keyType, Type valueType, ConnectorSession session, Block map, Object key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @Override
    public FunctionDependencyDeclaration getFunctionDependencies()
    {
        return FunctionDependencyDeclaration.builder()
                .addOptionalCastSignature(new TypeSignature("K"), VARCHAR.getTypeSignature())
                .build();
    }

    @Override
    public ScalarFunctionImplementation specialize(FunctionBinding functionBinding, FunctionDependencies functionDependencies)
    {
        Type keyType = functionBinding.getTypeVariable("K");
        Type valueType = functionBinding.getTypeVariable("V");

        MethodHandle methodHandle;
        if (keyType.getJavaType() == boolean.class) {
            methodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (keyType.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_LONG;
        }
        else if (keyType.getJavaType() == double.class) {
            methodHandle = METHOD_HANDLE_DOUBLE;
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT;
        }
        MissingKeyExceptionFactory missingKeyExceptionFactory = new MissingKeyExceptionFactory(functionDependencies, keyType);
        methodHandle = methodHandle.bindTo(missingKeyExceptionFactory).bindTo(keyType).bindTo(valueType);
        methodHandle = methodHandle.asType(methodHandle.type().changeReturnType(Primitives.wrap(valueType.getJavaType())));

        return new ChoicesScalarFunctionImplementation(
                functionBinding,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL, NEVER_NULL),
                methodHandle);
    }

    private static class MissingKeyExceptionFactory
    {
        private final FunctionMetadata castMetadata;
        private final FunctionInvoker castFunction;

        public MissingKeyExceptionFactory(FunctionDependencies functionDependencies, Type keyType)
        {
            FunctionMetadata castMetadata = null;
            FunctionInvoker castFunction = null;
            try {
                castMetadata = functionDependencies.getCastMetadata(keyType, VARCHAR);
                castFunction = functionDependencies.getCastInvoker(keyType, VARCHAR, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
            }
            catch (TrinoException ignored) {
            }
            this.castMetadata = castMetadata;
            this.castFunction = castFunction;
        }

        public TrinoException create(ConnectorSession session, Object value)
        {
            if (castFunction != null) {
                try {
                    Slice varcharValue = (Slice) InterpretedFunctionInvoker.invoke(castMetadata, castFunction, session, ImmutableList.of(value));

                    return new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Key not present in map: %s", varcharValue == null ? "NULL" : varcharValue.toStringUtf8()));
                }
                catch (RuntimeException ignored) {
                }
            }
            return new TrinoException(INVALID_FUNCTION_ARGUMENT, "Key not present in map");
        }
    }
}
