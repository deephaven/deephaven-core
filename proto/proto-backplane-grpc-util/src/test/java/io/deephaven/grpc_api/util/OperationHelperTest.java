package io.deephaven.grpc_api.util;

import com.google.protobuf.UnknownFieldSet;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation.Builder;
import io.deephaven.proto.backplane.grpc.TableReference;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OperationHelperTest {
    @Test
    public void getSourceIds()
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        // ensure we aren't missing any cases in OperationHelper#getSourceIds
        TableReference tableReferenceDefault = TableReference.getDefaultInstance();
        for (Operation operation : getAllDefaultOperations()) {
            OperationHelper.getSourceIds(operation)
                    .forEach(id -> assertEquals(tableReferenceDefault, id));
        }
    }

    /**
     * Protobuf generated code does not have a built-in way to enumerate all default objects for the underlying cases.
     */
    public static List<Operation> getAllDefaultOperations()
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        final List<Operation> operations = new ArrayList<>();
        for (Method declaredMethod : Builder.class.getDeclaredMethods()) {
            if (declaredMethod.getParameterCount() != 1) {
                continue;
            }
            if (!declaredMethod.getName().startsWith("set")) {
                continue;
            }
            final Class<?> parameterType = declaredMethod.getParameterTypes()[0];
            if ("Builder".equals(parameterType.getSimpleName())) {
                continue;
            }
            if (UnknownFieldSet.class.equals(parameterType)) {
                continue;
            }
            final Object defaultOperationValue = defaultInstance(parameterType);
            // operation = Operation.newBuilder().setX(X.getDefaultInstance()).build()
            final Operation operation =
                    ((Builder) declaredMethod.invoke(Operation.newBuilder(), defaultOperationValue))
                            .build();
            operations.add(operation);
        }
        return operations;
    }

    private static Object defaultInstance(Class<?> clazz)
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        Method method = clazz.getDeclaredMethod("getDefaultInstance");
        return method.invoke(null);
    }
}
