package io.deephaven.db.v2.select;

import io.deephaven.db.v2.sources.chunk.Chunk;
import org.jpy.PyModule;

import java.lang.reflect.Array;

public class PrimitiveArgumentsBuilder implements ArgumentsBuilder<PrimitiveArgumentsBuilder.Context> {

    private final String npTypeName;

    class Context implements ArgumentsBuilder.Context {
        private final PyModule np;
        private final Object npType;
        Object tentativeDestination;
        int currentSize = 0;
        Context() {
            this.np = PyModule.importModule("numpy");
            this.npType = np.getAttribute(npTypeName);
        }
    }

    private final Class<?> javaType;

    public PrimitiveArgumentsBuilder(String npTypeName, Class<?> javaType) {
        this.javaType = javaType;
        this.npTypeName = npTypeName;
    }


    @Override
    public int getArgumentsCount() {
        return 1;
    }

    @Override
    public Object[] packArguments(Chunk incomingData, Context context, int size) {
        Object destArray = context.tentativeDestination != null && context.currentSize == size ?
                context.tentativeDestination : Array.newInstance(javaType, size);
        context.tentativeDestination = destArray;
        incomingData.copyToArray(0, destArray, 0, size);
        return new Object[]{context.np.call("asarray", destArray, context.npType)};
    }

    @Override
    public Context getContext() {
        return new Context();
    }
}
