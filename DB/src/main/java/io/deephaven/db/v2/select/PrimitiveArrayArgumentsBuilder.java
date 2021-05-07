package io.deephaven.db.v2.select;

import io.deephaven.db.v2.sources.chunk.*;
import org.jpy.PyModule;
import org.jpy.PyObject;

import java.lang.reflect.Array;

public class PrimitiveArrayArgumentsBuilder implements ArgumentsBuilder<PrimitiveArrayArgumentsBuilder.Context> {

    private final Class<?> javaType;
    private final String npElementTypeName;

    class Context implements ArgumentsBuilder.Context {

        private final PyObject npInt32;
        private final PyModule np;
        private final Object npType;

        int[] tentativeOffsetsDestination;
        Context(){
            this.np = PyModule.importModule("numpy");
            this.npType = np.getAttribute(npElementTypeName);
            this.npInt32  = np.getAttribute("int32");
        }
    }

    public PrimitiveArrayArgumentsBuilder(String npTypeName) {
        npElementTypeName = npTypeName.substring(0, npTypeName.length() - 3);
        this.javaType = NumbaCompileTools.toJavaType(npElementTypeName);
    }

    @Override
    public int getArgumentsCount() {
        return 2;
    }

    @Override
    public Object[] packArguments(Chunk incomingData, Context context, int size) {
        int[] offsetDest = context.tentativeOffsetsDestination != null && context.tentativeOffsetsDestination.length == size ?
                context.tentativeOffsetsDestination : new int[size];
        context.tentativeOffsetsDestination = offsetDest;
        int totalSize = 0;
        ObjectChunk typedChunk = (ObjectChunk) incomingData;
        for (int i = 0; i < size; i++) {
            totalSize += Array.getLength(typedChunk.get(i));

        }
        Object destArray = Array.newInstance(javaType, totalSize);
        WritableChunk<Attributes.Any> chunkDest = ChunkType.fromElementType(javaType).writableChunkWrap(destArray, 0, totalSize);
        int offset = 0;
        for (int i = 0; i < size; i++) {
            Object srcArray = typedChunk.get(i);
            int len = Array.getLength(srcArray);
            chunkDest.copyFromArray(srcArray,0,offset,len);
            offset += len;
            offsetDest[i] = offset;
        }

        return new Object[]{context.np.call("asarray", destArray, context.npType),context.np.call("asarray", offsetDest, context.npInt32)};
    }

    @Override
    public Context getContext() {
        return new Context();
    }
}
