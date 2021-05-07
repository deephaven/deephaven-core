package io.deephaven.db.v2.select;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import org.jpy.PyModule;
import org.jpy.PyObject;

public class StringArgumentsBuilder implements ArgumentsBuilder<StringArgumentsBuilder.Context> {


    class Context implements ArgumentsBuilder.Context {

        private final PyObject npInt32;
        private final PyModule np;

        int[] tentativeOffsetsDestination;
        StringBuilder stringBuilder = new StringBuilder();
        Context(){
            this.np = PyModule.importModule("numpy");
            this.npInt32  = np.getAttribute("int32");
        }
    }

    public StringArgumentsBuilder() {
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
        context.stringBuilder.setLength(0);
        ObjectChunk<String, Attributes.Any> typedChunk = (ObjectChunk<String, Attributes.Any>) incomingData;
        for (int i = 0; i < size;i++) {
            context.stringBuilder.append(typedChunk.get(i));
            offsetDest[i] = context.stringBuilder.length();
        }

        return new Object[]{context.stringBuilder.toString(),context.np.call("asarray", offsetDest, context.npInt32)};
    }

    @Override
    public Context getContext() {
        return new Context();
    }
}
