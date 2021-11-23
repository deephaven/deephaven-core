package io.deephaven.engine.table.impl.sources.regioned.decoder;

import io.deephaven.base.string.cache.ByteArrayCharSequenceAdapterImpl;
import io.deephaven.base.string.cache.StringCache;
import io.deephaven.engine.util.string.StringUtils;
import io.deephaven.util.codec.ObjectDecoder;
import org.jetbrains.annotations.NotNull;

public class SimpleStringDecoder<STRING_LIKE_TYPE extends CharSequence> implements ObjectDecoder<STRING_LIKE_TYPE> {
    private static final ThreadLocal<ByteArrayCharSequenceAdapterImpl> DECODER_ADAPTER =
            ThreadLocal.withInitial(ByteArrayCharSequenceAdapterImpl::new);

    private final StringCache<STRING_LIKE_TYPE> cache;

    public SimpleStringDecoder(Class<STRING_LIKE_TYPE> dataType) {
        this.cache = StringUtils.getStringCache(dataType);
    }

    public SimpleStringDecoder(StringCache<STRING_LIKE_TYPE> cache) {
        this.cache = cache;
    }

    @Override
    public final int expectedObjectWidth() {
        return VARIABLE_WIDTH_SENTINEL;
    }

    @Override
    public final STRING_LIKE_TYPE decode(@NotNull final byte[] data, final int offset, final int length) {
        if (length == 0) {
            return null;
        }
        if (length == 1 && data[offset] == 0) {
            return cache.getEmptyString();
        }
        // NB: Because the StringCache implementations in use convert bytes to chars 1:1 (with a 0xFF mask), we're
        // effectively using an ISO-8859-1 decoder.
        // We could probably move towards StringCaches with configurable Charsets for encoding/decoding directly
        // to/from ByteBuffers, but that's a step for later.
        final ByteArrayCharSequenceAdapterImpl adapter = DECODER_ADAPTER.get();
        final STRING_LIKE_TYPE result = cache.getCachedString(adapter.set(data, offset, length));
        adapter.clear();
        return result;
    }
}
