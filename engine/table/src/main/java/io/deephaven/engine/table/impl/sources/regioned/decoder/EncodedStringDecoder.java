package io.deephaven.engine.table.impl.sources.regioned.decoder;

import io.deephaven.base.string.EncodingInfo;
import io.deephaven.base.string.cache.StringCache;
import io.deephaven.engine.util.string.StringUtils;
import io.deephaven.util.codec.ObjectDecoder;
import org.jetbrains.annotations.NotNull;

public class EncodedStringDecoder<STRING_LIKE_TYPE extends CharSequence> implements ObjectDecoder<STRING_LIKE_TYPE> {
    private final StringCache<STRING_LIKE_TYPE> cache;
    private final EncodingInfo encodingInfo;

    public EncodedStringDecoder(Class<STRING_LIKE_TYPE> dataType, EncodingInfo encodingInfo) {
        this.cache = StringUtils.getStringCache(dataType);
        this.encodingInfo = encodingInfo;
    }

    public EncodedStringDecoder(StringCache<STRING_LIKE_TYPE> cache, EncodingInfo encodingInfo) {
        this.cache = cache;
        this.encodingInfo = encodingInfo;
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
        if (length == 1 && data[0] == 0) {
            return cache.getEmptyString();
        }
        return cache.getCachedString(new String(data, offset, length, encodingInfo.getCharset()));
    }
}
