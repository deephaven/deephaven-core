/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.v2.select.Formula;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.*;

import static io.deephaven.util.QueryConstants.*;

public class ViewColumnSource<T> extends AbstractColumnSource<T> {
    private final Formula formula;
    // We explicitly want all Groovy commands to run under the 'file:/groovy/shell' source, so
    // explicitly create that.
    private static URL groovyShellUrl;
    static {
        try {
            groovyShellUrl = new URL("file:/groovy/shell");
        } catch (MalformedURLException ignored) {
            groovyShellUrl = null;
            // It should not be possible for this to get malformed.
        }
    }

    private static final CodeSource codeSource =
        new CodeSource(groovyShellUrl, (java.security.cert.Certificate[]) null);
    // The permission collection should not be static, because the class loader might take place
    // before the
    // custom policy object is assigned.
    private final PermissionCollection perms = Policy.getPolicy().getPermissions(codeSource);
    private final AccessControlContext context = AccessController
        .doPrivileged((PrivilegedAction<AccessControlContext>) () -> new AccessControlContext(
            new ProtectionDomain[] {new ProtectionDomain(
                new CodeSource(groovyShellUrl, (java.security.cert.Certificate[]) null), perms)}));

    public ViewColumnSource(Class<T> type, Formula formula) {
        super(type);
        this.formula = formula;
    }

    public ViewColumnSource(Class<T> type, Class elementType, Formula formula) {
        super(type, elementType);
        this.formula = formula;
    }

    @Override
    public void startTrackingPrevValues() {
        // Do nothing.
    }

    @Override
    public T get(long index) {
        if (index < 0) {
            return null;
        }
        final SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            return AccessController.doPrivileged((PrivilegedAction<T>) () -> {
                // noinspection unchecked
                return (T) formula.get(index);
            }, context);
        } else {
            // noinspection unchecked
            return (T) formula.get(index);
        }
    }

    @Override
    public Boolean getBoolean(long index) {
        if (index < 0) {
            return null;
        }
        return formula.getBoolean(index);
    }

    @Override
    public byte getByte(long index) {
        if (index < 0) {
            return NULL_BYTE;
        }
        return formula.getByte(index);
    }

    @Override
    public char getChar(long index) {
        if (index < 0) {
            return NULL_CHAR;
        }
        return formula.getChar(index);
    }

    @Override
    public double getDouble(long index) {
        if (index < 0) {
            return NULL_DOUBLE;
        }
        return formula.getDouble(index);
    }

    @Override
    public float getFloat(long index) {
        if (index < 0) {
            return NULL_FLOAT;
        }
        return formula.getFloat(index);
    }

    @Override
    public int getInt(long index) {
        if (index < 0) {
            return NULL_INT;
        }
        return formula.getInt(index);
    }

    @Override
    public long getLong(long index) {
        if (index < 0) {
            return NULL_LONG;
        }
        return formula.getLong(index);
    }

    @Override
    public short getShort(long index) {
        if (index < 0) {
            return NULL_SHORT;
        }
        return formula.getShort(index);
    }

    @Override
    public T getPrev(long index) {
        if (index < 0) {
            return null;
        }
        // noinspection unchecked
        return (T) formula.getPrev(index);
    }

    @Override
    public Boolean getPrevBoolean(long index) {
        if (index < 0) {
            return null;
        }
        return formula.getPrevBoolean(index);
    }

    @Override
    public byte getPrevByte(long index) {
        if (index < 0) {
            return NULL_BYTE;
        }
        return formula.getPrevByte(index);
    }

    @Override
    public char getPrevChar(long index) {
        if (index < 0) {
            return NULL_CHAR;
        }
        return formula.getPrevChar(index);
    }

    @Override
    public double getPrevDouble(long index) {
        if (index < 0) {
            return NULL_DOUBLE;
        }
        return formula.getPrevDouble(index);
    }

    @Override
    public float getPrevFloat(long index) {
        if (index < 0) {
            return NULL_FLOAT;
        }
        return formula.getPrevFloat(index);
    }

    @Override
    public int getPrevInt(long index) {
        if (index < 0) {
            return NULL_INT;
        }
        return formula.getPrevInt(index);
    }

    @Override
    public long getPrevLong(long index) {
        if (index < 0) {
            return NULL_LONG;
        }
        return formula.getPrevLong(index);
    }

    @Override
    public short getPrevShort(long index) {
        if (index < 0) {
            return NULL_SHORT;
        }
        return formula.getPrevShort(index);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedState) {
        return new VCSGetContext(formula.makeGetContext(chunkCapacity));
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedState) {
        return new VCSFillContext(formula.makeFillContext(chunkCapacity));
    }

    @Override
    public Chunk<Values> getChunk(@NotNull final GetContext context,
        @NotNull final OrderedKeys orderedKeys) {
        return formula.getChunk(((VCSGetContext) context).underlyingGetContext, orderedKeys);

    }

    @Override
    public Chunk<Values> getPrevChunk(@NotNull final GetContext context,
        @NotNull final OrderedKeys orderedKeys) {
        return formula.getPrevChunk(((VCSGetContext) context).underlyingGetContext, orderedKeys);

    }

    @Override
    public void fillChunk(@NotNull final FillContext context,
        @NotNull final WritableChunk<? super Values> destination,
        @NotNull final OrderedKeys orderedKeys) {
        formula.fillChunk(((VCSFillContext) context).underlyingFillContext, destination,
            orderedKeys);
    }


    @Override
    public void fillPrevChunk(@NotNull final FillContext context,
        @NotNull final WritableChunk<? super Values> destination,
        @NotNull final OrderedKeys orderedKeys) {
        formula.fillPrevChunk(((VCSFillContext) context).underlyingFillContext, destination,
            orderedKeys);
    }

    public static class VCSGetContext implements GetContext {
        private final Formula.GetContext underlyingGetContext;

        public VCSGetContext(Formula.GetContext underlyingGetContext) {
            this.underlyingGetContext = underlyingGetContext;
        }

        @Override
        public void close() {
            underlyingGetContext.close();
        }
    }

    public static class VCSFillContext implements FillContext {
        private final Formula.FillContext underlyingFillContext;

        public VCSFillContext(Formula.FillContext underlyingFillContext) {
            this.underlyingFillContext = underlyingFillContext;
        }

        @Override
        public void close() {
            underlyingFillContext.close();
        }
    }
}
