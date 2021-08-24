package io.deephaven.process;

import io.deephaven.db.tablelogger.ProcessInfoLogLogger;
import io.deephaven.properties.PropertyVisitorStringBase;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

public class ProcessInfoStoreDBImpl implements ProcessInfoStore {
    private final ProcessInfoLogLogger logger;

    public ProcessInfoStoreDBImpl(final ProcessInfoLogLogger logger) {
        this.logger = Objects.requireNonNull(logger, "logger");
    }

    @Override
    public void put(final ProcessInfo info) throws IOException {
        try {
            new Visitor(info.getId()).visitProperties(info);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private class Visitor extends PropertyVisitorStringBase {
        private final ProcessUniqueId id;

        public Visitor(final ProcessUniqueId id) {
            this.id = Objects.requireNonNull(id, "id");
        }

        @Override
        public void visit(final String key, String value) {
            final int ix1 = key.indexOf('.');
            final String type1 = key.substring(0, ix1);
            final String remaining = key.substring(ix1 + 1);
            final int ix2 = remaining.indexOf('.');
            if (ix2 == -1) {
                try {
                    log(type1, remaining, value);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return;
            }
            final String type2 = remaining.substring(0, ix2);
            final String remaining2 = remaining.substring(ix2 + 1);
            try {
                log(type1 + "." + type2, remaining2, value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private void log(final String type, final String key, final String value)
            throws IOException {
            logger.log(id.value(), type, key, value);
        }
    }
}
