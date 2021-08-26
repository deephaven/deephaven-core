package io.deephaven.web.client.api.widget.plot;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.SourceDescriptor;
import io.deephaven.web.client.api.JsTable;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;

import java.util.Map;

public class SeriesDataSource {
    private final JsAxis axis;
    private final SourceDescriptor sourceDescriptor;
    private String columnType;

    public SeriesDataSource(JsAxis axis, SourceDescriptor type) {
        this.axis = axis;
        this.sourceDescriptor = type;
    }

    public void initColumnType(Map<Integer, JsTable> tables) {
        if (sourceDescriptor.getTableId() != -1) {
            columnType =
                    tables.get(sourceDescriptor.getTableId()).findColumn(sourceDescriptor.getColumnName()).getType();
        } else if (sourceDescriptor.getTableMapId() != -1) {
            columnType = sourceDescriptor.getColumnType();
        } else {
            throw new SeriesDataSourceException(this, "No table available for source");
        }
    }

    @JsProperty
    public JsAxis getAxis() {
        return axis;
    }

    @JsProperty
    @SuppressWarnings("unusable-by-js")
    public int getType() {
        return sourceDescriptor.getType();
    }

    @JsProperty
    public String getColumnType() {
        return columnType;
    }

    @JsIgnore
    public SourceDescriptor getDescriptor() {
        return sourceDescriptor;
    }

    public class SeriesDataSourceException extends RuntimeException {
        private SeriesDataSource source;

        SeriesDataSourceException(SeriesDataSource source, String message) {
            super(message);

            this.source = source;
        }

        @JsProperty
        public SeriesDataSource getSource() {
            return source;
        }

        @Override
        @JsProperty
        public String getMessage() {
            return super.getMessage();
        }
    }
}
