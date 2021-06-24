package io.deephaven.db.v2.parquet;

class ColumnInstructions {
    private final String columnName;
    String parquetColumnName;
    private String codecName;
    private String codecArgs;

    public ColumnInstructions(final String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getParquetColumnName() {
        return parquetColumnName != null ? parquetColumnName : columnName;
    }
    public ColumnInstructions setParquetColumnName(final String parquetColumnName) {
        this.parquetColumnName = parquetColumnName;
        return this;
    }

    public String getCodecName() {
        return codecName;
    }
    public ColumnInstructions setCodecName(final String codecName) {
        this.codecName = codecName;
        return this;
    }

    public String getCodecArgs() {
        return codecArgs;
    }
    public ColumnInstructions setCodecArgs(final String codecArgs) {
        this.codecArgs = codecArgs;
        return this;
    }
}
