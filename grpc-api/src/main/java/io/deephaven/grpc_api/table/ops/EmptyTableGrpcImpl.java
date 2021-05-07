package io.deephaven.grpc_api.table.ops;

import io.deephaven.base.verify.Assert;
import com.google.rpc.Code;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.web.shared.data.LocalDate;
import io.deephaven.web.shared.data.LocalTime;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.grpc.StatusRuntimeException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class EmptyTableGrpcImpl extends GrpcTableOperation<EmptyTableRequest> {

    @Inject()
    public EmptyTableGrpcImpl() {
        super(BatchTableRequest.Operation::getEmptyTable, EmptyTableRequest::getResultId);
    }

    @SuppressWarnings("rawtypes")
    private List<Class> getClassTypes(final EmptyTableRequest request) {
        final List<Class> types = new ArrayList<>();
        for (final String typeName : request.getColumnTypesList()) {
            try {
                types.add(ColumnTypes.mapColumnType(typeName));
            } catch (final IllegalArgumentException iae) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Error mapping column type " + typeName + ".");
            }
        }
        return types;
    }

    @Override
    public void validateRequest(final EmptyTableRequest request) throws StatusRuntimeException {
        //noinspection rawtypes
        final List<Class> types = getClassTypes(request);
        final List<String> names = request.getColumnNamesList();
        if (types.size() != names.size()) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Inconsistent number of column names and types.");
        }
    }

    @Override
    public Table create(final EmptyTableRequest request, final List<SessionState.ExportObject<Table>> sourceTables) {
        Assert.eq(sourceTables.size(), "sourceTables.size()", 0);
        //noinspection rawtypes
        final List<Class> types = getClassTypes(request);
        final List<String> names = request.getColumnNamesList();
        if (types.size() != names.size()) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Inconsistent number of column names and types.");
        }

        return TableTools.emptyTable(request.getSize(), new TableDefinition(types, names));
    }

    // TODO: this disappears if emptyTable does not allow type definition and we instead use arrow schema to pass that information around
    private static class ColumnTypes {
        public final static String BooleanColumn = Boolean.class.getCanonicalName();
        public final static String ByteColumn = "byte";
        public final static String CharColumn = "char";
        public final static String ShortColumn = "short";
        public final static String IntColumn = "int";
        public final static String LongColumn = "long";
        public final static String FloatColumn = "float";
        public final static String DoubleColumn = "double";
        public final static String StringColumn = String.class.getCanonicalName();
        public final static String BigDecimalColumn = java.math.BigDecimal.class.getCanonicalName();
        public final static String BigIntegerColumn = java.math.BigInteger.class.getCanonicalName();
        public final static String DBDateTimeColumn = io.deephaven.db.tables.utils.DBDateTime.class.getCanonicalName();
        public final static String LocalDateColumn = java.time.LocalDate.class.getCanonicalName();
        public final static String LocalTimeColumn = java.time.LocalTime.class.getCanonicalName();

        private final static Map<String, Class<?>> typeMap = new HashMap<>();

        static {
            typeMap.put(BooleanColumn, Boolean.class);
            typeMap.put(ByteColumn, Byte.TYPE);
            typeMap.put(CharColumn, Character.TYPE);
            typeMap.put(ShortColumn, Short.TYPE);
            typeMap.put(IntColumn, Integer.TYPE);
            typeMap.put(LongColumn, Long.TYPE);
            typeMap.put(FloatColumn, Float.TYPE);
            typeMap.put(DoubleColumn, Double.TYPE);
            typeMap.put(StringColumn, String.class);
            typeMap.put(BigDecimalColumn, BigDecimal.class);
            typeMap.put(BigIntegerColumn, BigInteger.class);
            typeMap.put(DBDateTimeColumn, DBDateTime.class);
            typeMap.put(LocalDateColumn, LocalDate.class);
            typeMap.put(LocalTimeColumn, LocalTime.class);
        }

        public static Class<?> mapColumnType(final String columnType) {
            final Class<?> type = typeMap.get(columnType);
            if (type == null) {
                throw new IllegalArgumentException("Unsupported column type: " + columnType);
            }
            return type;
        }
    }
}
