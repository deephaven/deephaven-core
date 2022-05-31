package io.deephaven.client;

import io.deephaven.api.TableOperations;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreationLogic1Input;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.TableHeader;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static io.deephaven.client.AnnotatedTable.ofDynamic;
import static io.deephaven.client.AnnotatedTable.ofStatic;

public class AnnotatedTables {

    public static final TableHeader TIME = TableHeader.of(ColumnHeader.ofInstant("Timestamp"));

    public static final TableHeader INDEX = TableHeader.of(ColumnHeader.ofLong("I"));

    public static final TableHeader TIME_INDEX =
            TableHeader.of(ColumnHeader.ofInstant("Timestamp"), ColumnHeader.ofLong("I"));

    public static final AnnotatedTable EMPTY_0 = ofStatic(TableHeader.empty(), AnnotatedTables::empty0, 0);
    public static final AnnotatedTable EMPTY_1 = ofStatic(TableHeader.empty(), AnnotatedTables::empty1, 1);
    public static final AnnotatedTable EMPTY_2 = ofStatic(TableHeader.empty(), AnnotatedTables::empty2, 2);

    public static final AnnotatedTable TIME_1 = ofDynamic(TIME, AnnotatedTables::time1);
    public static final AnnotatedTable TIME_2 = ofDynamic(TIME, AnnotatedTables::time2);

    public static final AnnotatedTable MERGE_STATIC = ofStatic(TableHeader.empty(), AnnotatedTables::mergeStatic, 3);
    public static final AnnotatedTable MERGE = ofDynamic(TIME, AnnotatedTables::merge);

    public static final AnnotatedTable VIEW_STATIC = ofStatic(INDEX, AnnotatedTables::viewStatic, 1);
    public static final AnnotatedTable VIEW = ofDynamic(INDEX, AnnotatedTables::view);

    public static final AnnotatedTable UPDATE_VIEW_STATIC = ofStatic(INDEX, AnnotatedTables::updateViewStatic, 1);
    public static final AnnotatedTable UPDATE_VIEW = ofDynamic(TIME_INDEX, AnnotatedTables::updateView);

    public static final AnnotatedTable UPDATE_STATIC = ofStatic(INDEX, AnnotatedTables::updateStatic, 1);
    public static final AnnotatedTable UPDATE = ofDynamic(TIME_INDEX, AnnotatedTables::update);

    public static final AnnotatedTable SELECT_STATIC = ofStatic(INDEX, AnnotatedTables::selectStatic, 1);
    public static final AnnotatedTable SELECT = ofDynamic(INDEX, AnnotatedTables::select);

    public static final AnnotatedTable HEAD_STATIC = ofStatic(TableHeader.empty(), AnnotatedTables::headStatic, 1);
    public static final AnnotatedTable HEAD = ofDynamic(TIME, AnnotatedTables::head);

    public static final AnnotatedTable TAIL_STATIC = ofStatic(TableHeader.empty(), AnnotatedTables::tailStatic, 1);
    public static final AnnotatedTable TAIL = ofDynamic(TIME, AnnotatedTables::tail);

    public static final Adapter REVERSE_ADAPTER = AnnotatedTables::reverse;

    public static final Adapter HEAD_0_ADAPTER = in -> head(in, 0);

    public static final Adapter HEAD_1_ADAPTER = in -> head(in, 1);

    public static final Adapter HEAD_2_ADAPTER = in -> head(in, 2);

    public static final Adapter TAIL_0_ADAPTER = in -> tail(in, 0);

    public static final Adapter TAIL_1_ADAPTER = in -> tail(in, 1);

    public static final Adapter TAIL_2_ADAPTER = in -> tail(in, 2);

    public static final Adapter MERGE_2 = in -> {
        final TableCreationLogic merge2 = new TableCreationLogic() {
            @Override
            public <T extends TableOperations<T, T>> T create(TableCreator<T> creation) {
                final T t = in.logic().create(creation);
                return creation.merge(t, t);
            }
        };
        if (in.isStatic()) {
            return AnnotatedTable.ofStatic(in.header(), merge2, in.size() * 2);
        } else {
            return AnnotatedTable.ofDynamic(in.header(), merge2);
        }
    };

    public static final Adapter MERGE_3 = in -> {
        final TableCreationLogic merge3 = new TableCreationLogic() {
            @Override
            public <T extends TableOperations<T, T>> T create(TableCreator<T> creation) {
                final T t = in.logic().create(creation);
                return creation.merge(t, t, t);
            }
        };
        if (in.isStatic()) {
            return AnnotatedTable.ofStatic(in.header(), merge3, in.size() * 3);
        } else {
            return AnnotatedTable.ofDynamic(in.header(), merge3);
        }
    };

    public static Map<String, AnnotatedTable> explicitlyAnnotatedTables() {
        // I wish there were any easy way to get variable names or lambda function names from java :(
        final Map<String, AnnotatedTable> tables = new LinkedHashMap<>();
        tables.put("EMPTY_0", EMPTY_0);
        tables.put("EMPTY_1", EMPTY_1);
        tables.put("EMPTY_2", EMPTY_2);
        tables.put("TIME_1", TIME_1);
        tables.put("TIME_2", TIME_2);
        tables.put("MERGE_STATIC", MERGE_STATIC);
        tables.put("MERGE", MERGE);
        tables.put("VIEW_STATIC", VIEW_STATIC);
        tables.put("VIEW", VIEW);
        tables.put("UPDATE_VIEW_STATIC", UPDATE_VIEW_STATIC);
        tables.put("UPDATE_VIEW", UPDATE_VIEW);
        tables.put("UPDATE_STATIC", UPDATE_STATIC);
        tables.put("UPDATE", UPDATE);
        tables.put("SELECT_STATIC", SELECT_STATIC);
        tables.put("SELECT", SELECT);
        tables.put("HEAD_STATIC", HEAD_STATIC);
        tables.put("HEAD", HEAD);
        tables.put("TAIL_STATIC", TAIL_STATIC);
        tables.put("TAIL", TAIL);
        return tables;
    }

    public static Map<String, Adapter> adapters() {
        final Map<String, Adapter> map = new LinkedHashMap<>();
        map.put("REVERSE", REVERSE_ADAPTER);
        map.put("HEAD_0", HEAD_0_ADAPTER);
        map.put("HEAD_1", HEAD_1_ADAPTER);
        map.put("HEAD_2", HEAD_2_ADAPTER);
        map.put("TAIL_0", TAIL_0_ADAPTER);
        map.put("TAIL_1", TAIL_1_ADAPTER);
        map.put("TAIL_2", TAIL_2_ADAPTER);
        map.put("MERGE_2", MERGE_2);
        map.put("MERGE_3", MERGE_3);
        return map;
    }

    public static Map<String, Adapter> headerPreservingAggAllBy() {
        final Map<String, Adapter> map = new LinkedHashMap<>();
        map.put("AGGALL_BY_FIRST", a -> headerPreservingAgg(a, AggSpec.first()));
        map.put("AGGALL_BY_LAST", a -> headerPreservingAgg(a, AggSpec.last()));
        map.put("AGGALL_BY_MAX", a -> headerPreservingAgg(a, AggSpec.max()));
        map.put("AGGALL_BY_MIN", a -> headerPreservingAgg(a, AggSpec.min()));
        return map;
    }

    public static Map<String, AnnotatedTable> annotatedTables() {
        final Map<String, AnnotatedTable> explicit = explicitlyAnnotatedTables();

        // Copy the explicitly annotated tables specs *as-is*.
        final Map<String, AnnotatedTable> out = new LinkedHashMap<>(explicit);

        // Create "double-dip" derivative specs
        addDoubleDips(explicit, out);

        // Commented out, excessive for normal unit testing.
        // Potentially useful when adding new operations for deeper testing.
        // Create "triple-dip" derivative specs
        // addTripleDips(explicit, out);

        return out;
    }

    private static void addDoubleDips(Map<String, AnnotatedTable> explicit, Map<String, AnnotatedTable> out) {
        for (Entry<String, AnnotatedTable> e : explicit.entrySet()) {
            final String key = e.getKey();
            final AnnotatedTable value = e.getValue();
            // Apply all of our adapters once to create a derivative table spec
            for (Entry<String, Adapter> a1 : adapters().entrySet()) {
                final String doubleKey = key + " + " + a1.getKey();
                final AnnotatedTable derivativeSpec = a1.getValue().apply(value);
                out.put(doubleKey, derivativeSpec);
            }

            if (value.header().numColumns() > 0) {
                for (Entry<String, Adapter> a1 : headerPreservingAggAllBy().entrySet()) {
                    final String doubleKey = key + " + " + a1.getKey();
                    final AnnotatedTable derivativeSpec = a1.getValue().apply(value);
                    out.put(doubleKey, derivativeSpec);
                }
            }
        }
    }

    private static void addTripleDips(Map<String, AnnotatedTable> explicit, Map<String, AnnotatedTable> out) {
        for (Entry<String, AnnotatedTable> e : explicit.entrySet()) {
            final String key = e.getKey();
            final AnnotatedTable value = e.getValue();
            // Apply all of our adapters twice to create a derivative table spec
            for (Entry<String, Adapter> a1 : adapters().entrySet()) {
                for (Entry<String, Adapter> a2 : adapters().entrySet()) {
                    final String tripleKey = key + " + " + a1.getKey() + " + " + a2.getKey();
                    final AnnotatedTable derivativeSpec = a2.getValue().apply(a1.getValue().apply(value));
                    out.put(tripleKey, derivativeSpec);
                }
            }
        }
    }

    public static <T extends TableOperations<T, T>> T i32768(TableCreator<T> c) {
        return c.emptyTable(32768).view("I=i");
    }

    public static <T extends TableOperations<T, T>> T empty0(TableCreator<T> c) {
        return c.emptyTable(0);
    }

    public static <T extends TableOperations<T, T>> T empty1(TableCreator<T> c) {
        return c.emptyTable(1);
    }

    public static <T extends TableOperations<T, T>> T empty2(TableCreator<T> c) {
        return c.emptyTable(2);
    }

    public static <T extends TableOperations<T, T>> T time1(TableCreator<T> c) {
        return c.timeTable(Duration.ofSeconds(1));
    }

    public static <T extends TableOperations<T, T>> T time2(TableCreator<T> c) {
        return c.timeTable(Duration.ofSeconds(2));
    }

    public static <T extends TableOperations<T, T>> T mergeStatic(TableCreator<T> c) {
        return c.merge(empty1(c), empty2(c));
    }

    public static <T extends TableOperations<T, T>> T merge(TableCreator<T> c) {
        return c.merge(time1(c), time2(c));
    }

    public static <T extends TableOperations<T, T>> T viewStatic(TableCreator<T> c) {
        return empty1(c).view("I=ii");
    }

    public static <T extends TableOperations<T, T>> T updateStatic(TableCreator<T> c) {
        return empty1(c).update("I=ii");
    }

    public static <T extends TableOperations<T, T>> T updateViewStatic(TableCreator<T> c) {
        return empty1(c).updateView("I=ii");
    }

    public static <T extends TableOperations<T, T>> T selectStatic(TableCreator<T> c) {
        return empty1(c).select("I=ii");
    }

    public static <T extends TableOperations<T, T>> T view(TableCreator<T> c) {
        return time1(c).view("I=ii");
    }

    public static <T extends TableOperations<T, T>> T update(TableCreator<T> c) {
        return time1(c).update("I=ii");
    }

    public static <T extends TableOperations<T, T>> T updateView(TableCreator<T> c) {
        return time1(c).updateView("I=ii");
    }

    public static <T extends TableOperations<T, T>> T select(TableCreator<T> c) {
        return time1(c).select("I=ii");
    }

    public static <T extends TableOperations<T, T>> T headStatic(TableCreator<T> c) {
        return empty2(c).head(1);
    }

    public static <T extends TableOperations<T, T>> T head(TableCreator<T> c) {
        return time1(c).head(1);
    }

    public static <T extends TableOperations<T, T>> T tailStatic(TableCreator<T> c) {
        return empty2(c).tail(1);
    }

    public static <T extends TableOperations<T, T>> T tail(TableCreator<T> c) {
        return time1(c).tail(1);
    }

    public interface Adapter {
        AnnotatedTable apply(AnnotatedTable in);
    }

    public static AnnotatedTable reverse(AnnotatedTable in) {
        return new AnnotatedTable(in.header(), in.logic().andThen(TableOperations::reverse), in.isStatic(), in.size());
    }

    public static AnnotatedTable headerPreservingAgg(AnnotatedTable in, AggSpec spec) {
        return new AnnotatedTable(in.header(), in.logic().andThen(aggAllBy(spec)), in.isStatic(),
                Math.min(in.size(), 1));
    }

    private static TableCreationLogic1Input aggAllBy(AggSpec spec) {
        return new TableCreationLogic1Input() {
            @Override
            public <T extends TableOperations<T, T>> T create(T t1) {
                return t1.aggAllBy(spec);
            }
        };
    }


    public static AnnotatedTable head(AnnotatedTable in, int size) {
        final TableCreationLogic1Input op = new TableCreationLogic1Input() {
            @Override
            public <T extends TableOperations<T, T>> T create(T t1) {
                return t1.head(size);
            }
        };
        final TableCreationLogic headLogic = in.logic().andThen(op);
        if (in.isStatic() || size == 0) {
            return AnnotatedTable.ofStatic(in.header(), headLogic, Math.min(in.size(), size));
        } else {
            return AnnotatedTable.ofDynamic(in.header(), headLogic);
        }
    }

    public static AnnotatedTable tail(AnnotatedTable in, int size) {
        final TableCreationLogic1Input op = new TableCreationLogic1Input() {
            @Override
            public <T extends TableOperations<T, T>> T create(T t1) {
                return t1.tail(size);
            }
        };
        final TableCreationLogic tailLogic = in.logic().andThen(op);
        if (in.isStatic() || size == 0) {
            return AnnotatedTable.ofStatic(in.header(), tailLogic, Math.min(in.size(), size));
        } else {
            return AnnotatedTable.ofDynamic(in.header(), tailLogic);
        }
    }
}
