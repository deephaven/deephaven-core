package io.deephaven.client;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreationLogic1Input;
import io.deephaven.qst.TableCreator;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static io.deephaven.client.AnnotatedTable.ofDynamic;
import static io.deephaven.client.AnnotatedTable.ofStatic;

public class AnnotatedTables {

    public static final AnnotatedTable EMPTY_0 = ofStatic(AnnotatedTables::empty0, 0);
    public static final AnnotatedTable EMPTY_1 = ofStatic(AnnotatedTables::empty1, 1);
    public static final AnnotatedTable EMPTY_2 = ofStatic(AnnotatedTables::empty2, 2);

    public static final AnnotatedTable TIME_1 = ofDynamic(AnnotatedTables::time1);
    public static final AnnotatedTable TIME_2 = ofDynamic(AnnotatedTables::time2);

    public static final AnnotatedTable MERGE_STATIC = ofStatic(AnnotatedTables::mergeStatic, 3);
    public static final AnnotatedTable MERGE = ofDynamic(AnnotatedTables::merge);

    public static final AnnotatedTable VIEW_STATIC = ofStatic(AnnotatedTables::viewStatic, 1);
    public static final AnnotatedTable VIEW = ofDynamic(AnnotatedTables::view);

    public static final AnnotatedTable UPDATE_VIEW_STATIC = ofStatic(AnnotatedTables::updateViewStatic, 1);
    public static final AnnotatedTable UPDATE_VIEW = ofDynamic(AnnotatedTables::updateView);

    public static final AnnotatedTable UPDATE_STATIC = ofStatic(AnnotatedTables::updateStatic, 1);
    public static final AnnotatedTable UPDATE = ofDynamic(AnnotatedTables::update);

    public static final AnnotatedTable SELECT_STATIC = ofStatic(AnnotatedTables::selectStatic, 1);
    public static final AnnotatedTable SELECT = ofDynamic(AnnotatedTables::select);

    public static final AnnotatedTable HEAD_STATIC = ofStatic(AnnotatedTables::headStatic, 1);
    public static final AnnotatedTable HEAD = ofDynamic(AnnotatedTables::head);

    public static final AnnotatedTable TAIL_STATIC = ofStatic(AnnotatedTables::tailStatic, 1);
    public static final AnnotatedTable TAIL = ofDynamic(AnnotatedTables::tail);

    public static final Adapter REVERSE_ADAPTER = AnnotatedTables::reverse;

    public static final Adapter HEAD_0_ADAPTER = in -> head(in, 0);

    public static final Adapter HEAD_1_ADAPTER = in -> head(in, 1);

    public static final Adapter HEAD_2_ADAPTER = in -> head(in, 2);

    public static final Adapter TAIL_0_ADAPTER = in -> tail(in, 0);

    public static final Adapter TAIL_1_ADAPTER = in -> tail(in, 1);

    public static final Adapter TAIL_2_ADAPTER = in -> tail(in, 2);

    public static final Adapter BY_ADAPTER = AnnotatedTables::by;

    public static final Adapter MERGE_2 = in -> {
        final TableCreationLogic merge2 = new TableCreationLogic() {
            @Override
            public <T extends TableOperations<T, T>> T create(TableCreator<T> creation) {
                final T t = in.logic().create(creation);
                return creation.merge(t, t);
            }
        };
        if (in.isStatic()) {
            return AnnotatedTable.ofStatic(merge2, in.size() * 2);
        } else {
            return AnnotatedTable.ofDynamic(merge2);
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
            return AnnotatedTable.ofStatic(merge3, in.size() * 3);
        } else {
            return AnnotatedTable.ofDynamic(merge3);
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
        map.put("BY", BY_ADAPTER);
        map.put("MERGE_2", MERGE_2);
        map.put("MERGE_3", MERGE_3);
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
        return empty1(c).view("I=i");
    }

    public static <T extends TableOperations<T, T>> T updateStatic(TableCreator<T> c) {
        return empty1(c).update("I=i");
    }

    public static <T extends TableOperations<T, T>> T updateViewStatic(TableCreator<T> c) {
        return empty1(c).updateView("I=i");
    }

    public static <T extends TableOperations<T, T>> T selectStatic(TableCreator<T> c) {
        return empty1(c).select("I=i");
    }

    public static <T extends TableOperations<T, T>> T view(TableCreator<T> c) {
        return time1(c).view("I=i");
    }

    public static <T extends TableOperations<T, T>> T update(TableCreator<T> c) {
        return time1(c).update("I=i");
    }

    public static <T extends TableOperations<T, T>> T updateView(TableCreator<T> c) {
        return time1(c).updateView("I=i");
    }

    public static <T extends TableOperations<T, T>> T select(TableCreator<T> c) {
        return time1(c).select("I=i");
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
        return new AnnotatedTable(in.logic().andThen(TableOperations::reverse), in.isStatic(), in.size());
    }

    public static AnnotatedTable by(AnnotatedTable in) {
        return new AnnotatedTable(in.logic().andThen(TableOperations::by), in.isStatic(), Math.min(in.size(), 1));
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
            return AnnotatedTable.ofStatic(headLogic, Math.min(in.size(), size));
        } else {
            return AnnotatedTable.ofDynamic(headLogic);
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
            return AnnotatedTable.ofStatic(tailLogic, Math.min(in.size(), size));
        } else {
            return AnnotatedTable.ofDynamic(tailLogic);
        }
    }
}
