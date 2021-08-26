package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.Lifecycle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.client.impl.TableHandle.UncheckedInterruptedException;
import io.deephaven.client.impl.TableHandle.UncheckedTableHandleException;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.TableAdapterResults;
import io.deephaven.qst.TableAdapterResults.GetOutput;
import io.deephaven.qst.TableCreationLabeledLogic;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreationLogic1Input;
import io.deephaven.qst.TableCreationLogic2Inputs;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.TableSpec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A table handle manager that executes requests serially.
 *
 * <p>
 * Serial execution is useful for initial development and debugging. There will be a server/client round-trip for each
 * table operation. Exceptions should have the exact line if there is an error in operation execution.
 */
final class TableHandleManagerSerial extends TableHandleManagerBase {

    public static TableHandleManagerSerial of(Session session) {
        return new TableHandleManagerSerial(session);
    }

    private TableHandleManagerSerial(Session session) {
        super(session, null);
    }

    private TableHandleManagerSerial(Session session, Lifecycle lifecycle) {
        super(session, lifecycle);
    }

    @Override
    public TableHandle execute(TableSpec table) throws TableHandleException, InterruptedException {
        final Tracker tracker = new Tracker();
        final TableHandleManager manager = new TableHandleManagerSerial(session, tracker);
        final TableAdapterResults<TableHandle, TableHandle> results;
        try {
            results = checked(() -> TableCreator.create(manager, i -> i, i -> i, table));
        } catch (Throwable t) {
            tracker.closeAllExceptAndRemoveAll(Collections.emptySet());
            throw t;
        }
        final TableHandle out = results.map().get(table).walk(new GetOutput<>()).out();
        tracker.closeAllExceptAndRemoveAll(Collections.singleton(out));
        return out;
    }

    @Override
    public List<TableHandle> execute(Iterable<TableSpec> tables) throws TableHandleException, InterruptedException {
        final Tracker tracker = new Tracker();
        final TableHandleManager manager = new TableHandleManagerSerial(session, tracker);
        final TableAdapterResults<TableHandle, TableHandle> results;
        try {
            results = checked(() -> TableCreator.create(manager, i -> i, i -> i, tables));
        } catch (Throwable t) {
            tracker.closeAllExceptAndRemoveAll(Collections.emptySet());
            throw t;
        }
        final List<TableHandle> newRefs = new ArrayList<>();
        for (TableSpec table : tables) {
            TableHandle handle = results.map().get(table).walk(new GetOutput<>()).out();
            // each needs to be a newRef because the caller may double up on the same TableSpec
            newRefs.add(handle.newRef());
        }
        tracker.closeAllExceptAndRemoveAll(new HashSet<>(newRefs));
        return newRefs;
    }

    @Override
    public TableHandle executeLogic(TableCreationLogic logic) throws TableHandleException, InterruptedException {
        final Tracker tracker = new Tracker();
        final TableHandleManager manager = new TableHandleManagerSerial(session, tracker);
        final TableHandle out;
        try {
            out = checked(() -> logic.create(manager));
        } catch (Throwable t) {
            tracker.closeAllExceptAndRemoveAll(Collections.emptySet());
            throw t;
        }
        tracker.closeAllExceptAndRemoveAll(Collections.singleton(out));
        return out;
    }

    @Override
    public List<TableHandle> executeLogic(Iterable<TableCreationLogic> logics)
            throws TableHandleException, InterruptedException {
        final Tracker tracker = new Tracker();
        final TableHandleManager manager = new TableHandleManagerSerial(session, tracker);
        final List<TableHandle> out = new ArrayList<>();
        try {
            for (TableCreationLogic logic : logics) {
                out.add(checked(() -> logic.create(manager)));
            }
        } catch (Throwable t) {
            tracker.closeAllExceptAndRemoveAll(Collections.emptySet());
            throw t;
        }
        tracker.closeAllExceptAndRemoveAll(new HashSet<>(out));
        return out;
    }

    @Override
    public LabeledValues<TableHandle> executeLogic(TableCreationLabeledLogic logic)
            throws TableHandleException, InterruptedException {
        final Tracker tracker = new Tracker();
        final TableHandleManager manager = new TableHandleManagerSerial(session, tracker);
        final LabeledValues<TableHandle> out;
        try {
            out = checked(() -> logic.create(manager));
        } catch (Throwable t) {
            tracker.closeAllExceptAndRemoveAll(Collections.emptySet());
            throw t;
        }
        tracker.closeAllExceptAndRemoveAll(out.valueSet());
        return out;
    }

    @Override
    public TableHandle executeInputs(TableCreationLogic1Input logic, TableHandle t1)
            throws TableHandleException, InterruptedException {
        return checked(() -> logic.create(t1));
    }

    @Override
    public TableHandle executeInputs(TableCreationLogic2Inputs logic, TableHandle t1, TableHandle t2)
            throws InterruptedException, TableHandleException {
        return checked(() -> logic.create(t1, t2));
    }

    interface Unchecked<T> {
        T run();
    }

    private static <T> T checked(Unchecked<T> uncheckedCode) throws InterruptedException, TableHandleException {
        try {
            return uncheckedCode.run();
        } catch (UncheckedInterruptedException e) {
            throw e.getCause();
        } catch (UncheckedTableHandleException e) {
            throw e.getCause();
        }
    }

    private static class Tracker implements Lifecycle {
        private Set<TableHandle> handles = new LinkedHashSet<>();

        @Override
        public synchronized void onInit(TableHandle handle) {
            if (handles == null) {
                return;
            }
            handles.add(handle);
        }

        @Override
        public synchronized void onRelease(TableHandle handle) {
            if (handles == null) {
                return;
            }
            handles.remove(handle);
        }

        public synchronized void closeAllExceptAndRemoveAll(Set<TableHandle> exceptions) {
            Iterator<TableHandle> it = handles.iterator();
            while (it.hasNext()) {
                TableHandle handle = it.next();

                // even if we aren't closing it, we don't want to keep a hard reference to it.
                it.remove();

                if (exceptions.contains(handle)) {
                    continue;
                }
                handle.close(true);
            }
            handles = null;
        }
    }

}
