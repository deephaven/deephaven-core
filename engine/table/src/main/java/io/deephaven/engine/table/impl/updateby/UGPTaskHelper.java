package io.deephaven.engine.table.impl.updateby;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.updategraph.AbstractNotification;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class UGPTaskHelper {

    public static abstract class UGPTaskHelperNotification<T> extends AbstractNotification {
        protected final ArrayList<T> dataList;
        protected final AtomicInteger nextTask;
        protected final AtomicInteger tasksCompleted;
        protected final WorkerAction<T> workerAction;
        protected final Runnable completeAction;

        @FunctionalInterface
        public interface WorkerAction<T> {
            void run(T data, Runnable complete);
        }

        private UGPTaskHelperNotification(ArrayList<T> dataList, AtomicInteger nextTask, AtomicInteger tasksCompleted, WorkerAction<T> workerAction, Runnable completeAction) {
            super(true);
            this.dataList = dataList;
            this.nextTask = nextTask;
            this.tasksCompleted = tasksCompleted;
            this.workerAction = workerAction;
            this.completeAction = completeAction;
        }

        @Override
        public boolean canExecute(long step) {
            return true;
        }

        @Override
        public ExecutionContext getExecutionContext() {
            return null;
        }

        @Override
        public void run() {
            // do the work
            int taskIndex = nextTask.getAndIncrement();
            workerAction.run(dataList.get(taskIndex), this::complete);
        }

        protected abstract void complete();
    }

    private static class UGPTaskHelperSerialNotification<T> extends UGPTaskHelperNotification {
        private UGPTaskHelperSerialNotification(ArrayList<T> dataList, AtomicInteger nextTask, AtomicInteger tasksCompleted, WorkerAction workerAction, Runnable completeAction) {
            super(dataList, nextTask, tasksCompleted, workerAction, completeAction);
        }

        protected void complete() {
            // serially call the next task
            int completedCount = tasksCompleted.incrementAndGet();

            if (completedCount == dataList.size()) {
                completeAction.run();
            } else {
                // this will recurse N times, is it better to create a new UGP notification and schedule it?
                run();
            }
        }
    }

    private static class UGPTaskHelperParallelNotification<T> extends UGPTaskHelperNotification {
        private UGPTaskHelperParallelNotification(ArrayList<T> dataList, AtomicInteger nextTask, AtomicInteger tasksCompleted, WorkerAction workerAction, Runnable completeAction) {
            super(dataList, nextTask, tasksCompleted, workerAction, completeAction);
        }

        protected void complete() {
            // call the complete when all the tasks are done
            int completedCount = tasksCompleted.incrementAndGet();

            if (completedCount == dataList.size()) {
                completeAction.run();
            }
        }
    }

    public static <T> void ExecuteSerial(ArrayList<T> dataObjects, UGPTaskHelperNotification.WorkerAction<T> workerAction, Runnable completedAction) {
        final AtomicInteger completed = new AtomicInteger(0);
        final AtomicInteger nextTask = new AtomicInteger(0);

        // create and schedule a single notification, will auto serialize
        UGPTaskHelperSerialNotification notification = new UGPTaskHelperSerialNotification<T>(dataObjects, nextTask, completed, workerAction, completedAction);
        UpdateGraphProcessor.DEFAULT.addNotification(notification);
    }

    public static <T>void ExecuteParallel(ArrayList<T> dataObjects, UGPTaskHelperNotification.WorkerAction<T> workerAction, Runnable completedAction) {
        final AtomicInteger completed = new AtomicInteger(0);
        final AtomicInteger nextTask = new AtomicInteger(0);

        // create and schedule a notification for each object
        for (int ii = 0; ii < dataObjects.size(); ii++) {
            UGPTaskHelperParallelNotification notification = new UGPTaskHelperParallelNotification<T>(dataObjects, nextTask, completed, workerAction, completedAction);
            UpdateGraphProcessor.DEFAULT.addNotification(notification);
        }
    }
}
