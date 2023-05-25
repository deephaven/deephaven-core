/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.qst.table.TableSchema.Visitor;

import java.util.Iterator;
import java.util.Objects;

/**
 * Provides a potentially descriptive label for the parents of a {@link TableSpec}.
 */
public class LinkDescriber extends TableVisitorGeneric {

    public interface LinkConsumer {
        void link(TableSpec table);

        void link(TableSpec table, int linkIndex);

        void link(TableSpec table, String linkLabel);
    }

    private final LinkConsumer consumer;

    public LinkDescriber(LinkConsumer consumer) {
        this.consumer = Objects.requireNonNull(consumer);
    }

    @Override
    public void accept(TableSpec t) {
        Iterator<TableSpec> it = ParentsVisitor.getParents(t).iterator();
        if (!it.hasNext()) {
            return;
        }
        TableSpec first = it.next();
        if (!it.hasNext()) {
            consumer.link(first);
            return;
        }
        consumer.link(first, 0);
        for (int i = 1; it.hasNext(); ++i) {
            consumer.link(it.next(), i);
        }
    }

    public void join(Join join) {
        consumer.link(join.left(), "left");
        consumer.link(join.right(), "right");
    }

    @Override
    public void visit(NaturalJoinTable naturalJoinTable) {
        join(naturalJoinTable);
    }

    @Override
    public void visit(ExactJoinTable exactJoinTable) {
        join(exactJoinTable);
    }

    @Override
    public void visit(JoinTable joinTable) {
        join(joinTable);
    }

    @Override
    public void visit(AsOfJoinTable aj) {
        join(aj);
    }

    @Override
    public void visit(ReverseAsOfJoinTable raj) {
        join(raj);
    }

    @Override
    public void visit(RangeJoinTable rangeJoinTable) {
        consumer.link(rangeJoinTable.left(), "left");
        consumer.link(rangeJoinTable.right(), "right");
    }

    @Override
    public void visit(WhereInTable whereInTable) {
        consumer.link(whereInTable.left(), "left");
        consumer.link(whereInTable.right(), "right");
    }

    @Override
    public void visit(SnapshotTable snapshotTable) {
        consumer.link(snapshotTable.base(), "base");
    }

    @Override
    public void visit(SnapshotWhenTable snapshotWhenTable) {
        consumer.link(snapshotWhenTable.base(), "base");
        consumer.link(snapshotWhenTable.trigger(), "trigger");
    }

    @Override
    public void visit(InputTable inputTable) {
        inputTable.schema().walk(new Visitor() {
            @Override
            public void visit(TableSpec spec) {
                consumer.link(spec, "definition");
            }

            @Override
            public void visit(TableHeader header) {
                // no links
            }
        });

    }
}
