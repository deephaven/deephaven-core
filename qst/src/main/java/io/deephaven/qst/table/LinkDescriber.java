//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.qst.table.TableSchema.Visitor;

import java.util.Iterator;
import java.util.Objects;

/**
 * Provides a potentially descriptive label for the parents of a {@link TableSpec}.
 */
public class LinkDescriber extends TableVisitorGeneric {

    public interface LinkConsumer {
        void link(TableSpec parent);

        void link(TableSpec parent, int linkIndex);

        void link(TableSpec parent, String linkLabel);
    }

    private final LinkConsumer consumer;

    public LinkDescriber(LinkConsumer consumer) {
        this.consumer = Objects.requireNonNull(consumer);
    }

    @Override
    public Void accept(TableSpec t) {
        Iterator<TableSpec> it = ParentsVisitor.getParents(t).iterator();
        if (!it.hasNext()) {
            return null;
        }
        TableSpec first = it.next();
        if (!it.hasNext()) {
            consumer.link(first);
            return null;
        }
        consumer.link(first, 0);
        for (int i = 1; it.hasNext(); ++i) {
            consumer.link(it.next(), i);
        }
        return null;
    }

    public void join(Join join) {
        consumer.link(join.left(), "left");
        consumer.link(join.right(), "right");
    }

    @Override
    public Void visit(NaturalJoinTable naturalJoinTable) {
        join(naturalJoinTable);
        return null;
    }

    @Override
    public Void visit(ExactJoinTable exactJoinTable) {
        join(exactJoinTable);
        return null;
    }

    @Override
    public Void visit(JoinTable joinTable) {
        join(joinTable);
        return null;
    }

    @Override
    public Void visit(AsOfJoinTable aj) {
        join(aj);
        return null;
    }

    @Override
    public Void visit(RangeJoinTable rangeJoinTable) {
        consumer.link(rangeJoinTable.left(), "left");
        consumer.link(rangeJoinTable.right(), "right");
        return null;
    }

    @Override
    public Void visit(WhereInTable whereInTable) {
        consumer.link(whereInTable.left(), "left");
        consumer.link(whereInTable.right(), "right");
        return null;
    }

    @Override
    public Void visit(SnapshotTable snapshotTable) {
        consumer.link(snapshotTable.base(), "base");
        return null;
    }

    @Override
    public Void visit(SnapshotWhenTable snapshotWhenTable) {
        consumer.link(snapshotWhenTable.base(), "base");
        consumer.link(snapshotWhenTable.trigger(), "trigger");
        return null;
    }

    @Override
    public Void visit(InputTable inputTable) {
        return inputTable.schema().walk(new Visitor<Void>() {
            @Override
            public Void visit(TableSpec spec) {
                consumer.link(spec, "definition");
                return null;
            }

            @Override
            public Void visit(TableHeader header) {
                // no links
                return null;
            }
        });
    }
}
