package io.deephaven.web.shared.data.treetable;

import java.io.Serializable;
import java.util.Arrays;

public class TableDetails implements Serializable {
    private Key key;

    private Key[] children;

    public Key getKey() {
        return key;
    }

    public void setKey(Key key) {
        this.key = key;
    }

    public Key[] getChildren() {
        return children;
    }

    public void setChildren(Key[] children) {
        this.children = children;
    }

    @Override
    public String toString() {
        return "TableDetails{" +
                ", key=" + key +
                ", children=" + Arrays.toString(children) +
                '}';
    }
}
