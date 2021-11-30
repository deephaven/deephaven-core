package io.deephaven.web.shared.data.treetable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * OpenAPI compatible analog to SmartKey, leaving out hashcode when serializing. This is an opaque object to client
 * code, but serialized using the stream to get the benefits of shared fields rather than simply byte[] encoding the
 * java serialized data.
 *
 * In order to serialize this, as Object[] is not serializable, the custom field serializer must descend into the array,
 * and check each value. If the value is a serializable type as is, simply write it, otherwise check for special cases
 * that we accept (StringSet, DateTime) and use appropriate wrappers.
 */
public class Key implements Serializable {
    public static Key root() {
        return new Key();
    }

    public static Key ofArray(Object[] array) {
        Key key = new Key();
        key.setArray(array);
        return key;
    }

    public static Key ofList(List<?> array) {
        Key key = new Key();
        key.setList(array.toArray());// deliberately to Object[]
        return key;
    }

    public static Key ofDateTime(long nanos) {
        Key key = new Key();
        key.setNanos(nanos);
        return key;
    }

    public static Key ofObject(Object serverKey) {
        Objects.requireNonNull(serverKey);
        Key key = new Key();
        key.setLeaf(serverKey);
        return key;
    }

    private transient Object leaf;
    private transient Long nanos;
    private transient Object[] array;
    private transient Object[] list;

    // indicate to gwt-rpc that any of these could be assigned when an Object is serialized
    private Boolean ignoredBoolean;
    private String ignoredString;
    private Long ignoredLong;
    private Integer ignoredInteger;
    private Double ignoredDouble;
    private Short ignoredShort;
    private Float ignoredFloat;
    private Character ignoredCharacter;
    private Byte ignoredByte;

    Key() {}

    public boolean isRoot() {
        return !isList() && !isArray() && !isLeaf() && !isDateTime();
    }

    public boolean isArray() {
        return array != null;
    }

    public boolean isList() {
        return list != null;
    }

    public boolean isLeaf() {
        return leaf != null;
    }

    public boolean isDateTime() {
        return nanos != null;
    }

    public Object[] getList() {
        return list;
    }

    void setList(Object[] list) {
        this.list = list;
    }

    public Object[] getArray() {
        return array;
    }

    void setArray(Object[] array) {
        this.array = array;
    }

    public Object getLeaf() {
        return leaf;
    }

    void setLeaf(Object leaf) {
        this.leaf = leaf;
    }

    public Long getNanos() {
        return nanos;
    }

    void setNanos(Long nanos) {
        this.nanos = nanos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final Key key = (Key) o;

        if (!Objects.equals(leaf, key.leaf))
            return false;
        if (!Objects.equals(nanos, key.nanos))
            return false;
        if (!Arrays.equals(array, key.array))
            return false;
        return Arrays.equals(list, key.list);
    }

    @Override
    public int hashCode() {
        int result = leaf == null ? 0 : leaf.hashCode();
        result = 31 * result + (nanos == null ? 0 : Long.hashCode(nanos));
        result = 31 * result + Arrays.hashCode(array);
        result = 31 * result + Arrays.hashCode(list);
        return result;
    }

    @Override
    public String toString() {
        if (isDateTime()) {
            return "DateTime " + getNanos();
        }
        if (isArray()) {
            return "Array " + Arrays.toString(getArray());
        }
        if (isList()) {
            return "List " + Arrays.toString(getList());
        }
        if (isLeaf()) {
            return "Value " + getLeaf();
        }
        return "Root";
    }
}
