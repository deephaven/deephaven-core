package io.deephaven.properties;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;

/**
 * A property set represents a set of property keys and values. The key is a String type, and the value can be of type
 * int, long, boolean, or String. (Note: we may use a stronger type in the future for a key, and may expand the types
 * that a value can be.)
 *
 * <p>
 * A property set interface present read-only access to the keys and values via {@link #traverse(PropertyVisitor)}.
 *
 * @see PropertyVisitor
 */
public interface PropertySet {
    static PropertySet of(Properties properties) {
        return new PropertiesImpl(properties);
    }

    static PropertySet of(Map<String, String> map) {
        return new StringMapImpl(map);
    }

    static PropertySet of(List<String> list) {
        return new StringListImpl(list);
    }

    /**
     * Traverse this property set and output the property key/values to the given visitor.
     * <p>
     * Callers should typically prefer to call {@link PropertyVisitor#visitProperties(PropertySet)}, as the inversion of
     * logic allows the visitor (the more stateful object) to potentially perform initialization logic and traverse more
     * efficiently.
     *
     * @param visitor the visitor
     * @see PropertyVisitor#visitProperties(PropertySet)
     */
    void traverse(PropertyVisitor visitor);

    class StringMapImpl implements PropertySet {
        private final Map<String, String> map;

        StringMapImpl(Map<String, String> map) {
            this.map = Objects.requireNonNull(map, "map");
        }

        @Override
        public void traverse(PropertyVisitor visitor) {
            for (Entry<String, String> entry : map.entrySet()) {
                visitor.visit(entry.getKey(), entry.getValue());
            }
        }
    }

    class StringListImpl implements PropertySet {
        private final List<String> list;

        StringListImpl(List<String> list) {
            this.list = Objects.requireNonNull(list, "list");
        }

        @Override
        public void traverse(PropertyVisitor visitor) {
            final int len = list.size();
            visitor.visit("len", len);
            for (int i = 0; i < len; i++) {
                visitor.visit(Integer.toString(i), list.get(i));
            }
        }
    }

    class PropertiesImpl implements PropertySet {
        private final Properties properties;

        PropertiesImpl(Properties properties) {
            this.properties = Objects.requireNonNull(properties, "properties");
        }

        @Override
        public void traverse(PropertyVisitor visitor) {
            for (Entry<Object, Object> entry : this.properties.entrySet()) {
                visitor.visit((String) entry.getKey(), (String) entry.getValue());
            }
        }
    }
}
