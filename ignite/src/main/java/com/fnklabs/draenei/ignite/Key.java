package com.fnklabs.draenei.ignite;

import java.io.Serializable;
import java.util.Objects;

/**
 * {@link org.apache.ignite.IgniteCache} key
 */
public class Key implements Serializable {
    private final Object[] keys;

    public Key(Object... keys) {this.keys = keys;}

    public Object[] toArray() {
        return keys;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(keys);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Key) {
            return Objects.equals(this.keys, ((Key) obj).keys);
        }

        return false;
    }
}
