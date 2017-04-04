package com.fnklabs.draenei.ignite;

import java.io.Serializable;

/**
 * {@link org.apache.ignite.IgniteCache} key
 */
public class Key implements Serializable {
    private final Object[] keys;

    public Key(Object... keys) {this.keys = keys;}

    public Object[] toArray() {
        return keys;
    }
}
