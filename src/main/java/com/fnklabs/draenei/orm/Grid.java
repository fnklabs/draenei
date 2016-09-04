package com.fnklabs.draenei.orm;

import jdk.internal.org.objectweb.asm.tree.analysis.Value;
import org.apache.ignite.configuration.CacheConfiguration;

public interface Grid {
    public DataGrid<Key,Value> get(CacheConfiguration)
}
