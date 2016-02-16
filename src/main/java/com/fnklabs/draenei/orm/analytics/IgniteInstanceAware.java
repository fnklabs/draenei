package com.fnklabs.draenei.orm.analytics;

import org.apache.ignite.Ignite;
import org.jetbrains.annotations.NotNull;

public interface IgniteInstanceAware {
    void setIgnite(@NotNull Ignite ignite);
}
