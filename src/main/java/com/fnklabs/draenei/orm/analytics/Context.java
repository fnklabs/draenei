package com.fnklabs.draenei.orm.analytics;

import org.jetbrains.annotations.NotNull;

public interface Context<Key, Value> {
    void emit(@NotNull Key key, @NotNull Value value);
}
