package com.fnklabs.draenei.orm;

public interface EventListener<T> {
    void onEntrySave(T entry);

    void onEntryRemove(T entry);
}
