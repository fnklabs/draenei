package com.fnklabs.draenei.orm;

import org.jetbrains.annotations.NotNull;

/**
 * Cacheable DataProvider EventListener
 *
 * @param <T> DataProvider class type
 */
public interface EventListener<T> {
    /**
     * Called on after save new entry
     *
     * @param entry new entry
     */
    void onEntrySave(@NotNull T entry);

    /**
     * Called on after remove new entry
     *
     * @param entry new entry
     */
    void onEntryRemove(@NotNull T entry);
}
