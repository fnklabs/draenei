package com.fnklabs.draenei.orm;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;

/**
 * Entity Cacheable interface
 */
public interface Cacheable extends Serializable {
    /**
     * Get caching key
     *
     * @return Entity cache ID
     */
    @Nullable
    Long getCacheId();

    /**
     * Set entity cache id
     *
     * @param id Entity cache id
     */
    void setCacheId(@NotNull Long id);
}
