package com.fnklabs.draenei.orm;

import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.PrimaryKey;
import com.fnklabs.draenei.orm.annotations.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

@Table(name = "test")
public class TestEntity implements Cacheable {
    @PrimaryKey(order = 0)
    @Column(name = "id")
    private UUID id = UUID.randomUUID();

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    @Nullable
    @Override
    public Long getCacheKey() {
        return null;
    }

    @Override
    public void setCacheKey(@NotNull Long id) {

    }
}
