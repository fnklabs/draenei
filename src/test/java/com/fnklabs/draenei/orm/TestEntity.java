package com.fnklabs.draenei.orm;

import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.PrimaryKey;
import com.fnklabs.draenei.orm.annotations.Table;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Table(name = "test")
public class TestEntity implements Serializable {
    @PrimaryKey(order = 0)
    @Column(name = "id")
    private UUID id = UUID.randomUUID();

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getId());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TestEntity) {
            return Objects.equals(getId(), ((TestEntity) obj).getId());
        }

        return false;
    }
}
