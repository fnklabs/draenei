package com.fnklabs.draenei;

import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.PrimaryKey;

public class TestEntity {
    @PrimaryKey
    @Column
    private int id;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
