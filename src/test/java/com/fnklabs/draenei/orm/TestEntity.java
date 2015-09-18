package com.fnklabs.draenei.orm;

import com.fnklabs.draenei.orm.annotations.ClusteringKey;
import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.UUID;

@Table(name = "test")
public class TestEntity implements Cacheable {
    @ClusteringKey(order = 0)
    @Column(name = "id")
    private String id = UUID.randomUUID().toString();

    @ClusteringKey(order = 1)
    @Column(name = "id2")
    private int id2 = 1;

    @ClusteringKey(order = 2)
    @Column(name = "id3")
    private String id3 = UUID.randomUUID().toString();
    ;

    @ClusteringKey(order = 3)
    @Column(name = "id4")
    private String id4 = UUID.randomUUID().toString();

    private String cacheKey;


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getId2() {
        return id2;
    }

    public void setId2(int id2) {
        this.id2 = id2;
    }

    public String getId3() {
        return id3;
    }

    public void setId3(String id3) {
        this.id3 = id3;
    }

    public String getId4() {
        return id4;
    }

    public void setId4(String id4) {
        this.id4 = id4;
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
