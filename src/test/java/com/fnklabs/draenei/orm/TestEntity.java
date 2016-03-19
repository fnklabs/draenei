package com.fnklabs.draenei.orm;

import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.PrimaryKey;
import com.fnklabs.draenei.orm.annotations.Table;
import com.fnklabs.draenei.orm.annotations.UDTColumn;
import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Table(name = "test")
public class TestEntity implements Serializable {
    @PrimaryKey(order = 0)
    @Column(name = "id")
    private UUID id = UUID.randomUUID();

    @Column(name = "address")
    @UDTColumn(udtType = TestUdt.class)
    private TestUdt testUdt;

    @Column(name = "address_2")
    @UDTColumn(udtType = TestUdt.class)
    private List<TestUdt> testUdtList = new ArrayList<>();

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

    public TestUdt getTestUdt() {
        return testUdt;
    }

    public void setTestUdt(TestUdt testUdt) {
        this.testUdt = testUdt;
    }

    public List<TestUdt> getTestUdtList() {
        return testUdtList;
    }

    public void setTestUdtList(List<TestUdt> testUdtList) {
        this.testUdtList = testUdtList;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("id", getId())
                          .add("address", getTestUdt())
                          .add("address_1", getTestUdtList())
                          .toString();
    }
}
