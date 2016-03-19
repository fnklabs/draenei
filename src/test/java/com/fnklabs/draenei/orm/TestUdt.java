package com.fnklabs.draenei.orm;

import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.UDT;
import com.google.common.base.MoreObjects;

import java.util.List;

@UDT(name = "test_udt")
public class TestUdt {

    @Column
    private String phone;

    @Column
    private String country;

    @Column
    private List<String> code;

    public TestUdt() {
    }

    public TestUdt(String phone, String country, List<String> code) {
        this.phone = phone;
        this.country = country;
        this.code = code;
    }

    public List<String> getCode() {
        return code;
    }

    public void setCode(List<String> code) {
        this.code = code;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("phone", getPhone())
                          .add("country", getCountry())
                          .add("code", getCode())
                          .toString();
    }
}
