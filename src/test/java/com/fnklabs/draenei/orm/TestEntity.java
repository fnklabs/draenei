package com.fnklabs.draenei.orm;

import com.fnklabs.draenei.orm.annotations.*;
import com.fnklabs.draenei.orm.annotations.Collection;
import com.fnklabs.draenei.orm.annotations.Map;
import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.*;

@Table(name = "test_entity")
public class TestEntity implements Serializable {
    @PrimaryKey(order = 0)
    @Column(name = "id")
    private UUID id = UUID.randomUUID();

    @Column
    private String username;

    @Column
    @Enumerated(enumType = Role.class)
    private Role role;

    @Column
    @Collection(elementType = Role.class)
    private Set<Role> roles;

    @Column(name = "roles_map")
    @Map(elementKeyType = Role.class, elementValueType = Role.class)
    private java.util.Map<Role, Role> rolesMap;


    public java.util.Map<Role, Role> getRolesMap() {
        return rolesMap;
    }

    public void setRolesMap(java.util.Map<Role, Role> rolesMap) {
        this.rolesMap = rolesMap;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public Set<Role> getRoles() {
        return roles;
    }

    public void setRoles(Set<Role> roles) {
        this.roles = roles;
    }

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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("id", getId())
                          .toString();
    }

    enum Role {
        A,
        B
    }
}
