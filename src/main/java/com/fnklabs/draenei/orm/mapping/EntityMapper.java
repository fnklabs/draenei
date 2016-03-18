package com.fnklabs.draenei.orm.mapping;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Set;

public class EntityMapper {
    private final String name;
    private final Set<Property> properties;
    private final Class entityClassType;

    public EntityMapper(String name, Set<Property> properties, Class entityClassType) {
        this.name = name;
        this.properties = properties;
        this.entityClassType = entityClassType;
    }

    public Set<Property> getProperties() {
        return Collections.unmodifiableSet(properties);
    }

    public void map(String propertyName, Object entity, Object value) {
        for (Property property : properties) {
            if (StringUtils.equals(property.getName(), propertyName)) {
                property.writeValue(entity, value);
            }
        }
    }
}