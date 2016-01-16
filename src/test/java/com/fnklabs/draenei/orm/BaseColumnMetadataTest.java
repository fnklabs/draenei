package com.fnklabs.draenei.orm;

import com.datastax.driver.core.TableMetadata;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.UUID;

public class BaseColumnMetadataTest {

    @Test
    public void testBuildColumnMetadata() throws Exception {
        BeanInfo beanInfo = Introspector.getBeanInfo(TestEntity.class);

        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

        for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            if (propertyDescriptor.getName().equals("class")) {
                continue;
            }

            ColumnMetadata columnMetadata = ColumnMetadataBuilder.buildColumnMetadata(propertyDescriptor, TestEntity.class, Mockito.mock(TableMetadata.class));

            Assert.assertEquals("id", columnMetadata.getName());
        }
    }

    @Test
    public void testRead() throws Exception {
        BeanInfo beanInfo = Introspector.getBeanInfo(TestEntity.class);

        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

        for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            if (propertyDescriptor.getName().equals("class")) {
                continue;
            }

            ColumnMetadata columnMetadata = ColumnMetadataBuilder.buildColumnMetadata(propertyDescriptor, TestEntity.class, Mockito.mock(TableMetadata.class));

            Assert.assertNotNull(columnMetadata);

            Assert.assertEquals("id", columnMetadata.getName());

            UUID id = UUID.randomUUID();

            TestEntity entity = new TestEntity();
            entity.setId(id);

            UUID uuid = (UUID) columnMetadata.readValue(entity);

            Assert.assertEquals(id, uuid);
        }
    }

    @Test
    public void testWrite() throws Exception {
        BeanInfo beanInfo = Introspector.getBeanInfo(TestEntity.class);

        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

        for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            if (propertyDescriptor.getName().equals("class")) {
                continue;
            }

            ColumnMetadata columnMetadata = ColumnMetadataBuilder.buildColumnMetadata(propertyDescriptor, TestEntity.class, Mockito.mock(TableMetadata.class));

            Assert.assertNotNull(columnMetadata);

            Assert.assertEquals("id", columnMetadata.getName());

            UUID id = UUID.randomUUID();

            TestEntity entity = new TestEntity();
            entity.setId(id);

            UUID uuid = (UUID) columnMetadata.readValue(entity);

            Assert.assertEquals(id, uuid);

            UUID newId = UUID.randomUUID();

            columnMetadata.writeValue(entity, newId);

            Assert.assertEquals(newId, entity.getId());
        }

    }
}