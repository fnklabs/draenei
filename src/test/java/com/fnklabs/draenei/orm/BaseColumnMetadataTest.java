package com.fnklabs.draenei.orm;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.fnklabs.draenei.CassandraClient;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.UUID;

import static org.mockito.Mockito.when;

@Ignore
public class BaseColumnMetadataTest {

    @Test
    public void testBuildColumnMetadata() throws Exception {
        BeanInfo beanInfo = Introspector.getBeanInfo(TestEntity.class);

        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

        for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            if (propertyDescriptor.getName().equals("class")) {
                continue;
            }

            TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);
            com.datastax.driver.core.ColumnMetadata metadata = Mockito.mock(com.datastax.driver.core.ColumnMetadata.class);
            when(metadata.getType()).thenReturn(DataType.ascii());
            when(tableMetadata.getColumn(Matchers.anyString())).thenReturn(metadata);

            ColumnMetadata columnMetadata = EntityMetadata.buildColumnMetadata(propertyDescriptor, TestEntity.class, Mockito.mock(CassandraClient.class), tableMetadata);

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

            TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);

            com.datastax.driver.core.ColumnMetadata metadata = Mockito.mock(com.datastax.driver.core.ColumnMetadata.class);
            when(metadata.getType()).thenReturn(DataType.ascii());

            when(tableMetadata.getColumn(Matchers.anyString())).thenReturn(metadata);

            ColumnMetadata columnMetadata = EntityMetadata.buildColumnMetadata(propertyDescriptor, TestEntity.class, Mockito.mock(CassandraClient.class), tableMetadata);

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

            TableMetadata tableMetadata = Mockito.mock(TableMetadata.class);
            com.datastax.driver.core.ColumnMetadata metadata = Mockito.mock(com.datastax.driver.core.ColumnMetadata.class);
            when(metadata.getType()).thenReturn(DataType.ascii());
            when(tableMetadata.getColumn(Matchers.anyString())).thenReturn(metadata);

            ColumnMetadata columnMetadata = EntityMetadata.buildColumnMetadata(propertyDescriptor, TestEntity.class, Mockito.mock(CassandraClient.class), tableMetadata);

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