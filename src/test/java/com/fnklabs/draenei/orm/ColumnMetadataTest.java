package com.fnklabs.draenei.orm;

import com.datastax.driver.core.TableMetadata;
import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.PrimaryKey;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;

public class ColumnMetadataTest {

    @Test
    public void testBuildColumnMetadata() throws Exception {
        BeanInfo beanInfo = Introspector.getBeanInfo(TestEntity.class);

        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();

        for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            try {

                ColumnMetadata columnMetadata = ColumnMetadata.buildColumnMetadata(propertyDescriptor, TestEntity.class, Mockito.mock(TableMetadata.class));

                Assert.assertEquals("id", columnMetadata.getName());
            } catch (NoSuchFieldException e) {

            }

        }
    }

    private static class TestEntity {
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
}