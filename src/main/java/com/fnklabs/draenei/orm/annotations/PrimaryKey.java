package com.fnklabs.draenei.orm.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Cassandra table primary key annotation help to understand which fields of class is belong to primary key and which of them is partition, composite or clustering key types
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface PrimaryKey {
    /**
     * Current key order in primary keys set. Primary keys can't have same order, if so then exception can be thrown
     *
     * @return Key order
     */
    int order() default 0;

    /**
     * Flag to determine whether current partition key is belong to partition key
     * <p>
     * If in primary keys set more than two elements and of them is not belong to partition key, so first key is partition key and last are clustering keys
     * If in primary keys set more that two elements and more than two keys marked as partition key, so such group is composite key and other keys are clustering keys
     *
     * @return True if current key belong to partition key False otherwise
     */
    boolean isPartitionKey() default true;
}
