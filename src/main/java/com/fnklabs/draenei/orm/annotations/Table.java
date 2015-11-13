package com.fnklabs.draenei.orm.annotations;

import com.datastax.driver.core.ConsistencyLevel;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {
    /**
     * Table name
     *
     * @return Table name
     */
    String name();

    /**
     * Whether current table is compact
     *
     * @return True if table was created as compact storage
     */
    boolean compactStorage() default false;

    /**
     * Max fetch size for stmt
     *
     * @return Default fetch size
     */
    int fetchSize() default 500;

    /**
     * Default consistency level for read operations
     *
     * @return Consistency level
     */
    ConsistencyLevel readConsistencyLevel() default ConsistencyLevel.QUORUM;

    /**
     * Default consistency level for write/delete operations
     *
     * @return Consistency level
     */
    ConsistencyLevel writeConsistencyLevel() default ConsistencyLevel.QUORUM;
}
