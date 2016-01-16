package com.fnklabs.draenei.orm.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * If field type is enumerated or {@code Collection<Enum>}
 */
@Target(value = {ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Enumerated {

    /**
     * Enum class type
     *
     * @return
     */
    Class<?> enumType();
}
