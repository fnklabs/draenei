package com.fnklabs.draenei.orm.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(value = {ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface UDTColumn {
    /**
     * Provide Udt class type if current column type is UDT
     *
     * @return UDT class type
     */
    Class udtType();

}
