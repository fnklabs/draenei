package com.fnklabs.draenei.analytics.search.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for marking document fields as facet. Field marked as facets will be parsed by search engine
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(value = ElementType.FIELD)
public @interface Facet {
    /**
     * Flat that determine whether engine must parse (explode) String value and extract words from it
     *
     * @return True to parse value
     */
    boolean extractWords() default true;
}
