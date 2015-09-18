package com.fnklabs.draenei.orm.exception;

public class CanNotBuildEntryCacheKey extends RuntimeException {
    public CanNotBuildEntryCacheKey(Class clazz, Throwable throwable) {
        super(String.format("Can't build cache key for entry \"%s\"", clazz.getName()), throwable);
    }
}
