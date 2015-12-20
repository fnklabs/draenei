package com.fnklabs.draenei.orm;

import java.util.function.Function;

class JdkFunctionWrapper<Input, Output> implements com.google.common.base.Function<Input, Output> {
    private final Function<Input, Output> jdkFunction;

    JdkFunctionWrapper(Function<Input, Output> jdkFunction) {
        this.jdkFunction = jdkFunction;
    }

    @Override
    public Output apply(Input input) {
        return jdkFunction.apply(input);
    }
}
