package com.fnklabs.draenei.analytics.search;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;

public interface Document extends Serializable {
    @NotNull
    Long getId();
}
