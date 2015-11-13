package com.fnklabs.draenei.orm.analytics;

import org.jetbrains.annotations.NotNull;

public interface AnalyticsInstanceAware {
    void setAnalyticsInstance(@NotNull Analytics analytics);
}
