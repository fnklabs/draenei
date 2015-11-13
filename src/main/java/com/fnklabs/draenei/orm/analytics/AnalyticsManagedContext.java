package com.fnklabs.draenei.orm.analytics;

import com.hazelcast.core.ManagedContext;
import org.jetbrains.annotations.NotNull;

public class AnalyticsManagedContext implements ManagedContext {
    @NotNull
    private Analytics analytics;

    public void setAnalyticsContext(@NotNull Analytics analytics) {
        this.analytics = analytics;
    }

    @Override
    public Object initialize(Object obj) {
        if (obj instanceof AnalyticsInstanceAware) {
            ((AnalyticsInstanceAware) obj).setAnalyticsInstance(analytics);
        }

        return obj;
    }
}
