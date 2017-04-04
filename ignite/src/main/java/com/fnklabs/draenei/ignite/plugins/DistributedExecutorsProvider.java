package com.fnklabs.draenei.ignite.plugins;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.UUID;

public class DistributedExecutorsProvider implements PluginProvider<ExecutorsConfiguration> {

    public static final String PLUGIN_NAME = "distributed_executors";
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedExecutorsProvider.class);

    private Ignite ignite;

    @Override
    public String name() {
        return PLUGIN_NAME;
    }

    @Override
    public String version() {
        return "0.1";
    }

    @Override
    public String copyright() {
        return "fnklabs";
    }

    @Override
    public DistributedExecutors plugin() {
        LOGGER.debug("get plugin extension");
        return new DistributedExecutors(ignite);
    }

    @Override
    public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        LOGGER.debug("Init extension");

        ignite = ctx.grid();

    }

    @Nullable
    @Override
    public <T> T createComponent(PluginContext ctx, Class<T> cls) {
        LOGGER.debug("Create component: {}", cls);
        return null;
    }

    @Override
    public void start(PluginContext ctx) throws IgniteCheckedException {
        LOGGER.debug("Start ");
    }

    @Override
    public void stop(boolean cancel) throws IgniteCheckedException {
        LOGGER.debug("Stop");
    }

    @Override
    public void onIgniteStart() throws IgniteCheckedException {
        LOGGER.debug("On ignite start");
    }

    @Override
    public void onIgniteStop(boolean cancel) {
        LOGGER.debug("On ignite stop");
    }

    @Nullable
    @Override
    public Serializable provideDiscoveryData(UUID nodeId) {
        LOGGER.debug("Provide discovery data");
        return null;
    }

    @Override
    public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        LOGGER.debug("Receive discovery data");
    }

    @Override
    public void validateNewNode(ClusterNode node) throws PluginValidationException {
        LOGGER.debug("Validate new node");
    }
}
