package com.fnklabs.draenei.analytics;


import org.jetbrains.annotations.NotNull;

/**
 * Facet type of object
 */
final class FacetType {
    public static final FacetType TEXT_FACET = new FacetType("text", String.class);
    /**
     * Object facet type name
     */
    @NotNull
    private final String name;

    /**
     * Object facet type class
     */
    @NotNull
    private final Class type;

    public FacetType(@NotNull String name, @NotNull Class type) {
        this.name = name;
        this.type = type;
    }

    @NotNull
    public String getName() {
        return name;
    }

    @NotNull
    public Class getType() {
        return type;
    }
}
