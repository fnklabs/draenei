package com.fnklabs.draenei.analytics;

import tv.nemo.content.entity.PlatformContent;

enum FacetType {
    /**
     * word facet retrieved from content description
     * see {@link PlatformContent#description}
     */
    CONTENT_TEXT,

    /**
     * Content genre
     * see {@link PlatformContent#genre}
     */
    GENRE,

    /**
     * Content category
     * <p>
     * see {@link PlatformContent#category}
     */
    CATEGORY,

    /**
     * Content crew
     * see {@link PlatformContent#crew}
     */
    CREW,

    /**
     * Content type
     * see {@link PlatformContent#type}
     */
    TYPE,

    TAG,
}
