package com.fnklabs.draenei.analytics.search;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.Collection;
import java.util.function.Predicate;

public interface SearchService extends Serializable {
    /**
     * Ignite Service name
     */
    String SERVICE_NAME = "com.fnklabs.draenei.search_service";

    /**
     * Add document to search index if document already stored in index than old version will be replaced by new version
     *
     * @param document Document for search
     */
    void addDocument(@NotNull Document document);

    /**
     * Search documents by facets
     * <p>
     * At first step userPredicate will be applied to documentIndex and then check facets
     *
     * @param facets        Facets by which will be doing search
     * @param userPredicate User custom predicate for filtering document inedx
     *
     * @return Documents with similar facets
     */
    @NotNull
    <T extends Predicate<DocumentIndex> & Serializable> Collection<SearchResult> search(@NotNull Collection<Facet> facets, @Nullable T userPredicate);

    /**
     * Search similar document to specified document
     *
     * @param documentId ID document
     *
     * @return Documents with similar facets
     */
    @NotNull
    Collection<SearchResult> getSimilar(long documentId);

    /**
     * Search document by text. At first will create facets from search query and than call {@link #search(Collection, Predicate)} method
     *
     * @param text Facets by which will be doing search
     *
     * @return Documents with similar facets
     */
    @NotNull
    Collection<SearchResult> search(@NotNull String text);

    /**
     * Build facets from document
     *
     * @param document Document
     *
     * @return Document facets
     */
    @NotNull
    Collection<Facet> buildFacets(@NotNull Document document);

    /**
     * Build facets from text
     *
     * @param text Text
     *
     * @return Text facets
     */
    @NotNull
    Collection<Facet> buildFacets(@NotNull String text);

    /**
     * Calculate facets rank.
     * <p>
     * Current method will group facets by facet type and facet and then calculate facet RANK foreach facet type group
     * <p>
     * Current method also return distinct facets values
     *
     * @param facets Input facets
     *
     * @return FacetRank collection
     */
    Collection<FacetRank> calculateRank(@NotNull Collection<Facet> facets);

    /**
     * Calculate similarity between two vectors
     *
     * @param firstVector  First vector
     * @param secondVector Second vector
     *
     * @return similarity index
     */
    double calculateSimilarity(@NotNull Collection<FacetRank> firstVector, @NotNull Collection<FacetRank> secondVector);
}