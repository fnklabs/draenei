package com.fnklabs.draenei.analytics;

import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Content based TF clustering algorithm
 */
class TfIdfClusteringAlgorithm implements ClusteringAlgorithm {
    private TextUtils textUtils;

    public TfIdfClusteringAlgorithm(TextUtils textUtils) {
        this.textUtils = textUtils;
    }

    public static double calculateTfIdf(double tf, double idf) {
        return tf * idf;
    }

    /**
     * Calculate Inverse document frequency
     *
     * @param numberOfDocumentWithOccurrences Number of document containing term
     * @param numberOfTotalDocuments          Number of total document
     *
     * @return IDF
     */
    public static double calculateIdf(long numberOfDocumentWithOccurrences, long numberOfTotalDocuments) {
        if (numberOfDocumentWithOccurrences <= 0) {
            return 0;
        }

        return Math.log((double) numberOfTotalDocuments / numberOfDocumentWithOccurrences);
    }

    /**
     * Calculate TF
     *
     * @param repeats    Word repeats in document
     * @param totalWords Total words in document
     *
     * @return TF
     */
    public static double calculateTf(long repeats, long totalWords) {
        if (totalWords == 0) {
            return 0;
        }

        return (double) repeats / totalWords;
    }


    @NotNull
    @Override
    public Set<Facet> build(@NotNull Object platformContent) {

//        Set<Facet> wordFacet = build(platformContent.toString());
//
        Set<Facet> facetSet = new HashSet<>();
//        facetSet.addAll(wordFacet);
//
//        if (platformContent.getContentCategory() != null) {
//            facetSet.add(new Facet(FacetType.CATEGORY, platformContent.getContentCategory(), 1));
//        }
//
//        if (platformContent.getGenre() != null) {
//            platformContent.getGenre()
//                           .stream()
//                           .forEach(item -> {
//                               if (item != null) {
//                                   facetSet.add(new Facet(FacetType.GENRE, item, calculateTf(1, platformContent.getGenre().size())));
//                               }
//                           });
//
//        }
//
//        if (platformContent.getType() != null) {
//            platformContent.getType()
//                           .stream()
//                           .forEach(item -> {
//                               if (item != null) {
//                                   facetSet.add(new Facet(FacetType.TYPE, item, calculateTf(1, platformContent.getType().size())));
//
//                               }
//                           });
//
//
//        }
//
//        if (platformContent.getTags() != null) {
//            platformContent.getTags()
//                           .stream()
//                           .forEach(item -> {
//                               if (item != null) {
//                                   facetSet.add(new Facet(FacetType.TAG, item, calculateTf(1, platformContent.getTags().size())));
//                               }
//                           });
//        }
//
//        if (platformContent.getCrew() != null) {
//            platformContent.getCrew()
//                           .stream()
//                           .forEach(item -> {
//                               if (item != null) {
//                                   facetSet.add(new Facet(FacetType.CREW, item, calculateTf(1, platformContent.getCrew().size())));
//                               }
//                           });
//        }


        return facetSet;
    }

    @NotNull
    @Override
    public Set<Facet> build(@NotNull String content) {

        List<String> words = textUtils.tokenizeText(content, MorphologyFactory.Language.RU);

        List<String> wordList = words.parallelStream()
                                     .flatMap(word -> textUtils.getNormalForms(word.toLowerCase(), MorphologyFactory.Language.RU).stream())
                                     .collect(Collectors.toList());

        return wordList.parallelStream()
                       .collect(Collectors.<String, String>groupingBy(word -> word))
                       .entrySet()
                       .parallelStream()
                       .map(entry -> {
                           double tf = calculateTf(entry.getValue().size(), words.size());

                           List<String> value = entry.getValue();

                           Optional<String> first = value.stream().findFirst();

                           return new Facet(FacetType.TEXT_FACET, first.get(), tf);
                       })
                       .collect(Collectors.toSet());
    }
}
