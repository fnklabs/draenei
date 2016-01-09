package com.fnklabs.draenei.analytics.search;


class TfIdfUtils {
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
    public static double calculateIdf(double numberOfDocumentWithOccurrences, double numberOfTotalDocuments) {
        if (numberOfDocumentWithOccurrences <= 0) {
            return 0;
        }

        return Math.log(numberOfTotalDocuments / numberOfDocumentWithOccurrences);
    }

    /**
     * Calculate TF
     *
     * @param repeats     Word repeats in document
     * @param totalFacets Total facets in document
     *
     * @return TF
     */
    public static double calculateTf(double repeats, double totalFacets) {
        if (totalFacets == 0) {
            return 0;
        }

        return repeats / totalFacets;
    }
}
