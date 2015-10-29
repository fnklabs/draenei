package com.fnklabs.draenei.analytics;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import tv.nemo.core.Metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class SearchResultReducerFactory implements ReducerFactory<Long, Double, Double> {
    private final long totalDocuments;

    public SearchResultReducerFactory(long totalDocuments) {
        this.totalDocuments = totalDocuments;
    }


    @Override
    public Reducer<Double, Double> newReducer(Long key) {
        return new ResultReducer(totalDocuments);
    }


    private enum MetricsType implements Metrics.Type {
        SEARCH_REDUCER;
    }

    private static class ResultReducer extends Reducer<Double, Double> {
        private final long totalDocuments;
        private List<Double> searchResults = Collections.synchronizedList(new ArrayList<>());


        public ResultReducer(long totalDocuments) {
            this.totalDocuments = totalDocuments;
        }

        @Override
        public void reduce(Double value) {
            searchResults.add(value);
        }

        @Override
        public Double finalizeReduce() {
            return searchResults.stream().mapToDouble(item -> item).sum();
        }


    }

}
