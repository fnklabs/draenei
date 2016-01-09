package com.fnklabs.draenei.analytics.search;

import com.fnklabs.draenei.analytics.TextUtils;
import com.fnklabs.draenei.analytics.morphology.Language;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Content based TF clustering algorithm
 */
class ClusteringTfAlgorithm implements ClusteringAlgorithm {
    @NotNull
    private TextUtils textUtils;

    public ClusteringTfAlgorithm(@NotNull TextUtils textUtils) {
        this.textUtils = textUtils;
    }

    @NotNull
    @Override
    public Set<Facet> build(@NotNull Object content) {

        Map<FacetKey, List<FacetKey>> facetsMap = buildFacets(content);

        Set<Facet> facets = new HashSet<>();

        int totalFacets = facetsMap.entrySet()
                                   .stream()
                                   .mapToInt(entry -> entry.getValue().size())
                                   .sum();

        facetsMap.forEach((key, values) -> {
            double rank = TfIdfUtils.calculateTf(values.size(), totalFacets);
            facets.add(new Facet(key, rank, 0));
        });

        return facets;
    }

    @Override
    public Set<Facet> build(@NotNull Document document) {
        Map<FacetKey, List<FacetKey>> facetsMap = buildFacets(document.getId());


        Set<Facet> facets = new HashSet<>();

        int totalFacets = facetsMap.entrySet()
                                   .stream()
                                   .mapToInt(entry -> entry.getValue().size())
                                   .sum();

        facetsMap.forEach((key, values) -> {
            double rank = TfIdfUtils.calculateTf(values.size(), totalFacets);
            facets.add(new Facet(key, rank, document.getId()));
        });

        return facets;
    }

    @NotNull
    private Map<FacetKey, List<FacetKey>> buildFacets(@NotNull Object content) {
        Map<FacetKey, List<FacetKey>> facetsMap = new HashMap<>();

        if (!content.getClass().isPrimitive() && !(content instanceof String)) {
            try {
                BeanInfo beanInfo = Introspector.getBeanInfo(content.getClass());

                for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
                    Method readMethod = propertyDescriptor.getReadMethod();

                    String name = propertyDescriptor.getName();

                    if (name.equals("class")) {
                        continue;
                    }

                    Field field = content.getClass().getDeclaredField(name);

                    boolean annotationPresent = field.isAnnotationPresent(com.fnklabs.draenei.analytics.search.annotation.Facet.class);

                    if (annotationPresent) {

                        Object fieldValue = getFieldValue(content, readMethod);

                        if (fieldValue != null) {
                            List<Serializable> values = transformValue(fieldValue);

                            values.forEach(val -> {
                                FacetType facetType = new FacetType(field.getName(), val.getClass());

                                FacetKey key = new FacetKey(facetType, val);

                                facetsMap.compute(key, new AddFunction());
                            });
                        }
                    }
                }
            } catch (IntrospectionException | NoSuchFieldException e) {
                LoggerFactory.getLogger(getClass()).warn("Can't read value", e);
            }
        } else {
            transformValue(content).forEach(val -> {
                        FacetKey key = new FacetKey(new FacetType("primitive", val.getClass()), val);

                        facetsMap.compute(key, new AddFunction());
                    }
            );
        }
        return facetsMap;
    }

    /**
     * Try to transform field value to simple types
     *
     * @param value
     *
     * @return
     */
    private List<Serializable> transformValue(@NotNull Object value) {
        List<Serializable> values = new ArrayList<>();

        if (value instanceof String) {
            List<String> build = build((String) value);

            values.addAll(build);

        } else if (value instanceof Collection) {
            Collection<Serializable> collection = (Collection) value;
            List<Serializable> collect = collection.stream()
                                                   .flatMap(item -> {
                                                       return transformValue(item).stream();
                                                   })
                                                   .collect(Collectors.<Serializable>toList());

            values.addAll(collect);
        } else {
            values.add((Serializable) value);
        }

        return values;
    }

    private Object getFieldValue(@NotNull Object content, Method field) {
        try {
            return field.invoke(content);
        } catch (InvocationTargetException | IllegalAccessException e) {
            LoggerFactory.getLogger(ClusteringTfAlgorithm.class).warn("Can't get field value", e);
        }

        return null;
    }

    /**
     * Build String facet from text
     *
     * @param text Text content
     *
     * @return Word facets
     */
    private List<String> build(@NotNull String text) {

        List<String> words = textUtils.extractWords(text, Language.RU);

        List<String> wordList = words.stream()
                                     .flatMap(word -> {
                                         return textUtils.getNormalForms(word.toLowerCase(), Language.RU)
                                                         .stream()
                                                         .filter(element -> textUtils.isNormalWord(element, Language.RU));
                                     })
                                     .collect(Collectors.toList());

        return wordList;
    }

    private static class AddFunction implements BiFunction<FacetKey, List<FacetKey>, List<FacetKey>> {
        @Override
        public List<FacetKey> apply(FacetKey key, List<FacetKey> keys) {
            if (keys == null) {
                keys = new ArrayList<>();
            }

            keys.add(key);

            return keys;
        }
    }
}
