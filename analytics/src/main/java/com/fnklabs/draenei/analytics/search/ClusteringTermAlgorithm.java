package com.fnklabs.draenei.analytics.search;

import com.fnklabs.draenei.analytics.TextUtils;
import com.fnklabs.draenei.analytics.morphology.Language;
import org.apache.commons.beanutils.PropertyUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Content based TF clustering algorithm
 */
public class ClusteringTermAlgorithm implements ClusteringAlgorithm {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusteringTermAlgorithm.class);


    private TextUtils textUtils;

    public ClusteringTermAlgorithm(TextUtils textUtils) {
        this.textUtils = textUtils;
    }


    @Override
    public List<Facet> build(Object content) {
        return isPrimitive(content) ? buildFacetsFromPrimitive(content) : buildFacetsFromObject(content);
    }

    @Nullable
    private Object getFieldValue(Object content, Field field) {
        try {
            PropertyDescriptor propertyDescriptor = PropertyUtils.getPropertyDescriptor(content, field.getName());
            return propertyDescriptor.getReadMethod().invoke(content);
        } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException e) {
            LOGGER.warn("Can't get field value", e);
        }

        return null;
    }

    private boolean isPrimitive(Object document) {
        return document.getClass().isPrimitive() || document instanceof String;
    }

    /**
     * Build facets from document object by extracting all fields and values
     *
     * @param document Document object
     *
     * @return List of facets
     */
    private List<Facet> buildFacetsFromObject(Object document) {
        List<Facet> facets = new ArrayList<>();

        for (Field field : document.getClass().getDeclaredFields()) {
            if (isFacetField(field)) {
                Object fieldValue = getFieldValue(document, field);

                if (fieldValue != null) {
                    List<Serializable> values = transformValue(fieldValue);

                    values.forEach(value -> {
                        Facet key = new Facet(new FacetType(field.getName(), value.getClass()), value);

                        facets.add(key);
                    });
                }
            }
        }

        return facets;
    }

    private boolean isFacetField(Field field) {
        return field.isAnnotationPresent(com.fnklabs.draenei.analytics.search.annotation.Facet.class);
    }

    private List<Facet> buildFacetsFromPrimitive(Object document) {
        return transformValue(document).stream()
                                       .map(value -> new Facet(new FacetType(FacetType.UNKNOWN, value.getClass()), value))
                                       .collect(Collectors.toList());
    }

    /**
     * Try to transform field value to primitive type
     *
     * @param value Facet value
     *
     * @return Value transformed to base types
     */
    private List<Serializable> transformValue(Object value) {
        List<Serializable> values = new ArrayList<>();

        if (value instanceof String) {
            List<String> build = extractWords((String) value, Language.RU);

            values.addAll(build);

        } else if (value instanceof Collection) {
            Collection<Serializable> collection = (Collection) value;
            List<Serializable> collect = collection.stream()
                                                   .flatMap(item -> transformValue(item).stream())
                                                   .collect(Collectors.<Serializable>toList());

            values.addAll(collect);
        } else {
            values.add((Serializable) value);
        }

        return values;
    }

    /**
     * Build String facet from text
     *
     * @param text Text document
     *
     * @return Word facets
     */
    private List<String> extractWords(String text, Language language) {

        List<String> words = textUtils.extractWords(text, language);

        List<String> wordList = words.stream()
                                     .flatMap(word -> {
                                         return textUtils.getNormalForms(word.toLowerCase(), language)
                                                         .stream()
                                                         .filter(element -> textUtils.isNormalWord(element, language));
                                     })
                                     .collect(Collectors.toList());

        return wordList;
    }


}
