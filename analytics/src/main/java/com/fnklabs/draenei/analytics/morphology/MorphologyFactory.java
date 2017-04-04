package com.fnklabs.draenei.analytics.morphology;

import org.apache.lucene.morphology.Morphology;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class MorphologyFactory {
    /**
     * Default morphology implementation that used when can't retrieve morphology instance by language
     */
    protected static final Morphology DEFAULT_MORPHOLOGY = new Morphology() {
        @Override
        public List<String> getNormalForms(String s) {
            return Collections.<String>emptyList();
        }

        @Override
        public List<String> getMorphInfo(String s) {
            return Collections.<String>emptyList();
        }
    };

    private static final Map<Language, Morphology> languageMorphology = new HashMap<>();

    private MorphologyFactory() {
    }


    public static Morphology getMorphology(Language language) {
        return languageMorphology.compute(language, new RegisterMorphologyDictionary());
    }

    private static class RegisterMorphologyDictionary implements BiFunction<Language, Morphology, Morphology> {

        @Override
        public Morphology apply(Language language, Morphology morphology) {
            if (morphology == null) {
                try {
                    Class<Morphology> morphologyClass = (Class<Morphology>) Class.forName(language.getMorphologyClass());

                    morphology = morphologyClass.newInstance();

                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                    LoggerFactory.getLogger(MorphologyFactory.class).warn("Can't retrieve morphology dictionary instance", e);

                    morphology = DEFAULT_MORPHOLOGY;
                }
            }

            return morphology;
        }
    }


}
