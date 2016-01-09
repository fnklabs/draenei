package com.fnklabs.draenei.analytics.morphology;

import org.apache.lucene.morphology.english.EnglishLuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;

public enum Language {
    EN(EnglishLuceneMorphology.class.getName()),
    RU(RussianLuceneMorphology.class.getName());

    private String morphologyClass;

    Language(String morphologyClass) {
        this.morphologyClass = morphologyClass;
    }

    public String getMorphologyClass() {
        return morphologyClass;
    }
}
