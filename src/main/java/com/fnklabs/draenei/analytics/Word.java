package com.fnklabs.draenei.analytics;

import com.google.common.base.Objects;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

class Word implements Serializable, Comparable<Word> {
    private static final long serialVersionUID = -5921333876414394127L;
    /**
     * Word id. Need to determine unique word in the document by position
     */
    private final UUID id = UUID.randomUUID();

    /**
     * Word in the document
     */
    private final String word;

    /**
     * Normal forms fo word
     */
    private final Set<String> normalForm;

    /**
     * @param word       Word in the document
     * @param normalForm Normal forms of word
     */
    public Word(String word, Set<String> normalForm) {
        this.word = word;
        this.normalForm = normalForm;
    }

    public String getWord() {
        return word;
    }

    public Set<String> getNormalForm() {
        return Collections.unmodifiableSet(normalForm);
    }

    public UUID getId() {
        return id;
    }

    /**
     * Return hash code by word id
     *
     * @return Hash code of {@link #id} field
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(getId());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Word) {
            return Objects.equal(getId(), ((Word) obj).getId());
        }

        return false;
    }

    /**
     * Check whether words are same by comparing theirs normal forms
     *
     * @param word Word
     *
     * @return True if words are same and False otherwise
     */
    public boolean same(@NotNull Word word) {
        return getNormalForm()
                .stream()
                .anyMatch(item -> {
                    return word
                            .getNormalForm()
                            .stream()
                            .anyMatch(normalForm -> {
                                return StringUtils.equals(item, normalForm);
                            });
                });
    }


    @Override
    public int compareTo(Word o) {
        return same(o) ? 0 : getWord().compareTo(o.getWord());
    }
}
