package com.fnklabs.draenei.analytics;

import com.codahale.metrics.Timer;
import com.fnklabs.draenei.MetricsFactory;
import com.fnklabs.draenei.analytics.morphology.Language;
import com.fnklabs.draenei.analytics.morphology.MorphologyFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.morphology.Morphology;
import org.apache.lucene.morphology.WrongCharaterException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Text utils that help
 * - retrieve normal forms of word
 * - tokenize text
 */
public class TextUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TextUtils.class);

    /**
     * Special characters by which text will be exploded
     */
    private static final Set<Character> SPECIAL_CHARACTERS;

    @NotNull
    private final MetricsFactory metricsFactory;

    /**
     * @param metricsFactory MetricsFactory
     */
    public TextUtils(@NotNull MetricsFactory metricsFactory) {
        this.metricsFactory = metricsFactory;
    }

    /**
     * Get normal forms of word or if can't retrieve normal form of word return same word
     *
     * @param word Word
     *
     * @return Normal forms
     */
    @NotNull
    public Set<String> getNormalForms(@NotNull String word, @NotNull Language language) {
        Timer.Context timer = metricsFactory.getTimer(MetricsType.TEXT_UTILS_GET_NORMAL_FORMS).time();

        Set<String> normalForms = new HashSet<>();

        try {
            Morphology morphology = MorphologyFactory.getMorphology(language);
            List<String> normalForms1 = morphology.getNormalForms(word);
            normalForms.addAll(normalForms1);
        } catch (WrongCharaterException e) {
            LOGGER.warn("Can't get normal form of word", e);
        }

        timer.stop();

        return normalForms;
    }

    /**
     * Extract normal form of words from text
     *
     * @param text Text
     *
     * @return List of text token
     */
    public List<String> extractWords(@NotNull String text, @NotNull Language language) {
        Timer.Context timer = metricsFactory.getTimer(MetricsType.TEXT_UTILS_TOKENIZE_TEXT).time();

        if (StringUtils.isEmpty(text)) {
            timer.stop();
            return Collections.emptyList();
        }

        List<String> strings = splitText(text, language);

        timer.stop();

        return strings;
    }

    /**
     * Check if word is normal word
     *
     * @param word Checked word
     *
     * @return True if it normal form of word
     */
    public boolean isNormalWord(@NotNull String word, @NotNull Language language) {
        Timer.Context timer = metricsFactory.getTimer(MetricsType.TEXT_UTILS_IS_NORMAL_WORD).time();

        if (StringUtils.isEmpty(word)) {
            return false;
        }

        try {
            Timer.Context morphTimer = metricsFactory.getTimer(MetricsType.TEXT_UTILS_GET_MORPH_INFO).time();
            List<String> morphInfo = MorphologyFactory.getMorphology(language).getMorphInfo(word);
            morphTimer.stop();

            return !(StringUtils.contains(morphInfo.get(0), "МЕЖД")
                    || StringUtils.contains(morphInfo.get(0), "ПРЕДЛ")
                    || StringUtils.contains(morphInfo.get(0), "ЧАСТ")
                    || StringUtils.contains(morphInfo.get(0), "МС")
                    || StringUtils.contains(morphInfo.get(0), "СОЮЗ")
                    || StringUtils.contains(morphInfo.get(0), "ARTICLE")
                    || StringUtils.contains(morphInfo.get(0), "PREP")
                    || StringUtils.contains(morphInfo.get(0), "PN")
                    || StringUtils.contains(morphInfo.get(0), "CONJ"));

        } catch (Exception e) {
            LOGGER.warn("Can't get morph info: {" + word + "} ", e);
        } finally {
            timer.stop();
        }

        return false;
    }

    @NotNull
    private List<String> splitText(@NotNull String text, @NotNull Language language) {
        Timer.Context timer = metricsFactory.getTimer(MetricsType.TEXT_UTILS_SPLIT).time();

        List<String> tokens = new ArrayList<>();

        StringBuilder tokenBuilder = new StringBuilder();

        for (int i = 0; i < text.length(); i++) {
            char chr = text.charAt(i);

            if (!isSpecialCharacter(chr)) {
                tokenBuilder.append(chr);
            } else if (tokenBuilder.length() > 0) {
                String token = tokenBuilder.toString();

                if (canAddToWords(token, language)) {
                    tokens.add(token.toLowerCase());
                }

                tokenBuilder.setLength(0);
            }
        }

        if (canAddToWords(tokenBuilder.toString(), language)) {
            tokens.add(tokenBuilder.toString());
        }

        timer.stop();

        return tokens;
    }

    private boolean isSpecialCharacter(char chr) {
        return SPECIAL_CHARACTERS.contains(chr) || Character.isWhitespace(chr);
    }

    private boolean canAddToWords(@NotNull String token, @NotNull Language language) {
        return !StringUtils.isEmpty(token);
    }

    private enum MetricsType implements MetricsFactory.Type {
        TEXT_UTILS_TOKENIZE_TEXT,
        TEXT_UTILS_IS_NORMAL_WORD,
        TEXT_UTILS_SPLIT,
        TEXT_UTILS_GET_MORPH_INFO,
        TEXT_UTILS_GET_NORMAL_FORMS,
        TEXT_UTILS_IS_STOP_WORD
    }

    static {
        String chars = "/*!@#$%^&*()\\\"{}_:– -[]|\\\\?/<>,.«»—=\r\n\t ";

        SPECIAL_CHARACTERS = new HashSet<>();

        for (char chr : chars.toCharArray()) {
            SPECIAL_CHARACTERS.add(chr);
        }
    }
}
