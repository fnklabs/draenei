package com.fnklabs.draenei.analytics;


import com.fnklabs.draenei.analytics.morphology.Language;
import com.fnklabs.draenei.analytics.morphology.MorphologyFactory;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.morphology.Morphology;
import org.apache.lucene.morphology.WrongCharaterException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;

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


    public TextUtils() {
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
        com.fnklabs.metrics.Timer timer = MetricsFactory.getMetrics().getTimer(MetricsType.TEXT_UTILS_GET_NORMAL_FORMS.name());

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
        Timer timer = MetricsFactory.getMetrics().getTimer(MetricsType.TEXT_UTILS_TOKENIZE_TEXT.name());

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
        Timer timer = MetricsFactory.getMetrics().getTimer(MetricsType.TEXT_UTILS_IS_NORMAL_WORD.name());

        if (StringUtils.isEmpty(word)) {
            return false;
        }

        try {
            Timer morphTimer = MetricsFactory.getMetrics().getTimer(MetricsType.TEXT_UTILS_GET_MORPH_INFO.name());
            List<String> morphInfo = MorphologyFactory.getMorphology(language).getMorphInfo(word);
            morphTimer.stop();

            return morphInfo.stream()
                            .allMatch(new MorphRulesPredicate());

        } catch (Exception e) {
            LOGGER.warn("Can't get morph info: {" + word + "} ", e);
        } finally {
            timer.stop();
        }

        return false;
    }

    private static class MorphRulesPredicate implements Predicate<String> {

        /**
         * Evaluates this predicate on the given argument.
         *
         * @param morphInfo the input argument
         *
         * @return {@code true} if the input argument matches the predicate,
         * otherwise {@code false}
         */
        @Override
        public boolean test(String morphInfo) {
            return checkRussianRules(morphInfo) && checkEnglishRules(morphInfo);
        }

        private boolean checkEnglishRules(String morphInfo) {
            return !StringUtils.contains(morphInfo, "ARTICLE")
                    && !StringUtils.contains(morphInfo, "PREP")
                    && !StringUtils.contains(morphInfo, "PN")
                    && !StringUtils.contains(morphInfo, "CONJ");
        }

        private boolean checkRussianRules(String morphInfo) {
            return !StringUtils.contains(morphInfo, "МЕЖД")
                    && !StringUtils.contains(morphInfo, "ПРЕДЛ")
                    && !StringUtils.contains(morphInfo, "ЧАСТ")
                    && !StringUtils.contains(morphInfo, "МС")
                    && !StringUtils.contains(morphInfo, "СОЮЗ")
                    && !StringUtils.endsWith(morphInfo, "Н");
        }
    }

    @NotNull
    private List<String> splitText(@NotNull String text, @NotNull Language language) {
        Timer timer = MetricsFactory.getMetrics().getTimer(MetricsType.TEXT_UTILS_SPLIT.name());

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

    private enum MetricsType {
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
