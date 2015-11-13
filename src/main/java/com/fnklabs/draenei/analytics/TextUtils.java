package com.fnklabs.draenei.analytics;

import com.codahale.metrics.Timer;
import com.fnklabs.draenei.MetricsFactory;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.morphology.Morphology;
import org.apache.lucene.morphology.WrongCharaterException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Text utils that help
 * - retrieve normal forms of word
 * - tokenize text
 */
class TextUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TextUtils.class);
    private static final Set<Character> SPECIAL_CHARACTERS;

    private final MorphologyFactory morphologyFactory;
    private final MetricsFactory metricsFactory;
    private final StopWordsDao stopWordsDao;

    /**
     * @param morphologyFactory
     * @param stopWordsDao
     * @param metricsFactory
     *
     * @throws IOException
     */
    public TextUtils(MorphologyFactory morphologyFactory, StopWordsDao stopWordsDao, MetricsFactory metricsFactory) throws IOException {
        this.morphologyFactory = morphologyFactory;
        this.stopWordsDao = stopWordsDao;
        this.metricsFactory = metricsFactory;
    }

    /**
     * Get normal forms of word or if can't retrieve normal form of word return same word
     *
     * @param word Word
     *
     * @return Set of normal forms
     */
    @NotNull
    public Set<String> getNormalForms(String word, MorphologyFactory.Language language) {

        Timer.Context timer = metricsFactory.getTimer(MetricsType.TEXT_UTILS_GET_NORMAL_FORMS).time();

        Set<String> normalForms = new HashSet<>();


        Morphology morphology = morphologyFactory.getMorphology(language);

        try {
            normalForms.addAll(morphology.getNormalForms(word));
        } catch (WrongCharaterException e) {
            LOGGER.warn("Cant get normal form of word", e);
        }

        timer.stop();

        return normalForms;
    }

    /**
     * Token text into words
     *
     * @param text Text
     *
     * @return List of text token
     */
    public List<String> tokenizeText(String text, MorphologyFactory.Language language) {
        Timer.Context timer = metricsFactory.getTimer(MetricsType.TEXT_UTILS_TOKENIZE_TEXT).time();

        if (StringUtils.isEmpty(text)) {
            timer.stop();
            return new ArrayList<>();
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
     * @return
     */
    protected boolean isNormalWord(String word, MorphologyFactory.Language language) {
        Timer.Context timer = metricsFactory.getTimer(MetricsType.TEXT_UTILS_IS_NORMAL_WORD).time();

        try {
            if (StringUtils.isEmpty(word) || StringUtils.startsWith(word, "http://") || StringUtils.startsWith(word, "https://")) {
                return false;
            }

            Timer.Context morphTimer = metricsFactory.getTimer(MetricsType.TEXT_UTILS_GET_MORPH_INFO).time();
            List<String> morphInfo = morphologyFactory.getMorphology(language).getMorphInfo(word);
            morphTimer.stop();

            if (StringUtils.contains(morphInfo.get(0), "МЕЖД")
                    || StringUtils.contains(morphInfo.get(0), "ПРЕДЛ")
                    || StringUtils.contains(morphInfo.get(0), "ЧАСТ")
                    || StringUtils.contains(morphInfo.get(0), "МС")
                    || StringUtils.contains(morphInfo.get(0), "СОЮЗ")
                    || StringUtils.contains(morphInfo.get(0), "ARTICLE")
                    || StringUtils.contains(morphInfo.get(0), "PREP")
                    || StringUtils.contains(morphInfo.get(0), "PN")
                    || StringUtils.contains(morphInfo.get(0), "CONJ")) {
                return false;
            }

        } catch (Exception e) {
            LOGGER.warn("Can't get morph info: {" + word + "} ", e);
        } finally {
            timer.stop();
        }

        return true;
    }

    /**
     * Check if specified token is not stopword
     *
     * @param token Searched token/word
     *
     * @return True if token is stop word False otherwise
     */
    protected boolean isStopWord(@NotNull String token) {
        Timer.Context timer = metricsFactory.getTimer(MetricsType.TEXT_UTILS_IS_STOP_WORD).time();
        StopWord stopWord = Futures.getUnchecked(stopWordsDao.findOneAsync(token));
        timer.stop();

        return stopWord != null;
    }

    @NotNull
    private List<String> splitText(String text, MorphologyFactory.Language language) {
        Timer.Context timer = metricsFactory.getTimer(MetricsType.TEXT_UTILS_SPLIT).time();

        List<String> tokens = new ArrayList<>();

        StringBuilder tokenBuilder = new StringBuilder();

        for (int i = 0; i < text.length(); i++) {
            char chr = text.charAt(i);
            if (!SPECIAL_CHARACTERS.contains(chr)) {
                tokenBuilder.append(chr);
            } else if (tokenBuilder.length() > 0) {
                String token = tokenBuilder.toString();
                if (isNormalWord(token, language) && !isStopWord(token) && StringUtils.length(token) > 1) {
                    tokens.add(token.toLowerCase());
                }
                tokenBuilder.setLength(0);
            }
        }

        if (tokenBuilder.length() > 0) {
            String token = tokenBuilder.toString();
            if (isNormalWord(token, language)) {
                tokens.add(token.toLowerCase());
            }
        }

        timer.stop();

        return tokens;
    }

    private enum MetricsType implements MetricsFactory.Type {
        TEXT_UTILS_TOKENIZE_TEXT,
        TEXT_UTILS_IS_NORMAL_WORD,
        TEXT_UTILS_FIND_ONE,
        TEXT_UTILS_SPLIT, TEXT_UTILS_FILTER_WORDS, TEXT_UTILS_GET_MORPH_INFO, TEXT_UTILS_GET_NORMAL_FORMS, TEXT_UTILS_IS_STOP_WORD, TEXT_UTILS_ANALYZE_TEXT
    }

    @Nullable
    private static String cleanText(String text) {
        String[] searchList = {"\"", "'", ",", " ", ".", "#", "(", ")", "!", "?"};

        String[] replacementList = new String[searchList.length];

        for (int i = 0; i < replacementList.length; i++) {
            replacementList[i] = "";
        }

        return StringUtils.replaceEach(text, searchList, replacementList);
    }


    static {
        String chars = "/*!@#$%^&*()\\\"{}_:– -[]|\\\\?/<>,.«»—=\r\n\t";
        SPECIAL_CHARACTERS = new HashSet<>();
        for (char chr : chars.toCharArray()) {
            SPECIAL_CHARACTERS.add(chr);//[chr] = true;
        }
    }
}
