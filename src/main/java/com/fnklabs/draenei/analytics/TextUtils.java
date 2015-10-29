package com.fnklabs.draenei.analytics;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.morphology.EnglishLuceneMorphology;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tv.nemo.content.dao.StopWordsDao;
import tv.nemo.content.entity.StopWord;
import tv.nemo.core.Metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Text utils
 */
@Service
class TextUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TextUtils.class);
    private static final Set<Character> SPECIAL_CHARACTERS;
    private final RussianLuceneMorphology russianLuceneMorphology;
    private final EnglishLuceneMorphology englishLuceneMorphology;


    private StopWordsDao stopWordsDao;


    @Autowired
    public TextUtils(StopWordsDao stopWordsDao) throws IOException {
        this.stopWordsDao = stopWordsDao;
        this.russianLuceneMorphology = new RussianLuceneMorphology();
        this.englishLuceneMorphology = new EnglishLuceneMorphology();
    }

    /**
     * Get normal forms of word or if can't retrieve normal form of word return same word
     *
     * @param word Word
     *
     * @return Set of normal forms
     */
    public Set<String> getNormalForms(String word) {

        Timer.Context timer = Metrics.getTimer(MetricsType.TEXT_UTILS_GET_NORMAL_FORMS).time();

        Set<String> normalForms = new HashSet<>();

        if (russianLuceneMorphology.checkString(word)) {
            normalForms.addAll(russianLuceneMorphology.getNormalForms(word));
        } else if (englishLuceneMorphology.checkString(word)) {
            normalForms.addAll(englishLuceneMorphology.getNormalForms(word));
        } else {
            normalForms.add(word);
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
    public List<String> tokenizeText(String text) {
        Timer.Context timer = Metrics.getTimer(MetricsType.TEXT_UTILS_TOKENIZE_TEXT).time();

        if (StringUtils.isEmpty(text)) {
            timer.stop();
            return new ArrayList<>();
        }

        List<String> strings = splitText(text);

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
    protected boolean isNormalWord(String word) {
        Timer.Context timer = Metrics.getTimer(MetricsType.TEXT_UTILS_IS_NORMAL_WORD).time();

        try {
            if (StringUtils.isEmpty(word) || StringUtils.startsWith(word, "http://") || StringUtils.startsWith(word, "https://")) {
                return false;
            }

            if (russianLuceneMorphology.checkString(word)) {
                Timer.Context morphTimer = Metrics.getTimer(MetricsType.TEXT_UTILS_GET_MORPH_INFO).time();
                List<String> morphInfo = russianLuceneMorphology.getMorphInfo(word);
                morphTimer.stop();

                if (StringUtils.contains(morphInfo.get(0), "МЕЖД")
                        || StringUtils.contains(morphInfo.get(0), "ПРЕДЛ")
                        || StringUtils.contains(morphInfo.get(0), "ЧАСТ")
                        || StringUtils.contains(morphInfo.get(0), "МС")
                        || StringUtils.contains(morphInfo.get(0), "СОЮЗ")) {
                    return false;
                }
            } else if (englishLuceneMorphology.checkString(word)) {
                Timer.Context morphTimer = Metrics.getTimer(MetricsType.TEXT_UTILS_GET_MORPH_INFO).time();
                List<String> morphInfo = englishLuceneMorphology.getMorphInfo(word);
                morphTimer.stop();


                if (StringUtils.contains(morphInfo.get(0), "ARTICLE")
                        || StringUtils.contains(morphInfo.get(0), "PREP")
                        || StringUtils.contains(morphInfo.get(0), "PN")
                        || StringUtils.contains(morphInfo.get(0), "CONJ")) {
                    return false;
                }
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
        Timer.Context timer = Metrics.getTimer(MetricsType.TEXT_UTILS_IS_STOP_WORD).time();
        StopWord stopWord = Futures.getUnchecked(stopWordsDao.findOneAsync(token));
        timer.stop();

        return stopWord != null;
    }

    @NotNull
    private List<String> splitText(String text) {
        Timer.Context timer = Metrics.getTimer(MetricsType.TEXT_UTILS_SPLIT).time();

        List<String> tokens = new ArrayList<>();

        StringBuilder tokenBuilder = new StringBuilder();

        for (int i = 0; i < text.length(); i++) {
            char chr = text.charAt(i);
            if (!SPECIAL_CHARACTERS.contains(chr)) {
                tokenBuilder.append(chr);
            } else if (tokenBuilder.length() > 0) {
                String token = tokenBuilder.toString();
                if (isNormalWord(token) && !isStopWord(token) && StringUtils.length(token) > 1) {
                    tokens.add(token.toLowerCase());
                }
                tokenBuilder.setLength(0);
            }
        }

        if (tokenBuilder.length() > 0) {
            String token = tokenBuilder.toString();
            if (isNormalWord(token)) {
                tokens.add(token.toLowerCase());
            }
        }

        timer.stop();

        return tokens;
    }

    private enum MetricsType implements Metrics.Type {
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
