package com.fnklabs.draenei.analytics;

import com.codahale.metrics.Timer;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import tv.nemo.content.dao.StopWordsDao;
import tv.nemo.core.Metrics;

import java.io.IOException;
import java.util.Set;

@Service
class CacheableTextUtils extends TextUtils {

    private final LoadingCache<String, Boolean> IS_NORMAL_WORD_CACHE = CacheBuilder.newBuilder()
                                                                                   .maximumSize(50000)
                                                                                   .<String, Boolean>build(new CacheLoader<String, Boolean>() {
                                                                                       @Override
                                                                                       public Boolean load(@NotNull String key) throws Exception {
                                                                                           return CacheableTextUtils.super.isNormalWord(key);
                                                                                       }
                                                                                   });

    private final LoadingCache<String, Set<String>> NORMAL_FORMS_OF_WORD_CACHE = CacheBuilder.newBuilder()
                                                                                             .maximumSize(50000)
                                                                                             .<String, Set<String>>build(new CacheLoader<String, Set<String>>() {
                                                                                                 @Override
                                                                                                 public Set<String> load(@NotNull String key) throws Exception {
                                                                                                     return CacheableTextUtils.super.getNormalForms(key);
                                                                                                 }
                                                                                             });

    private final LoadingCache<String, Boolean> IS_STOP_WORD = CacheBuilder.newBuilder()
                                                                           .maximumSize(10000)
                                                                           .<String, Boolean>build(new CacheLoader<String, Boolean>() {
                                                                               @Override
                                                                               public Boolean load(@NotNull String key) throws Exception {
                                                                                   return CacheableTextUtils.super.isStopWord(key);
                                                                               }
                                                                           });

    @Autowired
    public CacheableTextUtils(StopWordsDao stopWordsDao) throws IOException {
        super(stopWordsDao);
    }

    @Override
    public Set<String> getNormalForms(String word) {
        Timer.Context timer = Metrics.getTimer(MetricsType.CACHEABLE_TEXT_UTILS_GET_NORMAL_FORMS).time();
        Set<String> unchecked = NORMAL_FORMS_OF_WORD_CACHE.getUnchecked(word);
        timer.stop();

        return unchecked;
    }

    @Override
    protected boolean isStopWord(@NotNull String token) {
        Timer.Context timer = Metrics.getTimer(MetricsType.CACHEABLE_TEXT_UTILS_IS_STOP_WORD).time();

        boolean isStopWord = IS_STOP_WORD.getUnchecked(token);

        timer.stop();

        return isStopWord;
    }

    @Override
    protected boolean isNormalWord(String word) {
        Timer.Context timer = Metrics.getTimer(MetricsType.CACHEABLE_TEXT_UTILS_IS_NORMAL_WORD).time();

        Boolean result = IS_NORMAL_WORD_CACHE.getUnchecked(word);
        timer.stop();

        return result;
    }

    private enum MetricsType implements Metrics.Type {
        CACHEABLE_TEXT_UTILS_GET_NORMAL_FORMS, CACHEABLE_TEXT_UTILS_IS_STOP_WORD, CACHEABLE_TEXT_UTILS_IS_NORMAL_WORD
    }


}
