package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.orm.Cacheable;
import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.PrimaryKey;
import com.fnklabs.draenei.orm.annotations.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@Table(name = "stop_word")
public class StopWord implements Cacheable {

    @PrimaryKey
    @Column(name = "stop_word")
    private String stopWord;

    private Long cacheId;

    public StopWord() {
    }

    public StopWord(String stopWord) {
        this.stopWord = stopWord;
    }

    public String getStopWord() {
        return stopWord;
    }

    public void setStopWord(String stopWord) {
        this.stopWord = stopWord;
    }


    @Nullable
    @Override
    public Long getCacheKey() {
        return null;
    }

    @Override
    public void setCacheKey(@NotNull Long id) {

    }
}
