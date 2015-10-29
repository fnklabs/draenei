package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.orm.annotations.ClusteringKey;
import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.Table;
import com.google.gson.annotations.Expose;
import tv.nemo.entity.CacheableEntity;

@Table(name = "stop_word")
public class StopWord extends CacheableEntity {

    @ClusteringKey
    @Column(name = "stop_word")
    private String stopWord;
    @Expose
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


}
