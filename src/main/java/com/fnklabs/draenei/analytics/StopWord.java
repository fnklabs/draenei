package com.fnklabs.draenei.analytics;

import com.fnklabs.draenei.orm.annotations.Column;
import com.fnklabs.draenei.orm.annotations.PrimaryKey;
import com.fnklabs.draenei.orm.annotations.Table;

import java.io.Serializable;

@Table(name = "stop_word")
public class StopWord implements Serializable {

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


}
