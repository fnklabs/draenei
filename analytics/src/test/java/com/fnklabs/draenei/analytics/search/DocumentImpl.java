package com.fnklabs.draenei.analytics.search;

class DocumentImpl implements Document {
    private long id;

    @com.fnklabs.draenei.analytics.search.annotation.Facet
    private String text;

    public DocumentImpl(long id, String text) {
        this.id = id;
        this.text = text;
    }


    @Override
    public Long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }
}
