package com.fnklabs.draenei.analytics.search;

import java.io.Serializable;
import java.util.function.Predicate;

public final class AlwaysTruePredicate implements Predicate<DocumentIndex>, Serializable {
    public final static AlwaysTruePredicate INSTANCE = new AlwaysTruePredicate();

    private AlwaysTruePredicate() {
    }

    @Override
    public boolean test(DocumentIndex documentIndex) {
        return true;
    }
}
