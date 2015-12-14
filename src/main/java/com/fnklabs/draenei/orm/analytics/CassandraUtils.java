package com.fnklabs.draenei.orm.analytics;

import com.fnklabs.draenei.CassandraClient;
import com.google.common.collect.Range;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;

class CassandraUtils {
    /**
     * Split ring into token range.
     * <p>
     * Current implementation create token range based on ring topology it's only create small token ranges
     *
     * @param cassandraClient Cassandra client
     *
     * @return Collection of token range
     */
    static Collection<Range<Long>> splitRing(@NotNull CassandraClient cassandraClient) {
        Collection<Range<Long>> ranges = new ArrayList<>();

        BigInteger startToken = new BigInteger(String.valueOf(Long.MIN_VALUE));
        BigInteger maxValue = new BigInteger(String.valueOf(Long.MAX_VALUE));

        int membersSize = cassandraClient.getMembers().size();

        BigInteger step = maxValue.divide(BigInteger.valueOf(256 * membersSize));

        while (startToken.compareTo(maxValue) <= 0) {

            BigInteger endToken = startToken.add(step);

            long startTokenLongValue = startToken.longValue();
            long endTokenLongValue = startTokenLongValue < endToken.longValue() ? endToken.longValue() : Long.MAX_VALUE;

            if (startTokenLongValue == Long.MIN_VALUE && endTokenLongValue != Long.MAX_VALUE) {
                ranges.add(Range.closedOpen(startTokenLongValue, endTokenLongValue));
            } else if (startTokenLongValue == Long.MIN_VALUE && endTokenLongValue == Long.MAX_VALUE) {
                ranges.add(Range.closed(startTokenLongValue, endTokenLongValue));
            } else if (endTokenLongValue == Long.MAX_VALUE) {
                ranges.add(Range.openClosed(startTokenLongValue, endTokenLongValue));
            } else {
                ranges.add(Range.closedOpen(startTokenLongValue, endTokenLongValue));
            }


            startToken = endToken;
        }

        return ranges;
    }
}
