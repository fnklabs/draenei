package com.fnklabs.draenei.analytics;

import java.io.Serializable;

public interface DataProviderRangeScanFactory<T extends Serializable> {
    DataProviderRangeScan<T> create(long startToken, long endToken);
}
