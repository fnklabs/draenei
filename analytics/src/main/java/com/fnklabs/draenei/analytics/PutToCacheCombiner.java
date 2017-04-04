package com.fnklabs.draenei.analytics;

import com.google.common.base.Verify;
import org.apache.ignite.cache.CacheEntryProcessor;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

public class PutToCacheCombiner<OutputKey, CombinerOutputValue> implements CacheEntryProcessor<OutputKey, CombinerOutputValue, CombinerOutputValue> {
    @Override
    public CombinerOutputValue process(MutableEntry<OutputKey, CombinerOutputValue> entry, Object... arguments) throws EntryProcessorException {
        CombinerOutputValue value = entry.getValue();

        Verify.verify(arguments.length != 0);

        entry.setValue((CombinerOutputValue) arguments[0]);

        return value;
    }
}
