package com.fnklabs.draenei;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class HazelcastInstanceTest {

    @Test
    public void testSubmitToKey() throws Exception {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
        IMap<String, Long> map = hazelcastInstance.<String, Long>getMap("test.map");


        map.submitToKey("1", new AbstractEntryProcessor<String, Long>() {
            @Override
            public Object process(Map.Entry<String, Long> entry) {
                entry.setValue(11l);
                return true;
            }
        });

        Long result = map.get("1");


        Assert.assertEquals(11l, result.longValue());
    }
}
