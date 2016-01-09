package com.fnklabs.draenei.analytics.search;

import org.junit.Assert;
import org.junit.Test;

public class TfIdfUtilsTest {

    @Test
    public void testCalculateTfIdf() throws Exception {
        double result = TfIdfUtils.calculateTfIdf(0, 0);

        Assert.assertEquals(0, result, 0);


        result = TfIdfUtils.calculateTfIdf(1, 0);

        Assert.assertEquals(0, result, 0);

        result = TfIdfUtils.calculateTfIdf(1, 1);

        Assert.assertEquals(1, result, 0);

        result = TfIdfUtils.calculateTfIdf(1, 2);

        Assert.assertEquals(2, result, 0);

        result = TfIdfUtils.calculateTfIdf(2, 1);

        Assert.assertEquals(2, result, 0);
    }

    @Test
    public void testCalculateIdf() throws Exception {
    }

    @Test
    public void testCalculateTf() throws Exception {
        double result = TfIdfUtils.calculateTf(0, 0);

        Assert.assertEquals(0, result, 0);


        result = TfIdfUtils.calculateTf(1, 0);

        Assert.assertEquals(0, result, 0);

        result = TfIdfUtils.calculateTf(1, 1);

        Assert.assertEquals(1, result, 0);

        result = TfIdfUtils.calculateTf(2, 2);

        Assert.assertEquals(1, result, 0);

        result = TfIdfUtils.calculateTf(10, 2);

        Assert.assertEquals(5, result, 0);
    }
}