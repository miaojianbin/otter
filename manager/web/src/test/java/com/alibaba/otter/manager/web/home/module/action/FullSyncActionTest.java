/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.web.home.module.action;

import org.junit.Assert;
import org.junit.Test;

public class FullSyncActionTest {

    @Test
    public void rejectsMissingOrIncorrectConfirmation() {
        assertRejected(null);
        assertRejected("确定");
        assertRejected(" 确认 ");
    }

    private void assertRejected(String confirmation) {
        try {
            new FullSyncAction().doCreate(null, 1L, confirmation, null);
            Assert.fail("invalid confirmation must be rejected");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("确认"));
        }
    }
}
