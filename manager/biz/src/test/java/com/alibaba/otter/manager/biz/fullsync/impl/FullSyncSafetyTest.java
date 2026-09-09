/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.biz.fullsync.impl;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FullSyncSafetyTest {

    @Test
    public void rejectsSourceTargetOverlap() {
        Set<String> sources = new HashSet<String>();
        sources.add("server-a:db:source");
        sources.add("server-b:db:shared");
        Set<String> targets = new HashSet<String>();
        targets.add("server-b:db:shared");

        try {
            FullSyncSafety.ensureNoSourceTargetOverlap(sources, targets);
            Assert.fail("overlapping source and target must be rejected");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("target table"));
        }
    }

    @Test
    public void acceptsIndependentSourceAndTargetTables() {
        FullSyncSafety.ensureNoSourceTargetOverlap(Collections.singleton("server-a:db:orders"),
            Collections.singleton("server-b:db:orders"));
    }

    @Test
    public void rejectsForeignKeyDdl() {
        try {
            FullSyncSafety.validateTemplateDdl(
                "CREATE TABLE `child` (`id` bigint, CONSTRAINT `fk_parent` FOREIGN KEY (`id`) REFERENCES `parent` (`id`))");
            Assert.fail("foreign-key DDL must be rejected");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("foreign keys"));
        }
    }

    @Test
    public void extractsValidatedDdlBody() {
        String ddl = "CREATE TABLE `orders` (`id` bigint NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB";
        FullSyncSafety.validateTemplateDdl(ddl);
        Assert.assertEquals("(`id` bigint NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB", FullSyncSafety.ddlBody(ddl));
    }

    @Test
    public void createsShortTaskScopedTemporaryNames() {
        Assert.assertEquals("__otter_fs_42_3", FullSyncSafety.temporaryTableName("__otter_fs_", 42L, 3));
        Assert.assertTrue(FullSyncSafety.temporaryTableName("__otter_bak_", Long.MAX_VALUE, 999).length() <= 64);
    }

    @Test
    public void acceptsMediaHaWithOnlyAMaster() {
        InetSocketAddress address = FullSyncSafety.requireSingleMediaMaster("127.0.0.1:3306", " ");
        Assert.assertEquals(address.getAddress().getHostAddress(), "127.0.0.1");
        Assert.assertEquals(address.getPort(), 3306);
    }

    @Test
    public void rejectsMediaHaWithAStandby() {
        try {
            FullSyncSafety.requireSingleMediaMaster("127.0.0.1:3306", "127.0.0.2:3306");
            Assert.fail("media HA with a standby must be rejected");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("standby"));
        }
    }

    @Test
    public void rejectsInvalidMediaHaMasterAddress() {
        try {
            FullSyncSafety.requireSingleMediaMaster("127.0.0.1", null);
            Assert.fail("an invalid media HA master must be rejected");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("master address"));
        }
    }
}
