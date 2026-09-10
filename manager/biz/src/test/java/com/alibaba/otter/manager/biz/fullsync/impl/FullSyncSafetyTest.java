/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.biz.fullsync.impl;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class FullSyncSafetyTest {

    @Test
    public void rejectsSourceTargetOverlap() {
        Map<String, String> sources = new LinkedHashMap<String, String>();
        sources.put("server-a:db:source", "source-a");
        sources.put("server-b:db:shared", "source-shared");
        Map<String, String> targets = new LinkedHashMap<String, String>();
        targets.put("server-b:db:shared", "target-shared");

        try {
            FullSyncSafety.ensureNoSourceTargetOverlap(sources, targets);
            Assert.fail("overlapping source and target must be rejected");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("source-shared"));
            Assert.assertTrue(expected.getMessage().contains("target-shared"));
        }
    }

    @Test
    public void acceptsIndependentSourceAndTargetTables() {
        Map<String, String> sources = Collections.singletonMap("server-a:db:orders", "source-orders");
        Map<String, String> targets = Collections.singletonMap("server-b:db:orders", "target-orders");
        FullSyncSafety.ensureNoSourceTargetOverlap(sources, targets);
    }

    @Test
    public void removesForeignKeysFromTargetDdl() {
        String ddl = "CREATE TABLE `child` (\n"
                     + "  `id` bigint NOT NULL,\n"
                     + "  `kind` varchar(20) DEFAULT 'a,b',\n"
                     + "  KEY `idx_parent` (`id`),\n"
                     + "  CONSTRAINT `fk_parent` FOREIGN KEY (`id`) REFERENCES `parent` (`id`) ON DELETE CASCADE\n"
                     + ") ENGINE=InnoDB";
        FullSyncSafety.validateTemplateDdl(ddl);
        String body = FullSyncSafety.ddlBody(ddl);
        Assert.assertFalse(body.contains("FOREIGN KEY"));
        Assert.assertFalse(body.contains("fk_parent"));
        Assert.assertTrue(body.contains("KEY `idx_parent`"));
        Assert.assertTrue(body.contains("DEFAULT 'a,b'"));
        Assert.assertTrue(body.endsWith("ENGINE=InnoDB"));
    }

    @Test
    public void removesMultipleNamedAndUnnamedForeignKeys() {
        String ddl = "CREATE TABLE `child` (`id` bigint, `owner_id` bigint, "
                     + "FOREIGN KEY (`id`) REFERENCES `parent` (`id`), "
                     + "CONSTRAINT `fk_owner` FOREIGN KEY (`owner_id`) REFERENCES `owner` (`id`), "
                     + "PRIMARY KEY (`id`)) ENGINE=InnoDB";
        String body = FullSyncSafety.ddlBody(ddl);
        Assert.assertFalse(body.toUpperCase().contains("FOREIGN KEY"));
        Assert.assertTrue(body.contains("PRIMARY KEY (`id`)"));
        Assert.assertTrue(body.contains("`owner_id` bigint"));
    }

    @Test
    public void identifiesNamedCheckConstraint() {
        try {
            FullSyncSafety.validateTemplateDdl(
                "CREATE TABLE `orders` (`amount` int, CONSTRAINT `chk_amount` CHECK ((`amount` >= 0)))");
            Assert.fail("named check constraint must be rejected");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("check"));
            Assert.assertTrue(expected.getMessage().contains("`chk_amount`"));
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
    public void excludesGeneratedColumnsFromExplicitWrites() {
        List<String> writable = FullSyncSafety.writableColumns(Arrays.asList("id", "type_code_numeric", "name"),
            Collections.singleton("type_code_numeric"));
        Assert.assertEquals(writable, Arrays.asList("id", "name"));
    }

    @Test
    public void excludesGeneratedColumnsCaseInsensitively() {
        List<String> writable = FullSyncSafety.writableColumns(Arrays.asList("ID", "TYPE_CODE_NUMERIC"),
            Collections.singleton("type_code_numeric"));
        Assert.assertEquals(writable, Collections.singletonList("ID"));
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
