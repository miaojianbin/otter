/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.biz.fullsync.impl;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

final class FullSyncSafety {

    private FullSyncSafety() {
    }

    static void ensureNoSourceTargetOverlap(Set<String> sourceTables, Set<String> targetTables) {
        Set<String> overlap = new HashSet<String>(sourceTables);
        overlap.retainAll(targetTables);
        if (!overlap.isEmpty()) {
            throw new IllegalStateException(
                "a target table is also used as a source table; bidirectional and chained mappings are not supported");
        }
    }

    static void validateTemplateDdl(String ddl) {
        String upper = ddl.toUpperCase(Locale.ENGLISH);
        if (upper.contains("FOREIGN KEY") || upper.contains(" CONSTRAINT ")) {
            throw new IllegalStateException("tables with named constraints or foreign keys are not supported by full sync");
        }
        if (ddl.indexOf('(') < 0) throw new IllegalStateException("unexpected SHOW CREATE TABLE result");
    }

    static String ddlBody(String ddl) {
        int body = ddl.indexOf('(');
        if (body < 0) throw new IllegalStateException("unexpected SHOW CREATE TABLE result");
        return ddl.substring(body);
    }

    static String temporaryTableName(String prefix, Long taskId, int index) {
        return prefix + taskId + "_" + index;
    }
}
