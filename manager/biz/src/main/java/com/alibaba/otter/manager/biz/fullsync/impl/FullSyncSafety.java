/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.biz.fullsync.impl;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class FullSyncSafety {

    private static final Pattern NAMED_CONSTRAINT = Pattern.compile(
        "(?i)\\bCONSTRAINT\\s+(`[^`]+`|[^\\s]+)\\s+(FOREIGN\\s+KEY|CHECK)");

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
        Matcher constraint = NAMED_CONSTRAINT.matcher(ddl);
        if (constraint.find()) {
            throw new IllegalStateException("unsupported " + constraint.group(2).toLowerCase(Locale.ENGLISH)
                                            + " constraint " + constraint.group(1));
        }
        if (upper.contains("FOREIGN KEY")) {
            throw new IllegalStateException("unsupported foreign key constraint (name unavailable)");
        }
        if (upper.contains(" CONSTRAINT ")) {
            throw new IllegalStateException("unsupported named constraint (name unavailable)");
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

    static List<String> writableColumns(List<String> columns, Set<String> generatedColumns) {
        List<String> writable = new ArrayList<String>();
        for (String column : columns) {
            if (!generatedColumns.contains(column.toLowerCase(Locale.ENGLISH))) writable.add(column);
        }
        return writable;
    }

    static InetSocketAddress requireSingleMediaMaster(String master, String slave) {
        if (slave != null && slave.trim().length() > 0) {
            throw new IllegalStateException("full sync does not support Canal media HA with a standby database");
        }
        if (master == null || master.trim().length() == 0) {
            throw new IllegalStateException("Canal media HA master address is missing");
        }
        String value = master.trim();
        int separator = value.lastIndexOf(':');
        if (separator <= 0 || separator == value.length() - 1) {
            throw new IllegalStateException("invalid Canal media HA master address: " + value);
        }
        String host = value.substring(0, separator).trim();
        if (host.startsWith("[") && host.endsWith("]")) host = host.substring(1, host.length() - 1);
        try {
            int port = Integer.parseInt(value.substring(separator + 1).trim());
            if (host.length() == 0 || port < 1 || port > 65535) throw new NumberFormatException();
            return new InetSocketAddress(host, port);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("invalid Canal media HA master address: " + value);
        }
    }
}
