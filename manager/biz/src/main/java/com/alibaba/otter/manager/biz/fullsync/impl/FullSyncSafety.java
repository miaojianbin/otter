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
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class FullSyncSafety {

    private static final Pattern NAMED_CHECK_CONSTRAINT = Pattern.compile(
        "(?i)\\bCONSTRAINT\\s+(`[^`]+`|[^\\s]+)\\s+CHECK\\b");
    private static final Pattern NAMED_FOREIGN_KEY = Pattern.compile(
        "(?is)^\\s*CONSTRAINT\\s+(`[^`]+`|[^\\s]+)\\s+FOREIGN\\s+KEY\\b");
    private static final Pattern UNNAMED_FOREIGN_KEY = Pattern.compile("(?is)^\\s*FOREIGN\\s+KEY\\b");

    private FullSyncSafety() {
    }

    static void ensureNoSourceTargetOverlap(Map<String, String> sourceTables, Map<String, String> targetTables) {
        Set<String> overlap = new HashSet<String>(sourceTables.keySet());
        overlap.retainAll(targetTables.keySet());
        if (!overlap.isEmpty()) {
            String physicalTable = overlap.iterator().next();
            throw new IllegalStateException("source/target physical table overlap: source ["
                                            + sourceTables.get(physicalTable) + "] and target ["
                                            + targetTables.get(physicalTable) + "] resolve to MySQL table ["
                                            + physicalTable
                                            + "]; bidirectional and chained mappings are not supported");
        }
    }

    static void validateTemplateDdl(String ddl) {
        String ddlWithoutForeignKeys = removeForeignKeys(ddl);
        String upper = ddlWithoutForeignKeys.toUpperCase(Locale.ENGLISH);
        Matcher constraint = NAMED_CHECK_CONSTRAINT.matcher(ddlWithoutForeignKeys);
        if (constraint.find()) {
            throw new IllegalStateException("unsupported check constraint " + constraint.group(1));
        }
        if (upper.contains(" CONSTRAINT ")) {
            throw new IllegalStateException("unsupported named constraint (name unavailable)");
        }
        if (ddl.indexOf('(') < 0) throw new IllegalStateException("unexpected SHOW CREATE TABLE result");
    }

    static String ddlBody(String ddl) {
        String ddlWithoutForeignKeys = removeForeignKeys(ddl);
        int body = ddlWithoutForeignKeys.indexOf('(');
        if (body < 0) throw new IllegalStateException("unexpected SHOW CREATE TABLE result");
        return ddlWithoutForeignKeys.substring(body);
    }

    static String removeForeignKeys(String ddl) {
        int opening = ddl == null ? -1 : ddl.indexOf('(');
        if (opening < 0) throw new IllegalStateException("unexpected SHOW CREATE TABLE result");
        int closing = matchingClosingParenthesis(ddl, opening);
        if (closing < 0) throw new IllegalStateException("unbalanced SHOW CREATE TABLE result");
        List<String> definitions = splitDefinitions(ddl.substring(opening + 1, closing));
        StringBuilder retained = new StringBuilder();
        for (String definition : definitions) {
            if (isForeignKey(definition)) continue;
            if (retained.length() > 0) retained.append(',');
            retained.append(definition);
        }
        if (retained.length() == 0) {
            throw new IllegalStateException("SHOW CREATE TABLE contains no definition after foreign keys are removed");
        }
        return ddl.substring(0, opening + 1) + retained + ddl.substring(closing);
    }

    private static boolean isForeignKey(String definition) {
        return NAMED_FOREIGN_KEY.matcher(definition).find() || UNNAMED_FOREIGN_KEY.matcher(definition).find();
    }

    private static List<String> splitDefinitions(String body) {
        List<String> definitions = new ArrayList<String>();
        int start = 0;
        int depth = 0;
        char quote = 0;
        for (int i = 0; i < body.length(); i++) {
            char current = body.charAt(i);
            if (quote != 0) {
                if (current == '\\' && i + 1 < body.length()) {
                    i++;
                } else if (current == quote) {
                    if (i + 1 < body.length() && body.charAt(i + 1) == quote) i++;
                    else quote = 0;
                }
            } else if (current == '\'' || current == '"' || current == '`') {
                quote = current;
            } else if (current == '(') {
                depth++;
            } else if (current == ')') {
                depth--;
            } else if (current == ',' && depth == 0) {
                definitions.add(body.substring(start, i));
                start = i + 1;
            }
        }
        definitions.add(body.substring(start));
        return definitions;
    }

    private static int matchingClosingParenthesis(String ddl, int opening) {
        int depth = 0;
        char quote = 0;
        for (int i = opening; i < ddl.length(); i++) {
            char current = ddl.charAt(i);
            if (quote != 0) {
                if (current == '\\' && i + 1 < ddl.length()) {
                    i++;
                } else if (current == quote) {
                    if (i + 1 < ddl.length() && ddl.charAt(i + 1) == quote) i++;
                    else quote = 0;
                }
            } else if (current == '\'' || current == '"' || current == '`') {
                quote = current;
            } else if (current == '(') {
                depth++;
            } else if (current == ')' && --depth == 0) {
                return i;
            }
        }
        return -1;
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
