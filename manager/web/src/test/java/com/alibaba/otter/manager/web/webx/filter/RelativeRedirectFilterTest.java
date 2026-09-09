/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.web.webx.filter;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class RelativeRedirectFilterTest {

    @Test
    public void convertsHttpRedirectToCurrentOrigin() {
        assertEquals("/channelList.htm?pageIndex=1&searchKey=a%20b",
            RelativeRedirectFilter.toCurrentOrigin(
                "http://manager-internal:8080/channelList.htm?pageIndex=1&searchKey=a%20b"));
    }

    @Test
    public void convertsHttpsRedirectAndPreservesFragment() {
        assertEquals("/channelList.htm#status",
            RelativeRedirectFilter.toCurrentOrigin("https://configured.example/channelList.htm#status"));
        assertEquals("/channelList.htm",
            RelativeRedirectFilter.toCurrentOrigin("//configured.example/channelList.htm"));
    }

    @Test
    public void usesRootForOriginOnlyRedirect() {
        assertEquals("/", RelativeRedirectFilter.toCurrentOrigin("http://manager-internal:8080"));
    }

    @Test
    public void leavesRelativeAndNonHttpRedirectsUnchanged() {
        assertEquals("channelList.htm", RelativeRedirectFilter.toCurrentOrigin("channelList.htm"));
        assertEquals("/channelList.htm", RelativeRedirectFilter.toCurrentOrigin("/channelList.htm"));
        assertEquals("mailto:admin@example.com", RelativeRedirectFilter.toCurrentOrigin("mailto:admin@example.com"));
        assertEquals("http://bad host/path", RelativeRedirectFilter.toCurrentOrigin("http://bad host/path"));
    }
}
