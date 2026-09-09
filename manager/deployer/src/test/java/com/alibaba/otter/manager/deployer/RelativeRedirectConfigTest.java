/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.deployer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.alibaba.otter.manager.web.webx.filter.RelativeRedirectFilter;
import org.eclipse.jetty.server.ForwardedRequestCustomizer;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.LocalConnector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.xml.XmlConfiguration;
import org.junit.Test;

public class RelativeRedirectConfigTest {

    @Test
    public void channelListShowsFullSyncProgressAndLocksOperations() throws Exception {
        String template = new String(Files.readAllBytes(
            Paths.get("src/main/resources/webapp/templates/home/screen/channelList.vm")), StandardCharsets.UTF_8);
        assertTrue(template.contains("#if($channel.fullSyncRunning)"));
        assertTrue(template.contains("查看同步进度"));
        assertTrue(template.contains("操作已锁定"));
        assertTrue(template.contains("fullSyncInfo.vm"));
    }

    @Test
    public void managerJettyPreservesExternalRedirectOrigin() throws Exception {
        Resource configXml = Resource.newSystemResource("jetty.xml");
        assertNotNull(configXml);
        Server server = (Server) new XmlConfiguration(configXml.getInputStream()).configure();
        try {
            ServerConnector connector = (ServerConnector) server.getConnectors()[0];
            HttpConnectionFactory factory = connector.getConnectionFactory(HttpConnectionFactory.class);
            assertNotNull(factory);
            HttpConfiguration configuration = factory.getHttpConfiguration();
            assertTrue(configuration.isRelativeRedirectAllowed());
            assertNotNull(configuration.getCustomizer(ForwardedRequestCustomizer.class));
            assertRelativeRedirect(configuration);
        } finally {
            server.destroy();
        }
    }

    private void assertRelativeRedirect(HttpConfiguration configuration) throws Exception {
        Server server = new Server();
        LocalConnector connector = new LocalConnector(server, new HttpConnectionFactory(configuration));
        final RelativeRedirectFilter redirectFilter = new RelativeRedirectFilter();
        server.addConnector(connector);
        server.setHandler(new AbstractHandler() {
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                               HttpServletResponse response) throws IOException, ServletException {
                redirectFilter.doFilter(request, response, new FilterChain() {
                    public void doFilter(ServletRequest request, ServletResponse response) throws IOException {
                        ((HttpServletResponse) response).sendRedirect(
                            "http://manager-internal:8080/channelList.htm?pageIndex=1");
                    }
                });
                baseRequest.setHandled(true);
            }
        });
        try {
            server.start();
            String response = connector.getResponse(
                "GET /operation.htm HTTP/1.1\r\nHost: manager-internal:8080\r\nConnection: close\r\n\r\n");
            assertTrue(response.contains("Location: /channelList.htm?pageIndex=1"));
        } finally {
            server.stop();
            server.destroy();
        }
    }
}
