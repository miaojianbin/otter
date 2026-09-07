/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.deployer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
        server.addConnector(connector);
        server.setHandler(new AbstractHandler() {
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                               HttpServletResponse response) throws IOException, ServletException {
                response.sendRedirect("channelList.htm?pageIndex=1");
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
