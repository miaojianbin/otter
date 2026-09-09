/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.web.webx.filter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

/**
 * Keeps application redirects on the origin used by the browser.
 *
 * WebX expands relative redirects with the servlet request URL. Behind a TLS
 * terminating reverse proxy that URL may contain the internal HTTP origin.
 */
public class RelativeRedirectFilter implements Filter {

    public void init(FilterConfig filterConfig) throws ServletException {
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                                                                                             throws IOException,
                                                                                             ServletException {
        if (response instanceof HttpServletResponse) {
            response = new HttpServletResponseWrapper((HttpServletResponse) response) {
                @Override
                public void sendRedirect(String location) throws IOException {
                    super.sendRedirect(toCurrentOrigin(location));
                }
            };
        }
        chain.doFilter(request, response);
    }

    public void destroy() {
    }

    static String toCurrentOrigin(String location) {
        if (location == null) {
            return null;
        }

        try {
            URI uri = new URI(location);
            String scheme = uri.getScheme();
            boolean networkPath = scheme == null && uri.getRawAuthority() != null;
            boolean http = "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme);
            if (!networkPath && !http) {
                return location;
            }

            StringBuilder relative = new StringBuilder();
            String path = uri.getRawPath();
            relative.append(path == null || path.length() == 0 ? "/" : path);
            if (uri.getRawQuery() != null) {
                relative.append('?').append(uri.getRawQuery());
            }
            if (uri.getRawFragment() != null) {
                relative.append('#').append(uri.getRawFragment());
            }
            return relative.toString();
        } catch (URISyntaxException e) {
            return location;
        }
    }
}
