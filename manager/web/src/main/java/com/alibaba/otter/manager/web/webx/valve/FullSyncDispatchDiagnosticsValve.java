/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.web.webx.valve;

import static com.alibaba.citrus.turbine.util.TurbineUtil.getTurbineRunData;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.citrus.service.moduleloader.ModuleLoaderException;
import com.alibaba.citrus.service.moduleloader.ModuleLoaderService;
import com.alibaba.citrus.service.pipeline.PipelineContext;
import com.alibaba.citrus.service.pipeline.support.AbstractValve;
import com.alibaba.citrus.turbine.TurbineRunData;

/**
 * Emits a compact dispatch trace for the full-sync workflow without logging credentials or form contents.
 */
public class FullSyncDispatchDiagnosticsValve extends AbstractValve {

    private static final Logger logger           = LoggerFactory.getLogger(FullSyncDispatchDiagnosticsValve.class);
    private static final String FULL_SYNC_ACTION = "full_sync_action";

    @Autowired
    private HttpServletRequest request;

    @Autowired
    private ModuleLoaderService moduleLoaderService;

    public void invoke(PipelineContext pipelineContext) throws Exception {
        String rawAction = request.getParameter("action");
        if (!FULL_SYNC_ACTION.equals(rawAction)) {
            pipelineContext.invokeNext();
            return;
        }

        TurbineRunData rundata = getTurbineRunData(request);
        String resolvedAction = rundata.getAction();
        boolean moduleAvailable = isModuleAvailable(resolvedAction);
        logger.warn("WARN ## full sync dispatch received: method={}, uri={}, rawAction={}, resolvedAction={}, "
                    + "resolvedEvent={}, moduleAvailable={}, channelId={}, taskId={}", request.getMethod(),
                    request.getRequestURI(), rawAction, resolvedAction, rundata.getActionEvent(), moduleAvailable,
                    request.getParameter("channelId"), request.getParameter("taskId"));

        try {
            pipelineContext.invokeNext();
        } finally {
            logger.warn("WARN ## full sync dispatch completed: resolvedAction={}, actionMarkedExecuted={}, "
                        + "redirected={}, redirectTarget={}, redirectLocation={}", resolvedAction,
                        request.getAttribute("_action_" + resolvedAction) != null, rundata.isRedirected(),
                        rundata.getRedirectTarget(), rundata.getRedirectLocation());
        }
    }

    private boolean isModuleAvailable(String action) {
        if (action == null) {
            return false;
        }
        try {
            return moduleLoaderService.getModuleQuiet("action", action) != null;
        } catch (ModuleLoaderException e) {
            logger.error("ERROR ## failed to inspect full sync action module " + action, e);
            return false;
        }
    }
}
