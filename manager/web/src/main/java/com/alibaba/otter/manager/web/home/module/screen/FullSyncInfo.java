/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.web.home.module.screen;

import javax.annotation.Resource;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.citrus.turbine.dataresolver.Param;
import com.alibaba.otter.manager.biz.fullsync.FullSyncService;

public class FullSyncInfo {

    @Resource(name = "fullSyncService")
    private FullSyncService fullSyncService;

    public void execute(@Param("taskId") Long taskId, Context context) {
        context.put("task", fullSyncService.findById(taskId));
    }
}
