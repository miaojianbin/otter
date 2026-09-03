/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.web.home.module.action;

import javax.annotation.Resource;

import com.alibaba.citrus.turbine.Navigator;
import com.alibaba.citrus.turbine.dataresolver.Param;
import com.alibaba.otter.manager.biz.fullsync.FullSyncService;
import com.alibaba.otter.manager.biz.fullsync.dal.dataobject.FullSyncTaskDO;

public class FullSyncAction {

    @Resource(name = "fullSyncService")
    private FullSyncService fullSyncService;

    public void doCreate(@Param("channelId") Long channelId, Navigator nav) {
        FullSyncTaskDO task = fullSyncService.create(channelId);
        nav.redirectToLocation("fullSyncInfo.htm?taskId=" + task.getId());
    }

    public void doRetryStart(@Param("taskId") Long taskId, Navigator nav) {
        fullSyncService.retryStart(taskId);
        nav.redirectToLocation("fullSyncInfo.htm?taskId=" + taskId);
    }
}
