/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.web.home.module.action;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.citrus.service.form.Group;
import com.alibaba.citrus.turbine.Navigator;
import com.alibaba.citrus.turbine.dataresolver.FormGroup;
import com.alibaba.citrus.turbine.dataresolver.Param;
import com.alibaba.otter.manager.biz.fullsync.FullSyncService;
import com.alibaba.otter.manager.biz.fullsync.dal.dataobject.FullSyncTaskDO;

public class FullSyncAction {

    private static final Logger logger = LoggerFactory.getLogger(FullSyncAction.class);

    @Resource(name = "fullSyncService")
    private FullSyncService fullSyncService;

    public void doCreate(@FormGroup("csrfCheck") Group csrfCheck, @Param("channelId") Long channelId,
                         @Param("confirmation") String confirmation, Navigator nav) {
        logger.warn("WARN ## entered full sync create action for channel {}, confirmationValid={}", channelId,
                    "确认".equals(confirmation));
        if (!"确认".equals(confirmation)) {
            throw new IllegalArgumentException("请输入“确认”后再执行全量同步");
        }
        logger.warn("WARN ## full sync requested for channel {}", channelId);
        FullSyncTaskDO task;
        try {
            task = fullSyncService.create(channelId);
        } catch (RuntimeException e) {
            logger.error("ERROR ## failed to create full sync task for channel " + channelId, e);
            throw e;
        }
        logger.warn("WARN ## full sync task {} created for channel {}", task.getId(), channelId);
        nav.redirectToLocation("fullSyncInfo.htm?taskId=" + task.getId());
    }

    public void doRetryStart(@FormGroup("csrfCheck") Group csrfCheck, @Param("taskId") Long taskId, Navigator nav) {
        logger.warn("WARN ## entered full sync retry action for task {}", taskId);
        fullSyncService.retryStart(taskId);
        nav.redirectToLocation("fullSyncInfo.htm?taskId=" + taskId);
    }
}
