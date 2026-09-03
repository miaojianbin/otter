/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.biz.fullsync.dal;

import com.alibaba.otter.manager.biz.fullsync.dal.dataobject.FullSyncTaskDO;

public interface FullSyncTaskDAO {

    FullSyncTaskDO insert(FullSyncTaskDO task);

    FullSyncTaskDO findById(Long id);

    FullSyncTaskDO findActiveByChannelId(Long channelId);

    void update(FullSyncTaskDO task);

    void failRunningTasks(String message);
}
