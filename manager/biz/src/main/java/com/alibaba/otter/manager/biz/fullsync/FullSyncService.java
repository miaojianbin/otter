/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.biz.fullsync;

import com.alibaba.otter.manager.biz.fullsync.dal.dataobject.FullSyncTaskDO;

public interface FullSyncService {

    FullSyncTaskDO create(Long channelId);

    FullSyncTaskDO findById(Long taskId);

    boolean isAvailable();

    void retryStart(Long taskId);
}
