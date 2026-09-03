/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.biz.fullsync.dal.ibatis;

import org.springframework.orm.ibatis.support.SqlMapClientDaoSupport;

import com.alibaba.otter.manager.biz.fullsync.dal.FullSyncTaskDAO;
import com.alibaba.otter.manager.biz.fullsync.dal.dataobject.FullSyncTaskDO;

public class IbatisFullSyncTaskDAO extends SqlMapClientDaoSupport implements FullSyncTaskDAO {

    public FullSyncTaskDO insert(FullSyncTaskDO task) {
        getSqlMapClientTemplate().insert("insertFullSyncTask", task);
        return task;
    }

    public FullSyncTaskDO findById(Long id) {
        return (FullSyncTaskDO) getSqlMapClientTemplate().queryForObject("findFullSyncTaskById", id);
    }

    public FullSyncTaskDO findActiveByChannelId(Long channelId) {
        return (FullSyncTaskDO) getSqlMapClientTemplate().queryForObject("findActiveFullSyncTaskByChannelId", channelId);
    }

    public void update(FullSyncTaskDO task) {
        getSqlMapClientTemplate().update("updateFullSyncTask", task);
    }

    public void failRunningTasks(String message) {
        getSqlMapClientTemplate().update("failRunningFullSyncTasks", message);
    }
}
