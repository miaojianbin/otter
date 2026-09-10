/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.biz.common.arbitrate;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;

import com.alibaba.otter.manager.biz.config.channel.dal.ChannelDAO;
import com.alibaba.otter.manager.biz.config.channel.dal.dataobject.ChannelDO;
import com.alibaba.otter.manager.biz.config.pipeline.dal.PipelineDAO;
import com.alibaba.otter.manager.biz.config.pipeline.dal.dataobject.PipelineDO;
import com.alibaba.otter.shared.arbitrate.ArbitrateManageService;

/**
 * Restores the persistent ZooKeeper structure represented by the Manager database.
 * All underlying init operations are idempotent and preserve existing channel state.
 */
public class ZookeeperMetadataRepairService implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperMetadataRepairService.class);

    private ChannelDAO channelDao;
    private PipelineDAO pipelineDao;
    private ArbitrateManageService arbitrateManageService;
    private boolean startupRepairEnabled = true;

    public synchronized RepairResult repair() {
        initSystem();
        int channelCount = 0;
        int pipelineCount = 0;
        List<ChannelDO> channels = channelDao.listAll();
        if (channels == null) channels = Collections.emptyList();
        for (ChannelDO channel : channels) {
            if (channel == null || channel.getId() == null) {
                throw new IllegalStateException("cannot repair ZooKeeper metadata for a channel without an id");
            }
            initChannel(channel.getId());
            channelCount++;
            List<PipelineDO> pipelines = pipelineDao.listByChannelIds(channel.getId());
            if (pipelines == null) pipelines = Collections.emptyList();
            for (PipelineDO pipeline : pipelines) {
                if (pipeline == null || pipeline.getId() == null) {
                    throw new IllegalStateException("cannot repair ZooKeeper metadata for channel " + channel.getId()
                                                    + " because a pipeline has no id");
                }
                initPipeline(channel.getId(), pipeline.getId());
                pipelineCount++;
            }
        }
        return new RepairResult(channelCount, pipelineCount);
    }

    public void afterPropertiesSet() {
        if (!startupRepairEnabled) {
            logger.info("INFO ## automatic ZooKeeper metadata repair is disabled");
            return;
        }
        try {
            RepairResult result = repair();
            logger.info("INFO ## automatic ZooKeeper metadata repair completed: channels={}, pipelines={}",
                result.getChannelCount(), result.getPipelineCount());
        } catch (Throwable e) {
            // ZooKeeper may be temporarily unavailable. Manager must remain available so an administrator can retry.
            logger.error("ERROR ## automatic ZooKeeper metadata repair failed; Manager will continue starting", e);
        }
    }

    protected void initSystem() {
        arbitrateManageService.systemEvent().init();
    }

    protected void initChannel(Long channelId) {
        arbitrateManageService.channelEvent().init(channelId);
    }

    protected void initPipeline(Long channelId, Long pipelineId) {
        arbitrateManageService.pipelineEvent().init(channelId, pipelineId);
    }

    public void setChannelDao(ChannelDAO channelDao) {
        this.channelDao = channelDao;
    }

    public void setPipelineDao(PipelineDAO pipelineDao) {
        this.pipelineDao = pipelineDao;
    }

    public void setArbitrateManageService(ArbitrateManageService arbitrateManageService) {
        this.arbitrateManageService = arbitrateManageService;
    }

    public void setStartupRepairEnabled(boolean startupRepairEnabled) {
        this.startupRepairEnabled = startupRepairEnabled;
    }

    public static final class RepairResult {

        private final int channelCount;
        private final int pipelineCount;

        RepairResult(int channelCount, int pipelineCount) {
            this.channelCount = channelCount;
            this.pipelineCount = pipelineCount;
        }

        public int getChannelCount() {
            return channelCount;
        }

        public int getPipelineCount() {
            return pipelineCount;
        }
    }
}
