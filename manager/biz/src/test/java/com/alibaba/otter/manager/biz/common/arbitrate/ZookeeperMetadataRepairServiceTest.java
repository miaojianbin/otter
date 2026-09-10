/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.biz.common.arbitrate;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.alibaba.otter.manager.biz.common.arbitrate.ZookeeperMetadataRepairService.RepairResult;
import com.alibaba.otter.manager.biz.config.channel.dal.ChannelDAO;
import com.alibaba.otter.manager.biz.config.channel.dal.dataobject.ChannelDO;
import com.alibaba.otter.manager.biz.config.pipeline.dal.PipelineDAO;
import com.alibaba.otter.manager.biz.config.pipeline.dal.dataobject.PipelineDO;

public class ZookeeperMetadataRepairServiceTest {

    @Test
    public void repairsSystemChannelsAndPipelinesFromDatabase() {
        ChannelDO first = channel(1L);
        ChannelDO second = channel(2L);
        final PipelineDO firstPipeline = pipeline(11L, 1L);
        final PipelineDO secondPipeline = pipeline(21L, 2L);
        final PipelineDO thirdPipeline = pipeline(22L, 2L);

        RecordingRepairService service = new RecordingRepairService();
        service.setChannelDao(proxy(ChannelDAO.class, new InvocationHandler() {
            public Object invoke(Object proxy, Method method, Object[] args) {
                if ("listAll".equals(method.getName())) return Arrays.asList(channel(1L), channel(2L));
                return null;
            }
        }));
        service.setPipelineDao(proxy(PipelineDAO.class, new InvocationHandler() {
            public Object invoke(Object proxy, Method method, Object[] args) {
                Long channelId = (Long) ((Object[]) args[0])[0];
                if (Long.valueOf(1L).equals(channelId)) return Collections.singletonList(firstPipeline);
                return Arrays.asList(secondPipeline, thirdPipeline);
            }
        }));

        RepairResult result = service.repair();

        Assert.assertTrue(service.systemInitialized);
        Assert.assertEquals(service.channels, Arrays.asList(first.getId(), second.getId()));
        Assert.assertEquals(service.pipelines, Arrays.asList("1:11", "2:21", "2:22"));
        Assert.assertEquals(result.getChannelCount(), 2);
        Assert.assertEquals(result.getPipelineCount(), 3);
    }

    private static ChannelDO channel(Long id) {
        ChannelDO channel = new ChannelDO();
        channel.setId(id);
        return channel;
    }

    private static PipelineDO pipeline(Long id, Long channelId) {
        PipelineDO pipeline = new PipelineDO();
        pipeline.setId(id);
        pipeline.setChannelId(channelId);
        return pipeline;
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class[] { type }, handler);
    }

    private static final class RecordingRepairService extends ZookeeperMetadataRepairService {

        private boolean systemInitialized;
        private final List<Long> channels = new java.util.ArrayList<Long>();
        private final List<String> pipelines = new java.util.ArrayList<String>();

        protected void initSystem() {
            systemInitialized = true;
        }

        protected void initChannel(Long channelId) {
            channels.add(channelId);
        }

        protected void initPipeline(Long channelId, Long pipelineId) {
            pipelines.add(channelId + ":" + pipelineId);
        }
    }
}
