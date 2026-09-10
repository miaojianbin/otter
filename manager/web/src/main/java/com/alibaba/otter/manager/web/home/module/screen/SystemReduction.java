/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.otter.manager.web.home.module.screen;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.citrus.turbine.dataresolver.Param;
import com.alibaba.otter.manager.biz.common.arbitrate.ZookeeperMetadataRepairService;
import com.alibaba.otter.manager.biz.common.arbitrate.ZookeeperMetadataRepairService.RepairResult;

/**
 * @author simon 2011-10-25 上午10:00:32
 */
public class SystemReduction {

    private static final Logger    logger = LoggerFactory.getLogger(SystemReduction.class);

    @Resource(name = "zookeeperMetadataRepairService")
    private ZookeeperMetadataRepairService zookeeperMetadataRepairService;

    public void execute(@Param("command") String command, Context context) throws Exception {
        @SuppressWarnings("unchecked")
        String resultStr = "";

        if ("true".equals(command)) {

            try {
                RepairResult result = zookeeperMetadataRepairService.repair();
                resultStr = "恭喜！Zookeeper节点数据已经补全（Channel " + result.getChannelCount() + " 个，Pipeline "
                            + result.getPipelineCount() + " 个）";
            } catch (Exception e) {
                logger.error("ERROR ## init zookeeper has a problem ", e);
                resultStr = "出错了！恢复zookeeper的时候遇到问题：" + e.getMessage();
            }

        }

        context.put("resultStr", resultStr);

    }
}
