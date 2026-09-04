/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.biz.fullsync.impl;

import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.utils.JsonUtils;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogIdentity;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.manager.biz.common.DataSourceCreator;
import com.alibaba.otter.manager.biz.config.canal.CanalService;
import com.alibaba.otter.manager.biz.config.channel.ChannelService;
import com.alibaba.otter.manager.biz.config.channel.dal.ChannelDAO;
import com.alibaba.otter.manager.biz.config.channel.dal.dataobject.ChannelDO;
import com.alibaba.otter.manager.biz.config.datamediapair.DataMediaPairService;
import com.alibaba.otter.manager.biz.config.pipeline.PipelineService;
import com.alibaba.otter.manager.biz.fullsync.FullSyncService;
import com.alibaba.otter.manager.biz.fullsync.dal.FullSyncTaskDAO;
import com.alibaba.otter.manager.biz.fullsync.dal.dataobject.FullSyncTaskDO;
import com.alibaba.otter.shared.arbitrate.ArbitrateManageService;
import com.alibaba.otter.shared.arbitrate.impl.zookeeper.ZooKeeperClient;
import com.alibaba.otter.shared.common.model.config.channel.ChannelStatus;
import com.alibaba.otter.shared.common.model.config.data.ColumnPair;
import com.alibaba.otter.shared.common.model.config.data.DataMedia;
import com.alibaba.otter.shared.common.model.config.data.DataMediaPair;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.common.model.config.data.db.DbMediaSource;
import com.alibaba.otter.shared.common.model.config.pipeline.Pipeline;
import com.alibaba.otter.shared.common.utils.zookeeper.ZkClientx;

public class FullSyncServiceImpl implements FullSyncService, InitializingBean, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(FullSyncServiceImpl.class);
    private static final int BATCH_SIZE = 500;
    private static final String RUNNING = "RUNNING";
    private static final String SUCCESS = "SUCCESS";
    private static final String FAILED = "FAILED";

    private FullSyncTaskDAO fullSyncTaskDao;
    private ChannelDAO channelDao;
    private ChannelService channelService;
    private PipelineService pipelineService;
    private DataMediaPairService dataMediaPairService;
    private CanalService canalService;
    private ArbitrateManageService arbitrateManageService;
    private DataSourceCreator dataSourceCreator;
    private TransactionTemplate transactionTemplate;
    private ExecutorService executor;
    private volatile boolean available;

    public FullSyncTaskDO create(final Long channelId) {
        requireAvailable();
        final FullSyncTaskDO task = transactionTemplate.execute(new TransactionCallback<FullSyncTaskDO>() {
            public FullSyncTaskDO doInTransaction(TransactionStatus status) {
                ChannelDO channel = channelDao.findByIdForUpdate(channelId);
                if (channel == null) throw new IllegalArgumentException("channel does not exist");
                requireStopped(channelId);
                if (fullSyncTaskDao.findActiveByChannelId(channelId) != null) {
                    throw new IllegalStateException("channel already has a running full sync task");
                }
                FullSyncTaskDO created = new FullSyncTaskDO();
                created.setChannelId(channelId);
                created.setStatus(RUNNING);
                created.setStage("PRECHECK");
                created.setTotalRows(0L);
                created.setCopiedRows(0L);
                created.setMessage("任务已创建，等待执行");
                return fullSyncTaskDao.insert(created);
            }
        });
        try {
            executor.submit(new Runnable() {
                public void run() {
                    execute(task.getId());
                }
            });
        } catch (RuntimeException e) {
            update(task, FAILED, "PRECHECK", "任务提交失败: " + message(e));
            throw e;
        }
        return task;
    }

    public FullSyncTaskDO findById(Long taskId) {
        requireAvailable();
        return fullSyncTaskDao.findById(taskId);
    }

    public boolean isAvailable() {
        return available;
    }

    public synchronized void retryStart(Long taskId) {
        requireAvailable();
        FullSyncTaskDO task = requireTask(taskId);
        if (!FAILED.equals(task.getStatus()) || !"START_CHANNEL".equals(task.getStage())) {
            throw new IllegalStateException("only a task that failed while starting the channel can be retried");
        }
        requireStopped(task.getChannelId());
        try {
            channelService.startChannelByFullSync(task.getChannelId());
            cleanupBackups(task);
            update(task, SUCCESS, "DONE", "全量数据已导入，Channel 已启动");
        } catch (Exception e) {
            update(task, FAILED, "START_CHANNEL", message(e));
            throw new IllegalStateException("start channel failed", e);
        }
    }

    private void execute(Long taskId) {
        FullSyncTaskDO task = requireTask(taskId);
        List<TargetGroup> groups = Collections.emptyList();
        boolean targetsSwitched = false;
        boolean channelStartAttempted = false;
        try {
            requireStopped(task.getChannelId());
            List<Pipeline> pipelines = pipelineService.listByChannelIds(task.getChannelId());
            if (pipelines.size() != 1) {
                throw new IllegalStateException("full sync supports a single unidirectional pipeline only");
            }
            List<DataMediaPair> pairs = loadPairs(pipelines);
            groups = buildGroups(pairs, taskId);
            validateSharedDestinations(task.getChannelId(), pipelines);
            validateCanalConfigurations(pipelines);
            validateGroups(groups);
            preflightGroups(groups);

            requireStopped(task.getChannelId());
            update(task, RUNNING, "RESET_POSITION", "正在清理旧批次并写入当前 Canal 位点");
            Map<String, LogPosition> positions = resetPositions(pipelines);

            update(task, RUNNING, "CREATE_STAGING", "正在创建全量同步临时表");
            createStagingTables(groups);
            long total = countRows(groups);
            task.setTotalRows(total);
            task.setCopiedRows(0L);
            update(task, RUNNING, "COPY_DATA", "正在导入全量数据到临时表");
            for (TargetGroup group : groups) {
                for (DataMediaPair pair : group.pairs) copyPair(task, pair, group);
            }

            requireStopped(task.getChannelId());
            verifyPositions(pipelines, positions);
            update(task, RUNNING, "SWITCH_TABLE", "正在切换全量数据表");
            switchTargets(task.getChannelId(), groups);
            targetsSwitched = true;

            requireStopped(task.getChannelId());
            verifyPositions(pipelines, positions);
            update(task, RUNNING, "START_CHANNEL", "数据导入完成，正在启动 Channel");
            channelStartAttempted = true;
            channelService.startChannelByFullSync(task.getChannelId());
            cleanupBackups(groups);
            update(task, SUCCESS, "DONE", "全量同步完成，Channel 已启动");
        } catch (Exception e) {
            logger.error("ERROR ## full sync task " + taskId + " failed", e);
            if (targetsSwitched && !channelStartAttempted) rollbackTargetSwitches(groups);
            else if (!targetsSwitched) cleanupStagingTables(groups);
            String stage = task.getStage() == null ? "PRECHECK" : task.getStage();
            update(task, FAILED, stage, message(e));
        }
    }

    private List<DataMediaPair> loadPairs(List<Pipeline> pipelines) {
        List<DataMediaPair> pairs = new ArrayList<DataMediaPair>();
        for (Pipeline pipeline : pipelines) pairs.addAll(dataMediaPairService.listByPipelineId(pipeline.getId()));
        if (pairs.isEmpty()) throw new IllegalStateException("channel has no data media pair");
        return pairs;
    }

    private List<TargetGroup> buildGroups(List<DataMediaPair> pairs, Long taskId) {
        Map<String, TargetGroup> result = new LinkedHashMap<String, TargetGroup>();
        Collections.sort(pairs, new Comparator<DataMediaPair>() {
            public int compare(DataMediaPair left, DataMediaPair right) {
                return left.getId().compareTo(right.getId());
            }
        });
        for (DataMediaPair pair : pairs) {
            DataMedia target = pair.getTarget();
            String key = target.getSource().getId() + ":" + target.getNamespaceMode().getSingleValue() + ":"
                         + target.getNameMode().getSingleValue();
            TargetGroup group = result.get(key);
            if (group == null) {
                group = new TargetGroup(target);
                result.put(key, group);
            }
            group.pairs.add(pair);
        }
        List<TargetGroup> groups = new ArrayList<TargetGroup>(result.values());
        for (int i = 0; i < groups.size(); i++) groups.get(i).prepareNames(taskId, i);
        return groups;
    }

    private void validateSharedDestinations(Long channelId, List<Pipeline> pipelines) {
        Set<String> destinations = new HashSet<String>();
        for (Pipeline pipeline : pipelines) destinations.add(pipeline.getParameters().getDestinationName());
        for (String destination : destinations) {
            for (Pipeline other : pipelineService.listByDestinationWithoutOther(destination)) {
                if (!channelId.equals(other.getChannelId())) {
                    throw new IllegalStateException("destination " + destination + " is used by another channel");
                }
            }
        }
    }

    private void validateCanalConfigurations(List<Pipeline> pipelines) {
        for (Pipeline pipeline : pipelines) {
            String destination = pipeline.getParameters().getDestinationName();
            if (StringUtils.isBlank(destination) || pipeline.getParameters().getMainstemClientId() == null) {
                throw new IllegalStateException("pipeline Canal destination or client id is missing");
            }
            Canal canal = canalService.findByName(destination);
            if (canal == null || canal.getCanalParameter() == null) {
                throw new IllegalStateException("Canal configuration is missing for destination " + destination);
            }
            CanalParameter parameter = canal.getCanalParameter();
            if (parameter.getSourcingType() == null || !parameter.getSourcingType().isMysql()) {
                throw new IllegalStateException("full sync supports a direct MySQL Canal source only");
            }
            if (parameter.getHaMode() != null && parameter.getHaMode().isMedia()) {
                throw new IllegalStateException("full sync does not support Canal media HA");
            }
            if (parameter.getMetaMode() == null
                || (!parameter.getMetaMode().isZookeeper() && !parameter.getMetaMode().isMixed())) {
                throw new IllegalStateException("full sync requires Canal meta mode ZOOKEEPER or MIXED");
            }
            if (parameter.getIndexMode() == null
                || (!parameter.getIndexMode().isMeta() && !parameter.getIndexMode().isMemoryMetaFailback())) {
                throw new IllegalStateException("full sync requires Canal index mode META or MEMORY_META_FAILBACK");
            }
            if (parameter.getGroupDbAddresses().size() != 1
                || parameter.getGroupDbAddresses().get(0).size() != 1
                || parameter.getGroupDbAddresses().get(0).get(0).getType() == null
                || !parameter.getGroupDbAddresses().get(0).get(0).getType().isMysql()) {
                throw new IllegalStateException("full sync supports one Canal MySQL address only");
            }
        }
    }

    private void validateGroups(List<TargetGroup> groups) {
        for (TargetGroup group : groups) {
            requireDirectMysql(group.target.getSource());
            for (DataMediaPair pair : group.pairs) {
                requireDirectMysql(pair.getSource().getSource());
                if (!pair.getSource().getNamespaceMode().getMode().isSingle()
                    || !pair.getSource().getNameMode().getMode().isSingle()
                    || !pair.getTarget().getNamespaceMode().getMode().isSingle()
                    || !pair.getTarget().getNameMode().getMode().isSingle()) {
                    throw new IllegalStateException("full sync supports single source and target tables only");
                }
                if (pair.isExistFilter() || pair.isExistResolver()) {
                    throw new IllegalStateException("full sync does not support filter or resolver mappings");
                }
                for (ColumnPair columnPair : pair.getColumnPairs()) {
                    if (!StringUtils.equals(columnPair.getSourceColumn().getName(), columnPair.getTargetColumn().getName())) {
                        throw new IllegalStateException("full sync does not support renamed column mappings");
                    }
                }
            }
        }
    }

    private void preflightGroups(List<TargetGroup> groups) throws Exception {
        Set<String> sourceTables = new HashSet<String>();
        Set<String> targetTables = new HashSet<String>();
        Map<Long, String> serverIdentities = new HashMap<Long, String>();
        for (TargetGroup group : groups) {
            group.physicalKey = physicalTableKey(group.target, serverIdentities);
            if (!targetTables.add(group.physicalKey)) {
                throw new IllegalStateException("multiple target definitions resolve to the same physical table");
            }
            validateTargetReplaceable(group);
            verifyTargetPrivileges(group);
            DataMediaPair template = group.pairs.get(0);
            DataSource source = dataSourceCreator.createDataSource(template.getSource().getSource());
            try {
                group.templateDdl = showCreateTable(source, table(template.getSource()));
                FullSyncSafety.validateTemplateDdl(group.templateDdl);
                group.templateColumns = readAllColumnDefinitions(source, template.getSource());
            } finally {
                dataSourceCreator.destroyDataSource(source);
            }
            for (DataMediaPair pair : group.pairs) {
                sourceTables.add(physicalTableKey(pair.getSource(), serverIdentities));
                DataSource pairSource = dataSourceCreator.createDataSource(pair.getSource().getSource());
                try {
                    Map<String, ColumnDefinition> columns = readColumnDefinitions(pairSource, pair);
                    for (Map.Entry<String, ColumnDefinition> column : columns.entrySet()) {
                        ColumnDefinition templateColumn = group.templateColumns.get(column.getKey());
                        if (templateColumn == null) {
                            throw new IllegalStateException("source column " + column.getKey()
                                                            + " is absent from target table template");
                        }
                        if (!templateColumn.equals(column.getValue())) {
                            throw new IllegalStateException("source column " + column.getKey()
                                                            + " is incompatible with target table template");
                        }
                    }
                } finally {
                    dataSourceCreator.destroyDataSource(pairSource);
                }
            }
        }
        FullSyncSafety.ensureNoSourceTargetOverlap(sourceTables, targetTables);
    }

    private void validateTargetReplaceable(TargetGroup group) throws Exception {
        DataSource target = dataSourceCreator.createDataSource(group.target.getSource());
        try {
            Connection connection = target.getConnection();
            try {
                if (queryCount(connection,
                    "SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE "
                    + "WHERE REFERENCED_TABLE_SCHEMA=? AND REFERENCED_TABLE_NAME=?",
                    group.schema,
                    group.targetName) > 0) {
                    throw new IllegalStateException("target tables referenced by foreign keys are not supported by full sync");
                }
                if (queryCount(connection,
                    "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA=? AND EVENT_OBJECT_TABLE=?",
                    group.schema,
                    group.targetName) > 0) {
                    throw new IllegalStateException("target tables with triggers are not supported by full sync");
                }
            } finally {
                connection.close();
            }
        } finally {
            dataSourceCreator.destroyDataSource(target);
        }
    }

    private long queryCount(Connection connection, String sql, String schema, String tableName) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        try {
            statement.setString(1, schema);
            statement.setString(2, tableName);
            ResultSet result = statement.executeQuery();
            try {
                if (!result.next()) throw new SQLException("metadata count query returned no row");
                return result.getLong(1);
            } finally {
                result.close();
            }
        } finally {
            statement.close();
        }
    }

    private void verifyTargetPrivileges(TargetGroup group) throws Exception {
        DataSource target = dataSourceCreator.createDataSource(group.target.getSource());
        try {
            Connection connection = target.getConnection();
            try {
                Statement statement = connection.createStatement();
                try {
                    String checkTable = qualified(group.schema, group.checkName);
                    String renamedTable = qualified(group.schema, group.checkRenamedName);
                    boolean checkCreated = false;
                    boolean renamed = false;
                    try {
                        if (tableExists(connection, group.schema, group.checkName)
                            || tableExists(connection, group.schema, group.checkRenamedName)) {
                            throw new IllegalStateException("full sync privilege-check table name already exists");
                        }
                        statement.execute("CREATE TABLE " + checkTable + " (`id` tinyint NOT NULL) ENGINE=InnoDB");
                        checkCreated = true;
                        statement.execute("INSERT INTO " + checkTable + " (`id`) VALUES (1)");
                        statement.execute("RENAME TABLE " + checkTable + " TO " + renamedTable);
                        checkCreated = false;
                        renamed = true;
                        statement.execute("DROP TABLE " + renamedTable);
                        renamed = false;
                    } finally {
                        try {
                            if (checkCreated) statement.execute("DROP TABLE IF EXISTS " + checkTable);
                            if (renamed) statement.execute("DROP TABLE IF EXISTS " + renamedTable);
                        } catch (SQLException cleanupError) {
                            logger.warn("WARN ## unable to clean full sync privilege-check table", cleanupError);
                        }
                    }
                } finally {
                    statement.close();
                }
            } finally {
                connection.close();
            }
        } finally {
            dataSourceCreator.destroyDataSource(target);
        }
    }

    private String physicalTableKey(DataMedia media, Map<Long, String> serverIdentities) throws Exception {
        Long sourceId = media.getSource().getId();
        String serverIdentity = serverIdentities.get(sourceId);
        if (serverIdentity == null) {
            DataSource source = dataSourceCreator.createDataSource(media.getSource());
            try {
                Connection connection = source.getConnection();
                try {
                    Statement statement = connection.createStatement();
                    try {
                        ResultSet result = statement.executeQuery("SELECT @@server_uuid");
                        try {
                            if (!result.next() || StringUtils.isBlank(result.getString(1))) {
                                throw new IllegalStateException("unable to identify MySQL server for data source " + sourceId);
                            }
                            serverIdentity = result.getString(1).toLowerCase(Locale.ENGLISH);
                        } finally {
                            result.close();
                        }
                    } finally {
                        statement.close();
                    }
                } finally {
                    connection.close();
                }
            } finally {
                dataSourceCreator.destroyDataSource(source);
            }
            serverIdentities.put(sourceId, serverIdentity);
        }
        return serverIdentity + ":" + media.getNamespaceMode().getSingleValue().toLowerCase(Locale.ENGLISH) + ":"
               + media.getNameMode().getSingleValue().toLowerCase(Locale.ENGLISH);
    }

    private Map<String, ColumnDefinition> readColumnDefinitions(DataSource source, DataMediaPair pair) throws SQLException {
        List<String> selected = readColumns(source, pair);
        Set<String> selectedNames = new HashSet<String>();
        for (String column : selected) selectedNames.add(column.toLowerCase(Locale.ENGLISH));
        return readColumnDefinitions(source, pair.getSource(), selectedNames, true);
    }

    private Map<String, ColumnDefinition> readAllColumnDefinitions(DataSource source, DataMedia media) throws SQLException {
        return readColumnDefinitions(source, media, null, false);
    }

    private Map<String, ColumnDefinition> readColumnDefinitions(DataSource source, DataMedia media,
                                                                 Set<String> selectedNames,
                                                                 boolean rejectGenerated) throws SQLException {
        Map<String, ColumnDefinition> result = new LinkedHashMap<String, ColumnDefinition>();
        Connection connection = source.getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet columns = statement.executeQuery("SHOW FULL COLUMNS FROM " + table(media));
                try {
                    while (columns.next()) {
                        String name = columns.getString("Field").toLowerCase(Locale.ENGLISH);
                        if (selectedNames != null && !selectedNames.contains(name)) continue;
                        String extra = StringUtils.defaultString(columns.getString("Extra"));
                        if (rejectGenerated && StringUtils.containsIgnoreCase(extra, "GENERATED")) {
                            throw new IllegalStateException("generated column " + name + " is not supported by full sync");
                        }
                        result.put(name, new ColumnDefinition(columns.getString("Type"), columns.getString("Collation"),
                            columns.getString("Null"), columns.getString("Key"), columns.getObject("Default"), extra));
                    }
                } finally {
                    columns.close();
                }
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
        if (selectedNames != null && result.size() != selectedNames.size()) {
            throw new IllegalStateException("unable to inspect all selected source columns");
        }
        return result;
    }

    private Map<String, LogPosition> resetPositions(List<Pipeline> pipelines) throws Exception {
        Map<String, LogPosition> positionsByDestination = new HashMap<String, LogPosition>();
        Map<String, LogPosition> positionsByClient = new HashMap<String, LogPosition>();
        ZkClientx zookeeper = ZooKeeperClient.getInstance();
        for (Pipeline pipeline : pipelines) {
            String destination = pipeline.getParameters().getDestinationName();
            short clientId = pipeline.getParameters().getMainstemClientId();
            LogPosition position = positionsByDestination.get(destination);
            if (position == null) {
                position = readCurrentPosition(destination, pipeline.getId());
                positionsByDestination.put(destination, position);
            }
            String batchPath = ZookeeperPathUtils.getBatchMarkPath(destination, clientId);
            if (zookeeper.exists(batchPath)) zookeeper.deleteRecursive(batchPath);
            zookeeper.createPersistent(batchPath, true);
            String cursorPath = ZookeeperPathUtils.getCursorPath(destination, clientId);
            byte[] data = JsonUtils.marshalToByte(position, SerializerFeature.WriteClassName);
            if (zookeeper.exists(cursorPath)) zookeeper.writeData(cursorPath, data);
            else zookeeper.createPersistent(cursorPath, data, true);
            positionsByClient.put(clientKey(destination, clientId), position);
        }
        verifyPositions(pipelines, positionsByClient);
        return positionsByClient;
    }

    private void verifyPositions(List<Pipeline> pipelines, Map<String, LogPosition> positions) {
        ZkClientx zookeeper = ZooKeeperClient.getInstance();
        for (Pipeline pipeline : pipelines) {
            String destination = pipeline.getParameters().getDestinationName();
            short clientId = pipeline.getParameters().getMainstemClientId();
            String batchPath = ZookeeperPathUtils.getBatchMarkPath(destination, clientId);
            if (!zookeeper.exists(batchPath) || zookeeper.countChildren(batchPath) != 0) {
                throw new IllegalStateException("Canal created a new unacknowledged batch while full sync was running");
            }
            String cursorPath = ZookeeperPathUtils.getCursorPath(destination, clientId);
            byte[] data = zookeeper.readData(cursorPath, true);
            LogPosition actual = data == null ? null : JsonUtils.unmarshalFromByte(data, LogPosition.class);
            if (!positions.get(clientKey(destination, clientId)).equals(actual)) {
                throw new IllegalStateException("Canal cursor changed while full sync was running");
            }
        }
    }

    private String clientKey(String destination, short clientId) {
        return destination + ":" + clientId;
    }

    private LogPosition readCurrentPosition(String destination, Long pipelineId) throws Exception {
        CanalParameter parameter = canalService.findByName(destination).getCanalParameter();
        InetSocketAddress address = parameter.getGroupDbAddresses().get(0).get(0).getDbAddress();
        Connection connection = DriverManager.getConnection("jdbc:mysql://" + address.getHostName() + ":" + address.getPort(),
            parameter.getDbUsername(), parameter.getDbPassword());
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet status = statement.executeQuery("SHOW MASTER STATUS");
                if (!status.next()) throw new IllegalStateException("SHOW MASTER STATUS returned no row for " + destination);
                EntryPosition entry = new EntryPosition(status.getString(1), status.getLong(2), System.currentTimeMillis());
                if (Boolean.TRUE.equals(parameter.getGtidEnable())) entry.setGtid(readGtid(statement));
                entry.setServerId(readServerId(statement));
                LogPosition position = new LogPosition();
                long slaveId = parameter.getSlaveId() == null ? 10000L : parameter.getSlaveId();
                position.setIdentity(new LogIdentity(address, slaveId + pipelineId));
                position.setPostion(entry);
                return position;
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    private Long readServerId(Statement statement) throws SQLException {
        ResultSet result = statement.executeQuery("SELECT @@server_id");
        try {
            return result.next() ? result.getLong(1) : null;
        } finally {
            result.close();
        }
    }

    private String readGtid(Statement statement) throws SQLException {
        ResultSet result = statement.executeQuery("SELECT @@GLOBAL.gtid_executed");
        try {
            return result.next() ? result.getString(1) : null;
        } finally {
            result.close();
        }
    }

    private void createStagingTables(List<TargetGroup> groups) throws Exception {
        try {
            for (TargetGroup group : groups) {
                DataSource target = dataSourceCreator.createDataSource(group.target.getSource());
                try {
                    Connection connection = target.getConnection();
                    try {
                        Statement statement = connection.createStatement();
                        try {
                            if (tableExists(connection, group.schema, group.stagingName)
                                || tableExists(connection, group.schema, group.backupName)) {
                                throw new IllegalStateException("full sync temporary table name already exists");
                            }
                            statement.execute("CREATE TABLE " + qualified(group.schema, group.stagingName) + " "
                                              + FullSyncSafety.ddlBody(group.templateDdl));
                            group.stagingCreated = true;
                        } finally {
                            statement.close();
                        }
                    } finally {
                        connection.close();
                    }
                } finally {
                    dataSourceCreator.destroyDataSource(target);
                }
            }
        } catch (Exception e) {
            cleanupStagingTables(groups);
            throw e;
        }
    }

    private List<String> readColumns(DataSource source, DataMediaPair pair) throws SQLException {
        Connection connection = source.getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet result = statement.executeQuery("SELECT * FROM " + table(pair.getSource()) + " WHERE 1=0");
                try {
                    return columns(result.getMetaData(), pair);
                } finally {
                    result.close();
                }
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    private String showCreateTable(DataSource source, String table) throws SQLException {
        Connection connection = source.getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet result = statement.executeQuery("SHOW CREATE TABLE " + table);
                try {
                    if (!result.next()) throw new SQLException("SHOW CREATE TABLE returned no row");
                    return result.getString(2);
                } finally {
                    result.close();
                }
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    private long countRows(List<TargetGroup> groups) throws Exception {
        long total = 0;
        for (TargetGroup group : groups) {
            for (DataMediaPair pair : group.pairs) {
                DataSource source = dataSourceCreator.createDataSource(pair.getSource().getSource());
                try {
                    Connection connection = source.getConnection();
                    try {
                        Statement statement = connection.createStatement();
                        try {
                            ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM " + table(pair.getSource()));
                            try {
                                result.next();
                                total += result.getLong(1);
                            } finally {
                                result.close();
                            }
                        } finally {
                            statement.close();
                        }
                    } finally {
                        connection.close();
                    }
                } finally {
                    dataSourceCreator.destroyDataSource(source);
                }
            }
        }
        return total;
    }

    private void copyPair(FullSyncTaskDO task, DataMediaPair pair, TargetGroup group) throws Exception {
        DataSource source = dataSourceCreator.createDataSource(pair.getSource().getSource());
        DataSource target = dataSourceCreator.createDataSource(group.target.getSource());
        try {
            Connection read = source.getConnection();
            Connection write = target.getConnection();
            try {
                Statement statement = read.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                statement.setFetchSize(Integer.MIN_VALUE);
                ResultSet result = statement.executeQuery("SELECT * FROM " + table(pair.getSource()));
                try {
                    ResultSetMetaData metadata = result.getMetaData();
                    List<String> columns = columns(metadata, pair);
                    if (columns.isEmpty()) throw new IllegalStateException("no columns selected for pair " + pair.getId());
                    PreparedStatement insert = write.prepareStatement(insertSql(qualified(group.schema, group.stagingName), columns));
                    try {
                        int pending = 0;
                        while (result.next()) {
                            for (int i = 0; i < columns.size(); i++) insert.setObject(i + 1, result.getObject(columns.get(i)));
                            insert.addBatch();
                            pending++;
                            if (pending == BATCH_SIZE) {
                                insert.executeBatch();
                                increase(task, pending);
                                pending = 0;
                            }
                        }
                        if (pending > 0) {
                            insert.executeBatch();
                            increase(task, pending);
                        }
                    } finally {
                        insert.close();
                    }
                } finally {
                    result.close();
                    statement.close();
                }
            } finally {
                write.close();
                read.close();
            }
        } finally {
            dataSourceCreator.destroyDataSource(source);
            dataSourceCreator.destroyDataSource(target);
        }
    }

    private void switchTargets(Long channelId, List<TargetGroup> groups) throws Exception {
        try {
            for (TargetGroup group : groups) {
                requireStopped(channelId);
                DataSource target = dataSourceCreator.createDataSource(group.target.getSource());
                try {
                    Connection connection = target.getConnection();
                    try {
                        group.hadOriginal = tableExists(connection, group.schema, group.targetName);
                        Statement statement = connection.createStatement();
                        try {
                            if (tableExists(connection, group.schema, group.backupName)) {
                                throw new IllegalStateException("full sync backup table name already exists");
                            }
                            if (group.hadOriginal) {
                                statement.execute("RENAME TABLE " + qualified(group.schema, group.targetName) + " TO "
                                                  + qualified(group.schema, group.backupName) + ", "
                                                  + qualified(group.schema, group.stagingName) + " TO "
                                                  + qualified(group.schema, group.targetName));
                            } else {
                                statement.execute("RENAME TABLE " + qualified(group.schema, group.stagingName) + " TO "
                                                  + qualified(group.schema, group.targetName));
                            }
                            group.stagingCreated = false;
                            group.backupCreated = group.hadOriginal;
                            group.switched = true;
                        } finally {
                            statement.close();
                        }
                    } finally {
                        connection.close();
                    }
                } finally {
                    dataSourceCreator.destroyDataSource(target);
                }
            }
        } catch (Exception e) {
            rollbackTargetSwitches(groups);
            throw e;
        }
    }

    private boolean tableExists(Connection connection, String schema, String name) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=? AND table_name=?");
        try {
            statement.setString(1, schema);
            statement.setString(2, name);
            ResultSet result = statement.executeQuery();
            try {
                return result.next();
            } finally {
                result.close();
            }
        } finally {
            statement.close();
        }
    }

    private void rollbackTargetSwitches(List<TargetGroup> groups) {
        List<TargetGroup> reversed = new ArrayList<TargetGroup>(groups);
        Collections.reverse(reversed);
        for (TargetGroup group : reversed) {
            if (!group.switched) continue;
            DataSource target = null;
            try {
                target = dataSourceCreator.createDataSource(group.target.getSource());
                Connection connection = target.getConnection();
                try {
                    Statement statement = connection.createStatement();
                    try {
                        if (group.hadOriginal) {
                            statement.execute("RENAME TABLE " + qualified(group.schema, group.targetName) + " TO "
                                              + qualified(group.schema, group.stagingName) + ", "
                                              + qualified(group.schema, group.backupName) + " TO "
                                              + qualified(group.schema, group.targetName));
                            group.backupCreated = false;
                        } else {
                            statement.execute("RENAME TABLE " + qualified(group.schema, group.targetName) + " TO "
                                              + qualified(group.schema, group.stagingName));
                        }
                        group.stagingCreated = true;
                        statement.execute("DROP TABLE IF EXISTS " + qualified(group.schema, group.stagingName));
                        group.stagingCreated = false;
                        group.switched = false;
                    } finally {
                        statement.close();
                    }
                } finally {
                    connection.close();
                }
            } catch (Exception rollbackError) {
                logger.error("ERROR ## failed to restore target table " + group.targetName, rollbackError);
            } finally {
                if (target != null) dataSourceCreator.destroyDataSource(target);
            }
        }
    }

    private void cleanupStagingTables(List<TargetGroup> groups) {
        cleanupTables(groups, false, false);
    }

    private void cleanupBackups(List<TargetGroup> groups) {
        cleanupTables(groups, true, false);
    }

    private void cleanupBackups(FullSyncTaskDO task) {
        try {
            List<Pipeline> pipelines = pipelineService.listByChannelIds(task.getChannelId());
            List<TargetGroup> groups = buildGroups(loadPairs(pipelines), task.getId());
            cleanupTables(groups, true, true);
        } catch (Exception e) {
            logger.warn("WARN ## unable to clean full sync backup tables for task " + task.getId(), e);
        }
    }

    private void cleanupTables(List<TargetGroup> groups, boolean backup, boolean force) {
        for (TargetGroup group : groups) {
            if (!force && !(backup ? group.backupCreated : group.stagingCreated)) continue;
            DataSource target = null;
            try {
                target = dataSourceCreator.createDataSource(group.target.getSource());
                Connection connection = target.getConnection();
                try {
                    Statement statement = connection.createStatement();
                    try {
                        statement.execute("DROP TABLE IF EXISTS "
                                          + qualified(group.schema, backup ? group.backupName : group.stagingName));
                        if (backup) group.backupCreated = false;
                        else group.stagingCreated = false;
                    } finally {
                        statement.close();
                    }
                } finally {
                    connection.close();
                }
            } catch (Exception cleanupError) {
                logger.warn("WARN ## unable to clean full sync temporary table", cleanupError);
            } finally {
                if (target != null) dataSourceCreator.destroyDataSource(target);
            }
        }
    }

    private List<String> columns(ResultSetMetaData metadata, DataMediaPair pair) throws SQLException {
        List<String> all = new ArrayList<String>();
        for (int i = 1; i <= metadata.getColumnCount(); i++) all.add(metadata.getColumnLabel(i));
        if (pair.getColumnPairs().isEmpty()) return all;
        Set<String> configured = new HashSet<String>();
        for (ColumnPair columnPair : pair.getColumnPairs()) configured.add(columnPair.getSourceColumn().getName());
        List<String> selected = new ArrayList<String>();
        for (String column : all) {
            if (pair.getColumnPairMode().isInclude() ? configured.contains(column) : !configured.contains(column)) {
                selected.add(column);
            }
        }
        return selected;
    }

    private String insertSql(String targetTable, List<String> columns) {
        StringBuilder names = new StringBuilder();
        StringBuilder values = new StringBuilder();
        StringBuilder updates = new StringBuilder();
        for (String column : columns) {
            if (names.length() > 0) {
                names.append(',');
                values.append(',');
                updates.append(',');
            }
            String quoted = quote(column);
            names.append(quoted);
            values.append('?');
            updates.append(quoted).append("=VALUES(").append(quoted).append(')');
        }
        return "INSERT INTO " + targetTable + " (" + names + ") VALUES (" + values + ") ON DUPLICATE KEY UPDATE "
               + updates;
    }

    private void increase(FullSyncTaskDO task, int rows) {
        task.setCopiedRows(task.getCopiedRows() + rows);
        update(task, RUNNING, "COPY_DATA", "正在导入全量数据到临时表");
    }

    private void requireStopped(Long channelId) {
        ChannelStatus status = arbitrateManageService.channelEvent().status(channelId);
        if (!ChannelStatus.STOP.equals(status)) {
            throw new IllegalStateException("channel must be stopped before and during full sync");
        }
    }

    private FullSyncTaskDO requireTask(Long id) {
        FullSyncTaskDO task = fullSyncTaskDao.findById(id);
        if (task == null) throw new IllegalArgumentException("full sync task does not exist");
        return task;
    }

    private void requireAvailable() {
        if (!available) {
            throw new IllegalStateException("full sync is unavailable; apply the Manager database upgrade SQL and restart Manager");
        }
    }

    private void update(FullSyncTaskDO task, String status, String stage, String detail) {
        task.setStatus(status);
        task.setStage(stage);
        task.setMessage(StringUtils.abbreviate(detail, 3900));
        fullSyncTaskDao.update(task);
    }

    private void requireDirectMysql(DataMediaSource source) {
        if (source == null || source.getType() == null || !source.getType().isMysql() || !(source instanceof DbMediaSource)) {
            throw new IllegalStateException("full sync supports direct MySQL data media only");
        }
        String url = ((DbMediaSource) source).getUrl();
        if (!StringUtils.startsWithIgnoreCase(url, "jdbc:mysql://") || StringUtils.containsIgnoreCase(url, "groupKey=")) {
            throw new IllegalStateException("full sync does not support grouped or custom MySQL data sources");
        }
    }

    private String table(DataMedia media) {
        return qualified(media.getNamespaceMode().getSingleValue(), media.getNameMode().getSingleValue());
    }

    private String qualified(String schema, String name) {
        return quote(schema) + "." + quote(name);
    }

    private String quote(String value) {
        return "`" + value.replace("`", "``") + "`";
    }

    private String message(Exception e) {
        return e.getClass().getSimpleName() + ": " + StringUtils.defaultString(e.getMessage(), "no detail");
    }

    public void afterPropertiesSet() {
        executor = Executors.newSingleThreadExecutor();
        try {
            fullSyncTaskDao.failRunningTasks("Manager restarted before task completion");
            available = true;
        } catch (RuntimeException e) {
            available = false;
            logger.error("ERROR ## full sync is disabled because FULL_SYNC_TASK is unavailable; apply the upgrade SQL", e);
        }
    }

    public void destroy() {
        if (executor != null) executor.shutdownNow();
    }

    public void setFullSyncTaskDao(FullSyncTaskDAO dao) {
        this.fullSyncTaskDao = dao;
    }

    public void setChannelDao(ChannelDAO value) {
        this.channelDao = value;
    }

    public void setChannelService(ChannelService value) {
        this.channelService = value;
    }

    public void setPipelineService(PipelineService value) {
        this.pipelineService = value;
    }

    public void setDataMediaPairService(DataMediaPairService value) {
        this.dataMediaPairService = value;
    }

    public void setCanalService(CanalService value) {
        this.canalService = value;
    }

    public void setArbitrateManageService(ArbitrateManageService value) {
        this.arbitrateManageService = value;
    }

    public void setDataSourceCreator(DataSourceCreator value) {
        this.dataSourceCreator = value;
    }

    public void setTransactionTemplate(TransactionTemplate value) {
        this.transactionTemplate = value;
    }

    private static class TargetGroup {
        private final DataMedia target;
        private final List<DataMediaPair> pairs = new ArrayList<DataMediaPair>();
        private String schema;
        private String targetName;
        private String stagingName;
        private String backupName;
        private String checkName;
        private String checkRenamedName;
        private String physicalKey;
        private String templateDdl;
        private Map<String, ColumnDefinition> templateColumns;
        private boolean hadOriginal;
        private boolean switched;
        private boolean stagingCreated;
        private boolean backupCreated;

        TargetGroup(DataMedia target) {
            this.target = target;
        }

        void prepareNames(Long taskId, int index) {
            schema = target.getNamespaceMode().getSingleValue();
            targetName = target.getNameMode().getSingleValue();
            stagingName = FullSyncSafety.temporaryTableName("__otter_fs_", taskId, index);
            backupName = FullSyncSafety.temporaryTableName("__otter_bak_", taskId, index);
            checkName = FullSyncSafety.temporaryTableName("__otter_check_", taskId, index);
            checkRenamedName = FullSyncSafety.temporaryTableName("__otter_check2_", taskId, index);
        }
    }

    private static class ColumnDefinition {
        private final String type;
        private final String collation;
        private final String nullable;
        private final String key;
        private final String defaultValue;
        private final String extra;

        ColumnDefinition(String type, String collation, String nullable, String key, Object defaultValue, String extra) {
            this.type = StringUtils.lowerCase(StringUtils.defaultString(type), Locale.ENGLISH);
            this.collation = StringUtils.lowerCase(StringUtils.defaultString(collation), Locale.ENGLISH);
            this.nullable = StringUtils.upperCase(StringUtils.defaultString(nullable), Locale.ENGLISH);
            this.key = StringUtils.upperCase(StringUtils.defaultString(key), Locale.ENGLISH);
            this.defaultValue = defaultValue == null ? "<NULL>" : defaultValue.getClass().getName() + ":" + defaultValue;
            this.extra = StringUtils.lowerCase(StringUtils.defaultString(extra), Locale.ENGLISH);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (!(object instanceof ColumnDefinition)) return false;
            ColumnDefinition other = (ColumnDefinition) object;
            return type.equals(other.type) && collation.equals(other.collation) && nullable.equals(other.nullable)
                   && key.equals(other.key) && defaultValue.equals(other.defaultValue) && extra.equals(other.extra);
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + collation.hashCode();
            result = 31 * result + nullable.hashCode();
            result = 31 * result + key.hashCode();
            result = 31 * result + defaultValue.hashCode();
            return 31 * result + extra.hashCode();
        }
    }
}
