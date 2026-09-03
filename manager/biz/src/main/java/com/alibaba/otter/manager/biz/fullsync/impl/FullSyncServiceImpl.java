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
import com.alibaba.otter.manager.biz.config.datamediapair.DataMediaPairService;
import com.alibaba.otter.manager.biz.config.pipeline.PipelineService;
import com.alibaba.otter.manager.biz.fullsync.FullSyncService;
import com.alibaba.otter.manager.biz.fullsync.dal.FullSyncTaskDAO;
import com.alibaba.otter.manager.biz.fullsync.dal.dataobject.FullSyncTaskDO;
import com.alibaba.otter.shared.arbitrate.ArbitrateManageService;
import com.alibaba.otter.shared.arbitrate.impl.zookeeper.ZooKeeperClient;
import com.alibaba.otter.shared.common.model.config.channel.Channel;
import com.alibaba.otter.shared.common.model.config.channel.ChannelStatus;
import com.alibaba.otter.shared.common.model.config.data.ColumnPair;
import com.alibaba.otter.shared.common.model.config.data.DataMedia;
import com.alibaba.otter.shared.common.model.config.data.DataMediaPair;
import com.alibaba.otter.shared.common.model.config.data.DataMediaSource;
import com.alibaba.otter.shared.common.model.config.pipeline.Pipeline;
import com.alibaba.otter.shared.common.utils.zookeeper.ZkClientx;

public class FullSyncServiceImpl implements FullSyncService, InitializingBean, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(FullSyncServiceImpl.class);
    private static final int BATCH_SIZE = 500;
    private static final String RUNNING = "RUNNING";
    private static final String SUCCESS = "SUCCESS";
    private static final String FAILED = "FAILED";

    private FullSyncTaskDAO fullSyncTaskDao;
    private ChannelService channelService;
    private PipelineService pipelineService;
    private DataMediaPairService dataMediaPairService;
    private CanalService canalService;
    private ArbitrateManageService arbitrateManageService;
    private DataSourceCreator dataSourceCreator;
    private ExecutorService executor;

    public synchronized FullSyncTaskDO create(Long channelId) {
        requireStopped(channelId);
        if (fullSyncTaskDao.findActiveByChannelId(channelId) != null) {
            throw new IllegalStateException("channel already has a running full sync task");
        }
        FullSyncTaskDO task = new FullSyncTaskDO();
        task.setChannelId(channelId);
        task.setStatus(RUNNING);
        task.setStage("PRECHECK");
        task.setTotalRows(0L);
        task.setCopiedRows(0L);
        task.setMessage("任务已创建，等待执行");
        fullSyncTaskDao.insert(task);
        executor.submit(new Runnable() {
            public void run() { execute(task.getId()); }
        });
        return task;
    }

    public FullSyncTaskDO findById(Long taskId) {
        return fullSyncTaskDao.findById(taskId);
    }

    public synchronized void retryStart(Long taskId) {
        FullSyncTaskDO task = requireTask(taskId);
        if (!FAILED.equals(task.getStatus()) || !"START_CHANNEL".equals(task.getStage())) {
            throw new IllegalStateException("only a task that failed while starting the channel can be retried");
        }
        requireStopped(task.getChannelId());
        try {
            channelService.startChannel(task.getChannelId());
            update(task, SUCCESS, "DONE", "全量数据已导入，Channel 已启动");
        } catch (Exception e) {
            update(task, FAILED, "START_CHANNEL", message(e));
            throw new IllegalStateException("start channel failed", e);
        }
    }

    private void execute(Long taskId) {
        FullSyncTaskDO task = requireTask(taskId);
        try {
            requireStopped(task.getChannelId());
            Channel channel = channelService.findById(task.getChannelId());
            List<Pipeline> pipelines = pipelineService.listByChannelIds(task.getChannelId());
            if (pipelines.isEmpty()) throw new IllegalStateException("channel has no pipeline");
            List<DataMediaPair> pairs = loadPairs(pipelines);
            List<TargetGroup> groups = buildGroups(pairs);
            validateSharedDestinations(task.getChannelId(), pipelines);
            validateGroups(groups);
            preflightGroups(groups);

            update(task, RUNNING, "RESET_POSITION", "正在写入当前 Canal 位点");
            resetPositions(pipelines);
            update(task, RUNNING, "RECREATE_TABLE", "正在重建目标表");
            for (TargetGroup group : groups) recreateTarget(group);

            long total = countRows(groups);
            task.setTotalRows(total);
            task.setCopiedRows(0L);
            update(task, RUNNING, "COPY_DATA", "正在导入全量数据");
            for (TargetGroup group : groups) {
                for (DataMediaPair pair : group.pairs) copyPair(task, pair, group);
            }

            update(task, RUNNING, "START_CHANNEL", "数据导入完成，正在启动 Channel");
            channelService.startChannel(task.getChannelId());
            update(task, SUCCESS, "DONE", "全量同步完成，Channel 已启动");
        } catch (Exception e) {
            logger.error("ERROR ## full sync task " + taskId + " failed", e);
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

    private List<TargetGroup> buildGroups(List<DataMediaPair> pairs) {
        Map<String, TargetGroup> result = new LinkedHashMap<String, TargetGroup>();
        Collections.sort(pairs, new Comparator<DataMediaPair>() {
            public int compare(DataMediaPair left, DataMediaPair right) { return left.getId().compareTo(right.getId()); }
        });
        for (DataMediaPair pair : pairs) {
            DataMedia target = pair.getTarget();
            String key = target.getSource().getId() + ":" + target.getNamespaceMode().getSingleValue() + ":" + target.getNameMode().getSingleValue();
            TargetGroup group = result.get(key);
            if (group == null) {
                group = new TargetGroup(target);
                result.put(key, group);
            }
            group.pairs.add(pair);
        }
        return new ArrayList<TargetGroup>(result.values());
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

    private void validateGroups(List<TargetGroup> groups) {
        for (TargetGroup group : groups) {
            requireMysql(group.target.getSource());
            for (DataMediaPair pair : group.pairs) {
                requireMysql(pair.getSource().getSource());
                if (!pair.getSource().getNamespaceMode().getMode().isSingle() || !pair.getSource().getNameMode().getMode().isSingle()
                    || !pair.getTarget().getNamespaceMode().getMode().isSingle() || !pair.getTarget().getNameMode().getMode().isSingle()) {
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
        for (TargetGroup group : groups) {
            DataMediaPair template = group.pairs.get(0);
            DataSource source = dataSourceCreator.createDataSource(template.getSource().getSource());
            try {
                group.templateDdl = showCreateTable(source, table(template.getSource()));
                group.templateColumns = new HashSet<String>(readColumns(source, template));
            } finally {
                dataSourceCreator.destroyDataSource(source);
            }
            for (DataMediaPair pair : group.pairs) {
                DataSource pairSource = dataSourceCreator.createDataSource(pair.getSource().getSource());
                try {
                    for (String column : readColumns(pairSource, pair)) {
                        if (!group.templateColumns.contains(column)) {
                            throw new IllegalStateException("source column " + column + " is absent from target table template");
                        }
                    }
                } finally {
                    dataSourceCreator.destroyDataSource(pairSource);
                }
            }
        }
    }

    private void resetPositions(List<Pipeline> pipelines) throws Exception {
        Map<String, LogPosition> positions = new HashMap<String, LogPosition>();
        for (Pipeline pipeline : pipelines) {
            String destination = pipeline.getParameters().getDestinationName();
            LogPosition position = positions.get(destination);
            if (position == null) {
                position = readCurrentPosition(destination);
                positions.put(destination, position);
            }
            String path = ZookeeperPathUtils.getCursorPath(destination, pipeline.getId().shortValue());
            ZkClientx zookeeper = ZooKeeperClient.getInstance();
            if (zookeeper.exists(path)) zookeeper.writeData(path, JsonUtils.marshalToByte(position, SerializerFeature.WriteClassName));
            else zookeeper.createPersistent(path, JsonUtils.marshalToByte(position, SerializerFeature.WriteClassName), true);
        }
    }

    private LogPosition readCurrentPosition(String destination) throws Exception {
        Canal canal = canalService.findByName(destination);
        if (canal == null || canal.getCanalParameter() == null || canal.getCanalParameter().getDbAddresses().isEmpty()) {
            throw new IllegalStateException("Canal configuration is missing for destination " + destination);
        }
        CanalParameter parameter = canal.getCanalParameter();
        InetSocketAddress address = parameter.getDbAddresses().get(0);
        Connection connection = DriverManager.getConnection("jdbc:mysql://" + address.getHostName() + ":" + address.getPort(),
            parameter.getDbUsername(), parameter.getDbPassword());
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet status = statement.executeQuery("SHOW MASTER STATUS");
                if (!status.next()) throw new IllegalStateException("SHOW MASTER STATUS returned no row for " + destination);
                EntryPosition entry = new EntryPosition(status.getString(1), status.getLong(2));
                if (Boolean.TRUE.equals(parameter.getGtidEnable())) entry.setGtid(readGtid(statement));
                entry.setServerId(readServerId(statement));
                LogPosition position = new LogPosition();
                position.setIdentity(new LogIdentity(address, parameter.getSlaveId()));
                position.setPostion(entry);
                return position;
            } finally { statement.close(); }
        } finally { connection.close(); }
    }

    private Long readServerId(Statement statement) throws SQLException {
        ResultSet result = statement.executeQuery("SELECT @@server_id");
        try { return result.next() ? result.getLong(1) : null; } finally { result.close(); }
    }

    private String readGtid(Statement statement) throws SQLException {
        ResultSet result = statement.executeQuery("SELECT @@GLOBAL.gtid_executed");
        try { return result.next() ? result.getString(1) : null; } finally { result.close(); }
    }

    private void recreateTarget(TargetGroup group) throws Exception {
        DataSource target = dataSourceCreator.createDataSource(group.target.getSource());
        try {
            int body = group.templateDdl.indexOf('(');
            if (body < 0) throw new IllegalStateException("unexpected SHOW CREATE TABLE result");
            String targetTable = table(group.target);
            Connection connection = target.getConnection();
            try {
                Statement statement = connection.createStatement();
                try {
                    statement.execute("DROP TABLE IF EXISTS " + targetTable);
                    statement.execute("CREATE TABLE " + targetTable + " " + group.templateDdl.substring(body));
                } finally { statement.close(); }
            } finally { connection.close(); }
        } finally {
            dataSourceCreator.destroyDataSource(target);
        }
    }

    private List<String> readColumns(DataSource source, DataMediaPair pair) throws SQLException {
        Connection connection = source.getConnection();
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet result = statement.executeQuery("SELECT * FROM " + table(pair.getSource()) + " WHERE 1=0");
                try { return columns(result.getMetaData(), pair); } finally { result.close(); }
            } finally { statement.close(); }
        } finally { connection.close(); }
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
                } finally { result.close(); }
            } finally { statement.close(); }
        } finally { connection.close(); }
    }

    private long countRows(List<TargetGroup> groups) throws Exception {
        long total = 0;
        for (TargetGroup group : groups) for (DataMediaPair pair : group.pairs) {
            DataSource source = dataSourceCreator.createDataSource(pair.getSource().getSource());
            try {
                Connection connection = source.getConnection();
                try {
                    Statement statement = connection.createStatement();
                    try {
                        ResultSet result = statement.executeQuery("SELECT COUNT(*) FROM " + table(pair.getSource()));
                        try { result.next(); total += result.getLong(1); } finally { result.close(); }
                    } finally { statement.close(); }
                } finally { connection.close(); }
            } finally { dataSourceCreator.destroyDataSource(source); }
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
                    PreparedStatement insert = write.prepareStatement(insertSql(table(group.target), columns));
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
                        if (pending > 0) { insert.executeBatch(); increase(task, pending); }
                    } finally { insert.close(); }
                } finally { result.close(); statement.close(); }
            } finally { write.close(); read.close(); }
        } finally {
            dataSourceCreator.destroyDataSource(source);
            dataSourceCreator.destroyDataSource(target);
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
            if (pair.getColumnPairMode().isInclude() ? configured.contains(column) : !configured.contains(column)) selected.add(column);
        }
        return selected;
    }

    private String insertSql(String targetTable, List<String> columns) {
        StringBuilder names = new StringBuilder();
        StringBuilder values = new StringBuilder();
        StringBuilder updates = new StringBuilder();
        for (String column : columns) {
            if (names.length() > 0) { names.append(','); values.append(','); updates.append(','); }
            String quoted = quote(column);
            names.append(quoted); values.append('?'); updates.append(quoted).append("=VALUES(").append(quoted).append(')');
        }
        return "INSERT INTO " + targetTable + " (" + names + ") VALUES (" + values + ") ON DUPLICATE KEY UPDATE " + updates;
    }

    private void increase(FullSyncTaskDO task, int rows) {
        task.setCopiedRows(task.getCopiedRows() + rows);
        update(task, RUNNING, "COPY_DATA", "正在导入全量数据");
    }

    private void requireStopped(Long channelId) {
        ChannelStatus status = arbitrateManageService.channelEvent().status(channelId);
        if (!ChannelStatus.STOP.equals(status)) throw new IllegalStateException("channel must be stopped before full sync");
    }

    private FullSyncTaskDO requireTask(Long id) {
        FullSyncTaskDO task = fullSyncTaskDao.findById(id);
        if (task == null) throw new IllegalArgumentException("full sync task does not exist");
        return task;
    }

    private void update(FullSyncTaskDO task, String status, String stage, String message) {
        task.setStatus(status); task.setStage(stage); task.setMessage(StringUtils.abbreviate(message, 3900));
        fullSyncTaskDao.update(task);
    }

    private void requireMysql(DataMediaSource source) {
        if (source == null || source.getType() == null || !source.getType().isMysql()) throw new IllegalStateException("full sync supports MySQL data media only");
    }

    private String table(DataMedia media) { return quote(media.getNamespaceMode().getSingleValue()) + "." + quote(media.getNameMode().getSingleValue()); }
    private String quote(String value) { return "`" + value.replace("`", "``") + "`"; }
    private String message(Exception e) { return e.getClass().getSimpleName() + ": " + StringUtils.defaultString(e.getMessage(), "no detail"); }

    public void afterPropertiesSet() { fullSyncTaskDao.failRunningTasks("Manager restarted before task completion"); executor = Executors.newSingleThreadExecutor(); }
    public void destroy() { if (executor != null) executor.shutdownNow(); }

    public void setFullSyncTaskDao(FullSyncTaskDAO dao) { this.fullSyncTaskDao = dao; }
    public void setChannelService(ChannelService value) { this.channelService = value; }
    public void setPipelineService(PipelineService value) { this.pipelineService = value; }
    public void setDataMediaPairService(DataMediaPairService value) { this.dataMediaPairService = value; }
    public void setCanalService(CanalService value) { this.canalService = value; }
    public void setArbitrateManageService(ArbitrateManageService value) { this.arbitrateManageService = value; }
    public void setDataSourceCreator(DataSourceCreator value) { this.dataSourceCreator = value; }

    private static class TargetGroup {
        private final DataMedia target;
        private final List<DataMediaPair> pairs = new ArrayList<DataMediaPair>();
        private String templateDdl;
        private Set<String> templateColumns;
        TargetGroup(DataMedia target) { this.target = target; }
    }
}
