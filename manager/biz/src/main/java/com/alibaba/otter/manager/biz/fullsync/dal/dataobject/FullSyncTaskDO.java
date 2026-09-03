/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.alibaba.otter.manager.biz.fullsync.dal.dataobject;

import java.io.Serializable;
import java.util.Date;

public class FullSyncTaskDO implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long id;
    private Long channelId;
    private String status;
    private String stage;
    private Long totalRows;
    private Long copiedRows;
    private String message;
    private Date gmtCreate;
    private Date gmtModified;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getChannelId() { return channelId; }
    public void setChannelId(Long channelId) { this.channelId = channelId; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getStage() { return stage; }
    public void setStage(String stage) { this.stage = stage; }
    public Long getTotalRows() { return totalRows; }
    public void setTotalRows(Long totalRows) { this.totalRows = totalRows; }
    public Long getCopiedRows() { return copiedRows; }
    public void setCopiedRows(Long copiedRows) { this.copiedRows = copiedRows; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public Date getGmtCreate() { return gmtCreate; }
    public void setGmtCreate(Date gmtCreate) { this.gmtCreate = gmtCreate; }
    public Date getGmtModified() { return gmtModified; }
    public void setGmtModified(Date gmtModified) { this.gmtModified = gmtModified; }
}
