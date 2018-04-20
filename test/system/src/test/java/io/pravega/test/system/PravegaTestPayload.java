/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import lombok.Data;

import java.io.Serializable;

@Data
public class PravegaTestPayload implements Serializable {
    //Copied from Platform repo with minor changes.
    private long timestamp;
    private String payload;
    private String senderId;
    private long sequenceNumber;

    public PravegaTestPayload() {
    }

    public PravegaTestPayload(long eventTime, String senderId, long sequenceNumber, String payload) {
        this.timestamp = eventTime;
        this.payload = payload;
        this.senderId = senderId;
        this.sequenceNumber = sequenceNumber;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getSenderId() {
        return senderId;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }
}
