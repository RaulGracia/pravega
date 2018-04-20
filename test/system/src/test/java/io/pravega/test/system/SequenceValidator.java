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

import java.util.HashMap;
import java.util.Map;

public class SequenceValidator {
    //sourced from platform team...
    private Map<String, Long> sequenceCounts = new HashMap<>();

    public static String createSenderId(String key, String writerId) {
        return String.format("%s---%s", key, writerId);
    }

    public synchronized void validate(String senderId, Long sequenceNumber) {
        Long lastSequenceNumber = sequenceCounts.getOrDefault(senderId, 0L);

        if (sequenceNumber <= lastSequenceNumber) {
            throw new IllegalStateException(String.format("Event out of sequence %s: lastSeq: %d, currentSeq:%d", senderId, lastSequenceNumber, sequenceNumber));
        } else {
            sequenceCounts.put(senderId, sequenceNumber);
        }
    }
}
