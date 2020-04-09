/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;

import lombok.Cleanup;

import java.net.InetAddress;
import java.net.URI;

public class StartWriter {

    public static void main(String[] args) throws Exception {
        @Cleanup
        StreamManager streamManager = StreamManager.create(URI.create("tcp://localhost:9090"));
        streamManager.createScope(StartLocalService.SCOPE);
        streamManager.createStream(StartLocalService.SCOPE, StartLocalService.STREAM_NAME + "16",
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(16)).build());
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(StartLocalService.SCOPE, ClientConfig.builder().build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(StartLocalService.STREAM_NAME + "16", new JavaSerializer<>(),
                                                                           EventWriterConfig.builder()
                                                                                            .transactionTimeoutTime(60000)
                                                                                            .build());
        String event = "\n Non-transactional Publish \n";
        long iniTime = System.currentTimeMillis();
        double events = 1000000.0;
        for (int i = 0; i < events; i++) {
            //System.err.println("Writing event: " + i);
            writer.writeEvent(event);
            //Thread.sleep(500);
        }
        System.err.println("TIME: " +  ((System.currentTimeMillis() - iniTime) / 1000.0) +
                " THROUGHPUT: " + (events / ((System.currentTimeMillis() - iniTime)) * 1000.0));
        //transaction.commit();
        System.exit(0);
    }
}
