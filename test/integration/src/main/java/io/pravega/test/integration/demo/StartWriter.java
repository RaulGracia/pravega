/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
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
        streamManager.createStream(StartLocalService.SCOPE, StartLocalService.STREAM_NAME, StreamConfiguration.builder().build());
        ClientFactory clientFactory = ClientFactory.withScope(StartLocalService.SCOPE,
                ClientConfig.builder().controllerURI(URI.create("tcp://localhost:9090")).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(StartLocalService.STREAM_NAME, new JavaSerializer<>(),
                                                                           EventWriterConfig.builder()
                                                                                            .transactionTimeoutTime(60000)
                                                                                            .build());


        for (int i = 0; i < 10; i++) {
            String event = "\n Non-transactional Publish \n";
            System.err.println("Writing event: " + event);
            writer.writeEvent(event);
            writer.flush();
            Thread.sleep(500);
        }
        System.exit(0);
    }
}
