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

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class EventOrderTest {

    private static final long READ_TIMEOUT = SECONDS.toMillis(30);
    private static final int RANDOM_SUFFIX = RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String SCOPE = "scope" + RANDOM_SUFFIX;
    private static final String STREAM = "stream123";
    private static final String READER_GROUP_NAME = "RG123" + RANDOM_SUFFIX;
    private static final int NUMBER_OF_READERS = 3; //this matches the number of segments in the stream
    private static final ScalingPolicy SCALING_POLICY = ScalingPolicy.byEventRate(1, 2, 1);
    //    @Rule no timeout.
    //    public Timeout globalTimeout = Timeout.seconds(7 * 60);

    private final ReaderConfig readerConfig = ReaderConfig.builder().build();
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "checkPointExecutor");
    private final StreamConfiguration streamConfig = StreamConfiguration.builder().scalingPolicy(SCALING_POLICY).build();
    private final ScheduledExecutorService readerExecutor = ExecutorServiceHelpers.newScheduledThreadPool(NUMBER_OF_READERS,
            "readerCheckpointTest-reader");

    private final ScheduledExecutorService writerExecutor = ExecutorServiceHelpers.newScheduledThreadPool(4, "WriterPool");
    private final ScheduledExecutorService verifyExecutor = ExecutorServiceHelpers.newScheduledThreadPool(2, "VerifyPool");

    private URI controllerURI;

    @Environment
    public static void initialize() {

        //1. check if zk is running, if not start it
        Service zkService = Utils.createZookeeperService();
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);

        //get the zk ip details and pass it to bk, host, controller
        //2, check if bk is running, otherwise start, get the zk ip
        Service bkService = Utils.createBookkeeperService(zkUris.get(0));
        if (!bkService.isRunning()) {
            bkService.start(true);
        }
        log.debug("Bookkeeper service details: {}", bkService.getServiceDetails());

        //3. start controller
        Service conService = Utils.createPravegaControllerService(zkUris.get(0));
        if (!conService.isRunning()) {
            conService.start(true);
        }
        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega Controller service details: {}", conUris);

        //4.start host
        Service segService = Utils.createPravegaSegmentStoreService(zkUris.get(0), conUris.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }
        log.debug("Pravega segment store details: {}", segService.getServiceDetails());
    }

    @Before
    public void setup() {
        controllerURI = fetchControllerURI();
        StreamManager streamManager = StreamManager.create(controllerURI);
        assertTrue("Creating Scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, streamConfig));
    }

    @Test
    public void eventOrderTests() throws Exception {

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, controllerURI);
        readerGroupManager.createReaderGroup(READER_GROUP_NAME,
                ReaderGroupConfig.builder().stream(io.pravega.client.stream.Stream.of(SCOPE, STREAM)).build());
        @Cleanup
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(READER_GROUP_NAME);

        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerURI);

        //Start 3 writers.
        List<CompletableFuture<Void>> writerFutureList = new ArrayList<>();
        writerFutureList.add(CompletableFuture.<Void>supplyAsync(() -> {
            createWriter(clientFactory).start();
            return null;
        }, writerExecutor));

        writerFutureList.add(CompletableFuture.<Void>supplyAsync(() -> {
            createWriter(clientFactory).start();
            return null;
        }, writerExecutor));

        writerFutureList.add(CompletableFuture.<Void>supplyAsync(() -> {
            createWriter(clientFactory).start();
            return null;
        }, writerExecutor));

        //Start read an verify after a delay of 30 seconds.
        CompletableFuture<Void> verifyFuture = Futures.delayedFuture(Duration.ofSeconds(30), verifyExecutor)
                .thenComposeAsync(v -> {
                    try {
                        readAndVerifyOrder(clientFactory);
                    } catch (ReinitializationRequiredException e) {
                        log.error("=> Reinitialization Required Exception observed", e);
                        throw new CompletionException(e);
                    }
                    return null;
                }, verifyExecutor)
                .handle((o, ex) -> {
                    log.error("Verification Error", ex);
                    return null;
                });

        //Wait until all the events are read.
        verifyFuture.get();
        readerGroupManager.deleteReaderGroup(READER_GROUP_NAME); //clean up
    }

    private URI fetchControllerURI() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        return ctlURIs.get(0);
    }

    private void readAndVerifyOrder( ClientFactory clientFactory) throws io.pravega.client.stream.ReinitializationRequiredException {
        Map<String, Long> resultMap = new HashMap<>(); //attempt to store the last sequence number.

        @Cleanup
        EventStreamReader<PravegaTestPayload> reader = clientFactory.createReader("readerId", "reader", new
                        JavaSerializer<>(),
                ReaderConfig.builder().build());
        EventRead<PravegaTestPayload> event = reader.readNextEvent(10000);
        long eventRead = 1L;
        while (event.getEvent() != null) {
            //update result Map
            eventRead++;
            PravegaTestPayload result = event.getEvent();

            log.info("==> Event Read : {}", result);
            if (resultMap.get(result.getSenderId()) == null) {
                resultMap.put(result.getSenderId(), result.getSequenceNumber());
            } else {
                if (resultMap.get(result.getSenderId()) < result.getSequenceNumber()) {
                    resultMap.put(result.getSenderId(), result.getSequenceNumber());
                } else {
                    Assert.fail("order mismatch");
                }
            }
            event = reader.readNextEvent(4000);
        }
        log.info("==> Number of events Read : {}", eventRead);
    }

    private PravegaWriter createWriter(ClientFactory clientFactory) {
        PravegaWriter writer = new PravegaWriter().withClientFactory(clientFactory).withStreamName("test")
                .withWriterFinishedHandler(t -> log.error("==> Error while writing", t))
                .withMaxEventCount(8_000_000L);
        writer.prepare();
        return writer;
    }
}
