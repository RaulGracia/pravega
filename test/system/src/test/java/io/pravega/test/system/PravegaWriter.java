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
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for Pravega Writer Workers that performs the basic stuff. (Copied from platform repo)... with minor changes
 */
@Slf4j
public class PravegaWriter {

    private final JavaSerializer<PravegaTestPayload> serializer = new JavaSerializer<>();

    private String streamName;
    private ClientFactory clientFactory;
    private AtomicBoolean writerFinished = new AtomicBoolean(false);

    private int maxOutstandingAcks = 4_000;
    private Semaphore pendingAckPermits = new Semaphore(maxOutstandingAcks);
    private Consumer<Throwable> writerFinishedHandler;

    private String taskId = UUID.randomUUID().toString();
    private AtomicLong sequenceCounter = new AtomicLong(1);
    private List<String> keys = Stream.generate( () -> UUID.randomUUID().toString() ).limit(20).collect(Collectors.toList());
    private Random rgen = new Random();
    private Supplier<String> keyGenerator = () -> keys.get(rgen.nextInt(keys.size()));
    private long maxEventCount;
    private EventStreamWriter<PravegaTestPayload> producer;

    public PravegaWriter withClientFactory(ClientFactory clientFactory) {
        this.clientFactory = clientFactory;
        return this;
    }

    public PravegaWriter withStreamName(String streamName) {
        this.streamName = streamName;
        return this;
    }

    public PravegaWriter withWriterFinishedHandler(Consumer<Throwable> handler) {
        this.writerFinishedHandler = handler;
        return this;
    }
    
    public PravegaWriter withMaxEventCount(long maxEventCount) {
        this.maxEventCount = maxEventCount;
        return this;
    }

    public void prepare() {
        EventWriterConfig eventWriterConfig = EventWriterConfig.builder().build();
        producer = clientFactory.createEventWriter(streamName, serializer, eventWriterConfig);
    }

    public void close() {
        producer.close();
    }

    public void start() {
        try {
            log.debug("I'm the only one? " + taskId);
            while (!writerFinished.get()) {

                pendingAckPermits.acquire();
                writeEvent()
                        .thenRun(pendingAckPermits::release);
            }

            producer.flush();

            pendingAckPermits.acquire(maxOutstandingAcks);
            finished(null);
        } catch (InterruptedException ignore) {
            finished(null);
        } catch (Throwable e) {
            finished(e);
        }
    }

    protected CompletableFuture<Void> writeEvent() throws InterruptedException {
        String nextKey = keyGenerator.get();

        long sequenceNumber = sequenceCounter.incrementAndGet();
        if (sequenceNumber > maxEventCount) {
            log.info("==> Reached max limit");
            throw new InterruptedException("Reached max limit");
        }
        String senderId = SequenceValidator.createSenderId(taskId, nextKey);
        log.info("==>+ {} -> {}", senderId, sequenceNumber);
        PravegaTestPayload payload = new PravegaTestPayload(System.currentTimeMillis(), senderId, sequenceNumber, "nonsense");

        CompletableFuture<Void> writeEvent = producer.writeEvent(nextKey, payload);
        return writeEvent.whenComplete((res, ex) -> {
            if (ex != null) {
                this.finished(ex);
            }
        });
    }

    protected void finished(Throwable e) {
        boolean wasAlreadyFinished = writerFinished.getAndSet(true);

        if (!wasAlreadyFinished) {
            if (e != null) {
                log.error("Writer finished with Error", e);
            } else {
                log.info("Writer finished");

            }

            if (writerFinishedHandler != null) {
                writerFinishedHandler.accept(e);
            }
        }
    }
}
