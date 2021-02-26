/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.CompositeByteArraySegment;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.test.common.TestUtils;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

@RunWith(JUnitParamsRunner.class)
@Slf4j
public class BookkeeperLogPerfTests {

    private static final AtomicInteger LOG_ID = new AtomicInteger(9999);
    private static final int THREAD_POOL_SIZE = 3;
    private static final int MAX_WRITE_ATTEMPTS = 3;
    private static final ConcurrentMap<Triple, Duration> durationByCase = new ConcurrentHashMap<>();

    private final Random random = new Random(0);
    private final AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
    private final AtomicReference<DurableDataLogFactory> factory = new AtomicReference<>();

    private BookKeeperServiceRunner bookiesRunner;
    private ScheduledExecutorService executorService = null;

    @Before
    public void setup() {
        startBookkeeper(1);
    }

    @SneakyThrows
    @After
    public void tearDown() {
        log.debug("Shutting down bookkeeper");
        shutDownBookkeeper();
    }

    @SneakyThrows
    @AfterClass
    public static void tearDownEnv() {
        printDurations();
    }

    @SneakyThrows
    @Test
    /*@Parameters({
            "100, 20",
            "2000, 20"
    })*/
    /*@Parameters({
            "100,   20",
            "2000,  20",
            "10000, 20",
            "30000, 20",
            "70000, 20",
            "100,   100",
            "2000,  100",
            "10000, 100",
            "30000, 100",
            "70000, 100",
            "100,   2000",
            "2000,  2000",
            "10000, 2000",
            "30000, 2000",
            "70000, 2000",
    })*/
    @Parameters({
            "200000, 4096",
            "500000, 4096",
    })
    public void writeLotsOfItems(int numEntries, int bytesLength) {
        List<CompletableFuture<LogAddress>> writeFutures = new ArrayList<>();

        @Cleanup
        BookKeeperLog bkLogWriter = (BookKeeperLog)this.factory.get().createDurableDataLog(
                LOG_ID.incrementAndGet());
        bkLogWriter.initialize(Duration.ofSeconds(60));
        log.debug("No. of ledgers: {}", bkLogWriter.loadMetadata().getLedgers().size());

        Instant startInstant = Instant.now();

        byte[] writeData = generateDummyWriteData(bytesLength);

        // Write data to Bookkeeper asynchronously
        for (int i = 0; i < numEntries; i++) {
            CompletableFuture<LogAddress> appendFuture =
                    bkLogWriter.append(new CompositeByteArraySegment(writeData), Duration.ofSeconds(60));
            writeFutures.add(appendFuture);
        }

        // Wait for all write futures to complete
        List<LogAddress> logEntries = Futures.allOfWithResults(writeFutures).join();
        Duration duration = Duration.between(startInstant, Instant.now());

        durationByCase.putIfAbsent(Triple.of(numEntries, bytesLength, System.currentTimeMillis()), duration);

        // Verify test resuts
        assertEquals(numEntries, logEntries.size());
    }


    private static void printDurations() {
        durationByCase.forEach((triplet, duration) -> {
            log.info("Took {} milliseconds to write {} entries of length {} at time{}",
                    duration.toMillis(), triplet.getLeft(), triplet.getMiddle(), triplet.getRight());
        });
    }

    private byte[]  generateDummyWriteData(int length) {
        byte[] data = new byte[length];
        this.random.nextBytes(data);
        return data;
    }

    private ScheduledExecutorService executorService() {
        ScheduledThreadPoolExecutor es = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);
        es.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        es.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        es.setRemoveOnCancelPolicy(true);
        return es;
    }

    private void shutDownBookkeeper() throws Exception {
        this.executorService.shutdownNow();
        this.executorService.awaitTermination(5, TimeUnit.SECONDS);

        DurableDataLogFactory dataLogFactory = this.factory.getAndSet(null);
        if (dataLogFactory != null) {
            dataLogFactory.close();
        }

        val zkClient = this.zkClient.getAndSet(null);
        if (zkClient != null) {
            zkClient.close();
        }

        log.debug("tearDownEnv entry");
        if (bookiesRunner != null) {
            bookiesRunner.close();
        }
    }

    @SneakyThrows
    private void startBookkeeper(int bookieCount) {
        log.debug("Staring Bookkeeper");

        int zookeeperPort = TestUtils.getAvailableListenPort();

        List<Integer> bookiePorts = new ArrayList<Integer>();
        for (int i = 0; i < bookieCount; i++) {
            bookiePorts.add(TestUtils.getAvailableListenPort());
        }

        bookiesRunner = BookKeeperServiceRunner.builder()
                .startZk(true)
                .zkPort(zookeeperPort)
                .ledgersPath("/pravega/bookkeeper/ledgers")
                .secureBK(false)
                .secureZK(false)
                .bookiePorts(bookiePorts)
                .build();

        bookiesRunner.startAll();
        log.debug("Bookkeeper Server started");

        log.debug("setup entry");
        // Create a ZKClient with a unique namespace.
        String namespace = "pravega/segmentstore/unittest_" + Long.toHexString(System.nanoTime());
        this.zkClient.set(CuratorFrameworkFactory
                .builder()
                .connectString("localhost:" + zookeeperPort)
                .namespace(namespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build());
        this.zkClient.get().start();

        // Setup bkConfig to use the port and namespace.

        BookKeeperConfig bookieConfig = BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + zookeeperPort)
                .with(BookKeeperConfig.MAX_WRITE_ATTEMPTS, MAX_WRITE_ATTEMPTS)
                .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, Integer.MAX_VALUE)
                .with(BookKeeperConfig.ZK_METADATA_PATH, namespace)
                .with(BookKeeperConfig.BK_LEDGER_PATH, "/pravega/bookkeeper/ledgers")
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, bookieCount)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, bookieCount)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, bookieCount)
                .with(BookKeeperConfig.BK_WRITE_TIMEOUT, 1000) // This is the minimum we can set anyway.
                .build();

        this.executorService = executorService();

        // Create default factory.
        DurableDataLogFactory factory = new BookKeeperLogFactory(bookieConfig, this.zkClient.get(),
                this.executorService);
        factory.initialize();
        this.factory.set(factory);
    }
}

