/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AtomicDouble;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.PromiseCombiner;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.shared.metrics.MetricNotifier;
import io.pravega.shared.protocol.netty.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.jcip.annotations.GuardedBy;

import static io.pravega.shared.NameUtils.segmentTags;
import static io.pravega.shared.metrics.ClientMetricKeys.CLIENT_APPEND_LATENCY;

@Slf4j
public class ClientConnectionImpl implements ClientConnection {

    @Getter
    private final String connectionName;
    @Getter
    private final int flowId;
    @VisibleForTesting
    @Getter
    private final FlowHandler nettyHandler;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Semaphore throttle = new Semaphore(AppendBatchSizeTracker.MAX_BATCH_SIZE);

    // With these values we calculate whether it is worth it to switch to batch mode.
    private final AtomicDouble eventCount = new AtomicDouble(0);
    private final AtomicDouble byteCount = new AtomicDouble(0);
    private final AtomicLong lastRateCalculation = new AtomicLong(0);
    private final AtomicBoolean batchMode = new AtomicBoolean(false);
    private final long BATCH_MODE_EVENT_THRESHOLD = 10000;
    private final long BATCH_MODE_BYTE_THRESHOLD = 10000000;
    private final long BATCH_SIZE_BYTES = 1 * 1024 * 1024;
    private final long BATCH_SIZE_EVENTS = 500;

    @GuardedBy("appends")
    private final List<CommandAndPromise> appends = new ArrayList<>();
    @GuardedBy("appends")
    private AtomicLong batchSizeBytes = new AtomicLong(0);
    @GuardedBy("appends")
    private AtomicLong batchSizeEvents = new AtomicLong(0);
    private final AtomicBoolean shouldFlush = new AtomicBoolean(false);
    private final AtomicLong tokenCounter = new AtomicLong(0);

    @SneakyThrows
    public ClientConnectionImpl(String connectionName, int flowId, FlowHandler nettyHandler) {
        this.connectionName = connectionName;
        this.flowId = flowId;
        this.nettyHandler = nettyHandler;
    }

    @Override
    public void send(WireCommand cmd) throws ConnectionFailedException {
        checkClientConnectionClosed();
        nettyHandler.setRecentMessage();
        write(cmd);
    }

    @Override
    public void send(Append append) throws ConnectionFailedException {
        Timer timer = new Timer();
        checkClientConnectionClosed();
        nettyHandler.setRecentMessage();
        write(append);
        // Monitoring appends has a performance cost (e.g., split strings); only do that if we configure a metric notifier.
        if (!nettyHandler.getMetricNotifier().equals(MetricNotifier.NO_OP_METRIC_NOTIFIER)) {
            nettyHandler.getMetricNotifier()
                    .updateSuccessMetric(CLIENT_APPEND_LATENCY, segmentTags(append.getSegment(), append.getWriterId().toString()),
                            timer.getElapsedMillis());
        }
    }

    private void write(Append cmd) throws ConnectionFailedException {
        Channel channel = nettyHandler.getChannel();
        ChannelPromise promise = channel.newPromise();
        promise.addListener((ChannelFutureListener) future -> {
            throttle.release(cmd.getDataLength());
            if (!future.isSuccess()) {
                future.channel().pipeline().fireExceptionCaught(future.cause());
                future.channel().close();
            }
        });
        writeToBatch(cmd, promise);
        if (!batchMode.get() || (batchMode.get() && isBatchSizeMet())) {
            flushBatch();
        }
    }

    private void write(WireCommand cmd) throws ConnectionFailedException {
        Channel channel = nettyHandler.getChannel();
        ChannelPromise promise = channel.newPromise();
        promise.addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                future.channel().pipeline().fireExceptionCaught(future.cause());
                future.channel().close();
            }
        });
        writeToBatch(cmd, promise);
        flushBatch();
    }

    private void writeToBatch(Object cmd, ChannelPromise channelPromise) {
        synchronized (appends) {
            // Append both the command itself and the promise in the batch of appends.
            appends.add(new CommandAndPromise(cmd, channelPromise));
        }
        if (cmd instanceof Append) {
            batchSizeBytes.getAndAdd(((Append) cmd).getDataLength());
            byteCount.getAndAdd(((Append) cmd).getDataLength());
            batchSizeEvents.getAndAdd(1);
            eventCount.getAndAdd(1);
        }
        // Mark the batch as candidate to flush.
        if (batchMode.get() && shouldFlush.compareAndSet(false, true)) {
            channelPromise.channel().eventLoop().schedule(new BlockTimeouter(tokenCounter.get()),
                    AppendBatchSizeTracker.MAX_BATCH_TIME_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    private void flushBatch() throws ConnectionFailedException {
        Channel channel = nettyHandler.getChannel();
        EventLoop eventLoop = channel.eventLoop();
        synchronized (appends) {
            for (CommandAndPromise append: appends) {
                // Work around for https://github.com/netty/netty/issues/3246
                eventLoop.execute(() -> {
                    channel.write(append.getCommand(), append.getPromise());
                });
                if (append.getCommand() instanceof Append) {
                    Exceptions.handleInterrupted(() -> throttle.acquire(((Append) append.getCommand()).getDataLength()));
                }
            }
            appends.clear();
        }
        shouldFlush.set(false);
        tokenCounter.incrementAndGet();
        batchSizeBytes.set(0);
        batchSizeEvents.set(0);
        updateIORate();
    }

    @Override
    public void sendAsync(WireCommand cmd, CompletedCallback callback) {
        Channel channel = null;
        try {
            checkClientConnectionClosed();
            nettyHandler.setRecentMessage();
            channel = nettyHandler.getChannel();
            log.debug("Write and flush message {} on channel {}", cmd, channel);
            channel.writeAndFlush(cmd)
                    .addListener((Future<? super Void> f) -> {
                        if (f.isSuccess()) {
                            callback.complete(null);
                        } else {
                            callback.complete(new ConnectionFailedException(f.cause()));
                        }
                    });
        } catch (ConnectionFailedException cfe) {
            log.debug("ConnectionFailedException observed when attempting to write WireCommand {} ", cmd);
            callback.complete(cfe);
        } catch (Exception e) {
            log.warn("Exception while attempting to write WireCommand {} on netty channel {}", cmd, channel);
            callback.complete(new ConnectionFailedException(e));
        }
    }

    @Override
    public void sendAsync(List<Append> appends, CompletedCallback callback) {
        Channel ch;
        try {
            checkClientConnectionClosed();
            nettyHandler.setRecentMessage();
            ch = nettyHandler.getChannel();
        } catch (ConnectionFailedException e) {
            callback.complete(new ConnectionFailedException("Connection to " + connectionName + " is not established."));
            return;
        }
        PromiseCombiner combiner = new PromiseCombiner();
        for (Append append : appends) {
            combiner.add(ch.write(append));
        }
        ch.flush();
        ChannelPromise promise = ch.newPromise();
        promise.addListener(future -> {
            Throwable cause = future.cause();
            callback.complete(cause == null ? null : new ConnectionFailedException(cause));
        });
        combiner.finish(promise);
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            nettyHandler.closeFlow(this);
        }
    }

    private void checkClientConnectionClosed() throws ConnectionFailedException {
        if (closed.get()) {
            log.error("ClientConnection to {} with flow id {} is already closed", connectionName, flowId);
            throw new ConnectionFailedException("Client connection already closed for flow " + flowId);
        }
    }

    private boolean isBatchSizeMet() {
        return batchSizeBytes.get() >= BATCH_SIZE_BYTES || batchSizeEvents.get() >= BATCH_SIZE_EVENTS;
    }

    private void updateIORate() {
        if (System.currentTimeMillis() - lastRateCalculation.get() >= 1000) {
            final double currentEventRate = (eventCount.get() / (System.currentTimeMillis() - lastRateCalculation.get())) * 1000.0;
            final double currentByteRate = (byteCount.get() / (System.currentTimeMillis() - lastRateCalculation.get())) * 1000.0;
            if (currentEventRate >= BATCH_MODE_EVENT_THRESHOLD || currentByteRate >= BATCH_MODE_BYTE_THRESHOLD) {
                batchMode.set(true);
            } else {
                batchMode.set(false);
            }
            eventCount.set(0);
            byteCount.set(0);
            lastRateCalculation.set(System.currentTimeMillis());
        }
    }

    @AllArgsConstructor
    private static class CommandAndPromise {
        @Getter
        private final Object command;
        @Getter
        private final ChannelPromise promise;
    }

    @RequiredArgsConstructor
    private final class BlockTimeouter implements Runnable {
        private final long token;

        /**
         * Check if the current token is still valid.
         * If its still valid, then block Timeout message is sent to netty Encoder.
         */
        @SneakyThrows
        @Override
        public void run() {
            //System.err.println("tokenCounter: " + tokenCounter.get() + ", token: " + token);
            if (tokenCounter.get() == token) {
                flushBatch();
            }
        }
    }
}
