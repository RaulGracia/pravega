/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Processes items in order, subject to capacity constraints.
 */
@ThreadSafe
@Slf4j
public class OrderedItemProcessor<ItemType, ResultType> implements AutoCloseable {
    //region members

    private static final int CLOSE_TIMEOUT_MILLIS = 60 * 1000;
    private final int capacity;
    @GuardedBy("processingLock")
    private final Function<ItemType, CompletableFuture<ResultType>> processor;
    @GuardedBy("stateLock")
    private final Deque<QueueItem> pendingItems;
    private final boolean closeOnException;
    private final Executor executor;

    /**
     * Guards access to the OrderedItemProcessor's internal state (counts and queues).
     */
    private final Object stateLock = new Object();

    /**
     * Guards access to the individual item's processors. No two invocations of the processor may run at the same time.
     */
    private final Object processingLock = new Object();
    @GuardedBy("stateLock")
    private int activeCount;
    @GuardedBy("stateLock")
    private boolean closed;
    @GuardedBy("stateLock")
    private ReusableLatch emptyNotifier;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OrderedItemProcessor class.
     *
     * @param capacity  The maximum number of concurrent executions.
     * @param processor A Function that, given an Item, returns a CompletableFuture that indicates when the item has been
     *                  processed (successfully or not).
     * @param closeOnException Whether or not the OrderedItemProcessor should shut down when it throws an exception
     *                         processing an item. The default behavior is to close the processor on exception.
     * @param executor  An Executor for async invocations.
     */
    public OrderedItemProcessor(int capacity, Function<ItemType, CompletableFuture<ResultType>> processor,
                                boolean closeOnException, Executor executor) {
        Preconditions.checkArgument(capacity > 0, "capacity must be a non-negative number.");
        this.capacity = capacity;
        this.processor = Preconditions.checkNotNull(processor, "processor");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.pendingItems = new ArrayDeque<>();
        this.activeCount = 0;
        this.closeOnException = closeOnException;
    }

    public OrderedItemProcessor(int capacity, Function<ItemType, CompletableFuture<ResultType>> processor, Executor executor) {
        this(capacity, processor, true, executor);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    @SneakyThrows(Exception.class)
    public void close() {
        ReusableLatch waitSignal = null;
        synchronized (this.stateLock) {
            if (this.closed) {
                return;
            }

            this.closed = true;
            if (this.activeCount != 0 || !this.pendingItems.isEmpty()) {
                // Setup a latch that will be released when the last item completes.
                this.emptyNotifier = new ReusableLatch(false);
                waitSignal = this.emptyNotifier;
            }
        }

        if (waitSignal != null) {
            // We have unfinished items. Wait for them.
            waitSignal.await(CLOSE_TIMEOUT_MILLIS);
        }
    }

    //endregion

    //region Processing

    /**
     * Processes the given item.
     * * If the OrderedItemProcessor is below capacity, then the item will be processed immediately and the returned
     * future is the actual result from the Processor.
     * * If the max capacity is exceeded, the item will be queued up and it will be processed when capacity allows, after
     * all the items added before it have been processed.
     * * If an item before this one failed to process and this item is still in the queue (it has not yet been processed),
     * the returned future will be cancelled with a ProcessingException to prevent out-of-order executions.
     * * This method guarantees ordered execution as long as its invocations are serial. That is, it only guarantees that
     * the item has begun processing or an order assigned if the method returned successfully.
     * * If an item failed to execute, this class will auto-close and will not be usable anymore, unless the parameter
     * closeOnException is set to false.
     *
     * @param item The item to process.
     * @return A CompletableFuture that, when completed, will indicate that the item has been processed. This will contain
     * the result of the processing function applied to this item.
     */
    public CompletableFuture<ResultType> process(ItemType item) {
        Preconditions.checkNotNull(item, "item");
        CompletableFuture<ResultType> result = null;
        log.info("OrderedItemProcessor - process1: {}", item);
        synchronized (this.stateLock) {
            log.info("OrderedItemProcessor - process2 after lock: {}", item);
            Exceptions.checkNotClosed(this.closed, this);
            if (hasCapacity() && this.pendingItems.isEmpty()) {
                log.info("OrderedItemProcessor - process3 has capacity and no pending items: {}", item);
                // Not at capacity. Reserve a spot now.
                this.activeCount++;
            } else {
                log.info("OrderedItemProcessor - process4 else: {}", item);
                // We are at capacity or have a backlog. Put the item in the queue and return its associated future.
                result = new CompletableFuture<>();
                this.pendingItems.add(new QueueItem(item, result));
            }
        }
        log.info("OrderedItemProcessor - process5 before result: {}", item);
        if (result == null) {
            // We are below max capacity, so simply process the item, without queuing up.
            // It is possible, under some conditions, that we may get in here when the queue empties up (by two spots at
            // the same time). In that case, we need to acquire the processing lock to ensure that we don't accidentally
            // process this item before we finish processing the last item in the queue.
            log.info("OrderedItemProcessor - process6 before second lock: {}", item);
            synchronized (this.processingLock) {
                log.info("OrderedItemProcessor - process7 after second lock: {}", item);
                result = processInternal(item);
            }
        }

        return result;
    }

    /**
     * Callback that is invoked when an item has completed execution.
     *
     * @param exception (Optional) An exception from the execution. If set, it indicates the item has not been
     *                  processed successfully.
     */
    @VisibleForTesting
    protected void executionComplete(Throwable exception) {
        log.info("OrderedItemProcessor - executionComplete1 : ", exception);
        Collection<QueueItem> toFail = null;
        Throwable failEx = null;
        synchronized (this.stateLock) {
            log.info("OrderedItemProcessor - executionComplete2 after lock: ", exception);
            // Release the spot occupied by this item's execution.
            this.activeCount--;
            if (exception != null) {
                log.warn("Exception thrown while processing item: ", exception);
                // Only close the processor and cancel pending items if exceptions from processed items are not allowed.
                if (!this.closed && this.closeOnException) {
                    // Need to fail all future items and close to prevent new items from being processed.
                    failEx = new ProcessingException("A previous item failed to commit. Cannot process new items.", exception);
                    toFail = new ArrayList<>(this.pendingItems);
                    this.pendingItems.clear();
                    this.closed = true;
                }
            }

            if (this.emptyNotifier != null && this.activeCount == 0 && this.pendingItems.isEmpty()) {
                // We were asked to notify when we were empty.
                this.emptyNotifier.release();
                this.emptyNotifier = null;
            }
        }
        log.info("OrderedItemProcessor - executionComplete3 before toFail: ", exception);
        if (toFail != null) {
            for (QueueItem q : toFail) {
                q.result.completeExceptionally(failEx);
            }

            return;
        }

        log.info("OrderedItemProcessor - executionComplete4 before processingLock: ", exception);
        // We need to ensure the items are still executed in order. Once out of the main sync block, it is possible that
        // this callback may be invoked concurrently after various completions. As such, we need a special handler for
        // these, using its own synchronization, different from the main one, so that we don't hold that stateLock for too long.
        synchronized (this.processingLock) {
            while (true) {
                QueueItem toProcess;
                log.info("OrderedItemProcessor - executionComplete5 before stateLock: ", exception);
                synchronized (this.stateLock) {
                    log.info("OrderedItemProcessor - executionComplete6 after stateLock: ", exception);
                    if (hasCapacity() && !this.pendingItems.isEmpty()) {
                        // We have spare capacity and we have something to process. Dequeue it and reserve the spot.
                        toProcess = this.pendingItems.pollFirst();
                        this.activeCount++;
                        log.info("OrderedItemProcessor - executionComplete7 found new element to process: {}", toProcess);
                    } else {
                        log.info("OrderedItemProcessor - executionComplete8 break ", exception);
                        // No capacity left or no more pending items.
                        break;
                    }
                }
                log.info("OrderedItemProcessor - executionComplete9 after stateLock: {}", toProcess.data);
                Futures.completeAfter(() -> processInternal(toProcess.data), toProcess.result);
            }
        }
    }

    @GuardedBy("processingLock")
    private CompletableFuture<ResultType> processInternal(ItemType data) {
        try {
            log.info("OrderedItemProcessor - processInternal1 pending items size: {}", this.pendingItems.size());
            log.info("OrderedItemProcessor - processInternal2: {}", data);
            val result = this.processor.apply(data);
            log.info("OrderedItemProcessor - processInternal3 after apply: {}", data);
            result.whenCompleteAsync((r, ex) -> executionComplete(ex), this.executor);
            log.info("OrderedItemProcessor - processInternal4 after whenCompleteAsync: {}", data);
            return result;
        } catch (Throwable ex) {
            log.info("OrderedItemProcessor - processInternal5 catch: ", ex);
            if (!Exceptions.mustRethrow(ex)) {
                executionComplete(ex);
            }

            throw ex;
        }
    }

    @GuardedBy("stateLock")
    private boolean hasCapacity() {
        return this.activeCount < this.capacity;
    }

    //endregion

    //region ProcessingException

    public static class ProcessingException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private ProcessingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    //endregion

    //region QueueItem

    @RequiredArgsConstructor
    private class QueueItem {
        final ItemType data;
        final CompletableFuture<ResultType> result;
    }

    //endregion
}
