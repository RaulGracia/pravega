/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.host.handler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.pravega.common.Exceptions;
import io.pravega.common.MathHelpers;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.SimpleCache;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.pravega.segmentstore.contracts.ReadResultEntryType.Cache;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.EndOfStreamSegment;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.Future;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.Storage;
import static io.pravega.segmentstore.contracts.ReadResultEntryType.Truncated;

/**
 * This class is responsible for issuing asynchronous prefetch reads based on incoming reads to regular Segments. It
 * does two main tasks:
 * - Keep track of current Segments being read as well as their prefetched data, if any.
 * - Issue asynchronous reads for Segments that require prefetching data.
 *
 * An asynchronous prefetch read will be issued for a Segment only if the following conditions apply:
 * - Reads are related to a regular Segment (e.g., not a Table Segment) that is not system-related (i.e., not in _system scope).
 * - The Direct Memory cache has space to allow further reads.
 * - The last user read is sequential with respect with the last tracked one.
 * - The last user read comes from Storage or, in the case the user read comes from cache (from already prefetched data),
 * the amount of already prefetched data for a Segment is lower than (1 - {@link ReadPrefetchManagerConfig#CONSUMED_PREFETCHED_DATA_THRESHOLD}) *
 * {@link ReadPrefetchManagerConfig#PREFETCH_READ_LENGTH}.
 */
@Slf4j
public class ReadPrefetchManager implements AutoCloseable {
    //region Members

    @Getter(AccessLevel.PACKAGE)
    @VisibleForTesting
    private final Supplier<Boolean> canPrefetch;
    @Getter(AccessLevel.PACKAGE)
    @VisibleForTesting
    private final SimpleCache<UUID, SegmentPrefetchInfo> prefetchingInfoCache;
    @Getter
    @VisibleForTesting
    private final int prefetchReadLength;
    @Getter(AccessLevel.PACKAGE)
    @VisibleForTesting
    private final double consumedPrefetchedDataThreshold;
    private final MultiKeySequentialProcessor<UUID> readPrefetchProcessor;
    private final ExecutorService executorService;
    private final SegmentStoreMetrics.ReadPrefetch readPrefetchMetrics;
    private final Duration timeout = Duration.ofSeconds(10);

    //endregion

    //region Constructor

    public ReadPrefetchManager(@NonNull Supplier<Boolean> canPrefetch, @NonNull ReadPrefetchManagerConfig config, @NonNull ExecutorService executorService) {
        this.canPrefetch = canPrefetch;
        this.prefetchReadLength = config.getPrefetchReadLength();
        this.consumedPrefetchedDataThreshold = config.getConsumedPrefetchedDataThreshold();
        this.prefetchingInfoCache = new SimpleCache<>(config.getTrackedEntriesMaxCount(), config.getTrackedEntriesEvictionTimeSeconds(),
                (key, value) -> log.debug("Evicting prefetch key: {}, value: {}", key, value));
        this.readPrefetchProcessor = new MultiKeySequentialProcessor<>(executorService);
        this.readPrefetchMetrics = new SegmentStoreMetrics.ReadPrefetch();
        this.executorService = executorService;
    }

    @VisibleForTesting
    public ReadPrefetchManager() {
        this(() -> true, ReadPrefetchManagerConfig.builder().build(), ForkJoinPool.commonPool());
    }

    //endregion

    /**
     * Collect information from external reads on a Segment. This information will help us to decide whether to issue
     * prefetch reads or not.
     *
     * @param request Read request sent from the client.
     * @param result Read result after performing the read.
     * @param fromStorage Whether the read process detected that we fetched data for this Segment from Storage.
     */
    void collectInfoFromUserRead(@NonNull WireCommands.ReadSegment request, @NonNull ReadResult result, boolean fromStorage) {
        if (isPrefetchingDisallowed(request.getSegment())) {
            // If prefetch is disallowed, there is no point on gathering information for this Segment.
            return;
        }
        // Identifier that combines the Segment name and the reader (i.e., connection) to perform prefetching.
        UUID prefetchId = createPrefetchId(request.getSegment(), request.getRequestId());
        // Queue a task to add the info to the prefetchingInfoCache.
        this.readPrefetchProcessor.add(ImmutableList.of(prefetchId), () -> CompletableFuture.supplyAsync(() -> {
            SegmentPrefetchInfo segmentPrefetchInfo = this.prefetchingInfoCache.get(prefetchId);
            if (segmentPrefetchInfo == null) {
                // Initialize the entry for this reader and Segment.
                this.prefetchingInfoCache.put(prefetchId, new SegmentPrefetchInfo(result.getStreamSegmentStartOffset(),
                        result.getMaxResultLength(), fromStorage, this.prefetchReadLength));
            } else {
                segmentPrefetchInfo.updateInfoFromUserRead(result.getStreamSegmentStartOffset(), result.getMaxResultLength(), fromStorage);
            }
            return null;
        }, this.executorService));
    }

    /**
     * Attempts to trigger an asynchronous prefetch read for a given Segment and offset, if the required conditions are met.
     *
     * @param segmentStore Segment Store instance to trigger the prefetch read.
     * @param segment Segment to read from.
     * @param request Read request related to the prefetch read to be generated.
     */
    CompletableFuture<Void> tryPrefetchData(@NonNull StreamSegmentStore segmentStore, @NonNull String segment, @NonNull WireCommands.ReadSegment request) {
        if (isPrefetchingDisallowed(segment)) {
            // Prefetching is disallowed, do nothing.
            return CompletableFuture.completedFuture(null);
        }

        // Now, we need to check if we can issue a prefetch read request for this Segment and Reader.
        final UUID prefetchId = createPrefetchId(segment, request.getRequestId());
        return this.readPrefetchProcessor.add(ImmutableList.of(prefetchId), () -> buildPrefetchingReadFuture(segmentStore, segment, prefetchId));
    }

    private boolean isPrefetchingDisallowed(String segment) {
        // We can prefetch only if canPrefetch evaluates to true (i.e., Direct Memory cache has space) and the Segment is not system-related.
        boolean isPrefetchingDisallowed = !canPrefetch.get() || !isPrefetchableSegmentType(segment);
        if (!canPrefetch.get()) {
            // Direct Memory Cache is full, let's warn about it.
            log.warn("Not allowed to trigger prefetch reads (Direct Memory cache may be under pressure).");
        }
        return isPrefetchingDisallowed;
    }

    private boolean isPrefetchableSegmentType(String segment) {
        return !NameUtils.isSegmentInSystemScope(segment);
    }

    private CompletableFuture<Void> buildPrefetchingReadFuture(@NonNull StreamSegmentStore segmentStore, @NonNull String segment, UUID prefetchId) {
        Timer prefetchReadLatency = new Timer();
        return segmentStore.getStreamSegmentInfo(segment, timeout)
                .thenApply(segmentProperties -> checkPrefetchPreconditions(prefetchId, segmentProperties))
                .thenApply(segmentProperties -> calculatePrefetchReadLength(prefetchId, segmentProperties))
                .thenCompose(offsetAndLength -> segmentStore.read(segment, offsetAndLength.getLeft(), offsetAndLength.getRight(), timeout))
                .handle((prefetchReadResult, ex) -> {
                    if (ex != null) {
                        if (Exceptions.unwrap(ex) instanceof UnableToPrefetchException || Exceptions.unwrap(ex) instanceof PrefetchNotNeededException) {
                            log.debug("Not prefetching this time.");
                        } else {
                            log.error("Problem while performing a prefetch read.", ex);
                        }
                    } else {
                        ImmutablePair<Integer, Boolean> prefetchResultInfo = prefetchReadCallback(prefetchReadResult);
                        // Update the prefetching information for this entry based on the read result.
                        SegmentPrefetchInfo segmentPrefetchInfo = this.prefetchingInfoCache.get(prefetchId);
                        if (segmentPrefetchInfo != null) {
                            segmentPrefetchInfo.setPrefetchedDataLength(prefetchResultInfo.getLeft());
                            segmentPrefetchInfo.setPrefetchStartOffset(prefetchReadResult.getStreamSegmentStartOffset());
                            segmentPrefetchInfo.setPrefetchEndOffset(prefetchReadResult.getStreamSegmentStartOffset() + prefetchResultInfo.getLeft());
                            segmentPrefetchInfo.setLastReadFromStorage(prefetchResultInfo.getRight());
                        }
                        // Report metrics after a successful prefetch read.
                        this.readPrefetchMetrics.reportPrefetchDataRead(prefetchResultInfo.getLeft());
                        this.readPrefetchMetrics.reportPrefetchDataReadLatency(prefetchReadLatency.getElapsedMillis());
                    }
                    return null;
                });
    }

    /**
     * Creates a new UUID that identifies the prefetching state for this pair of Segment and Reader.
     *
     * @param segmentName Name of the Segment being read.
     * @param readerId Identifier for the reader connection (persistent across reads if the connection is alive).
     * @return Identifier to keep the prefetching state for this pair of Segment and Reader.
     */
    @VisibleForTesting
    UUID createPrefetchId(String segmentName, long readerId) {
        return new UUID(segmentName.hashCode(), readerId);
    }

    /**
     * To issue a prefetch read, the following preconditions should be met: i) The Segment should not be truncated,
     * ii) The Segment should not be finished, iii) The Segment should not be read at tail, iv) The last read for this
     * Segment and reader should have been fetched from storage, v) Either there is no prefetched data for this reader
     * or the amount of prefetched data is lower than {@link #consumedPrefetchedDataThreshold}.
     *
     * @param prefetchId
     * @return
     */
    @VisibleForTesting
    SegmentProperties checkPrefetchPreconditions(UUID prefetchId, SegmentProperties segmentProperties) {
        SegmentPrefetchInfo segmentPrefetchInfo = this.prefetchingInfoCache.get(prefetchId);

        // If we know that the reader has a last read result that is either truncated, end of Segment, or at tail, there
        // is nothing to prefetch.
        if (segmentPrefetchInfo == null || !segmentPrefetchInfo.isSequentialRead()) {
            throw new CompletionException(new UnableToPrefetchException("Unable to prefetch data."));
        // We need to check if we have enough prefetched data for this Segment and Reader.
        } else if (segmentPrefetchInfo.isLastReadFromStorage()
                   || segmentPrefetchInfo.needsToRefillPrefetchedData(this.prefetchReadLength, this.consumedPrefetchedDataThreshold)) {
            return segmentProperties;
        }

        // We still have prefetched data for this reader, so do nothing.
        throw new CompletionException(new PrefetchNotNeededException());
    }

    @VisibleForTesting
    ImmutablePair<Long, Integer> calculatePrefetchReadLength(UUID prefetchId, SegmentProperties segmentProperties) {
        SegmentPrefetchInfo entry = this.prefetchingInfoCache.get(prefetchId);
        if (entry == null || segmentProperties.isDeleted()) {
            throw new CompletionException(new UnableToPrefetchException("Prefetch entry not found or Segment deleted when attempting to calculate prefetch length."));
        }
        long lastUserReadBytePosition = entry.getLastReadOffset() + entry.getLastReadLength();
        long maxAlreadyReadOffset = Math.max(lastUserReadBytePosition, entry.getPrefetchEndOffset());
        long maxAvailableDataToPrefetch = Math.max(0, segmentProperties.getLength() - maxAlreadyReadOffset);
        int prefetchReadLength = MathHelpers.minMax(this.prefetchReadLength, 0, (int) maxAvailableDataToPrefetch);
        if (prefetchReadLength <= 0) {
            log.warn("Negative prefetch read length calculated for segment {}, with stored info {}.", segmentProperties, entry);
            throw new CompletionException(new UnableToPrefetchException("Not possible to issue prefetch read."));
        }
        return new ImmutablePair<>(maxAlreadyReadOffset, prefetchReadLength);
    }

    /**
     * Does actual reading of the prefetch read, so it can be cached in memory for subsequent user reads.
     *
     * @param prefetchReadResult {@link ReadResult} of the prefetch read.
     * @return ImmutablePair consisting of the read length and whether we should continue prefetching for this Segment and reader.
     */
    @VisibleForTesting
    ImmutablePair<Integer, Boolean> prefetchReadCallback(ReadResult prefetchReadResult) {
        int prefetchedDataLength = 0;
        boolean canPrefetch = true;
        try {
            while (prefetchReadResult.hasNext()) {
                ReadResultEntry entry = prefetchReadResult.next();
                if (entry.getType() == Storage) {
                    // By reading the entry contents, data should be cached to improve latency of the next user reads.
                    BufferView data = entry.getContent().get(timeout.getSeconds(), TimeUnit.SECONDS);
                    prefetchedDataLength += data.getLength();
                    data.release();
                } else {
                    // Stopped reading data from Storage, so do not continue prefetching.
                    log.debug("Interrupting read prefetch after reading {} bytes (Cache={}, Truncated={}, EndOfSegment={}, AtTail={}, Storage={}).",
                            prefetchedDataLength, entry.getType() == Cache, entry.getType() == Truncated, entry.getType() == EndOfStreamSegment,
                            entry.getType() == Future, entry.getType() == Storage);
                    canPrefetch = false;
                    break;
                }
            }
        } catch (Exception ex) {
            log.error("Error reading prefetched data in callback.", ex);
        }
        return new ImmutablePair<>(prefetchedDataLength, canPrefetch);
    }

    //region AutoCloseable Implementation

    @Override
    public void close() throws Exception {
        this.readPrefetchProcessor.close();
        this.prefetchingInfoCache.cleanUp();
        this.readPrefetchMetrics.close();
    }

    //endregion

    //region Helper Classes

    /**
     * Encapsulates the information about the current state of reads and prefetched data for a given Segment.
     */
    @Getter
    @Setter
    @ToString
    @VisibleForTesting
    static class SegmentPrefetchInfo {
        // Info about progress of reader.
        protected volatile long lastReadOffset;
        protected volatile int lastReadLength;
        private volatile boolean lastReadFromStorage;
        private volatile boolean sequentialRead;

        // Prefetching info for this entry.
        private final int maxPrefetchReadLength;
        private volatile long prefetchStartOffset;
        private volatile long prefetchEndOffset;
        private volatile long prefetchedDataLength;

        SegmentPrefetchInfo(long lastReadOffset, int lastReadLength, boolean fromStorage, int maxPrefetchReadLength) {
            this.lastReadOffset = lastReadOffset;
            this.lastReadLength = lastReadLength;
            this.lastReadFromStorage = fromStorage;
            this.maxPrefetchReadLength = maxPrefetchReadLength;
            this.sequentialRead = true; // Let's be optimistic by default.
            this.prefetchStartOffset = 0;
            this.prefetchEndOffset = 0;
            this.prefetchedDataLength = 0;
        }

        /**
         * Updates the prefetching-related information for this entry.
         *
         * @param readSegmentStartOffset Start offset for last user read on this Segment.
         * @param maxResultLength Max length of last user read on this Segment.
         * @param fromStorage Whether the last user read comes from Storage or not.
         */
        void updateInfoFromUserRead(long readSegmentStartOffset, int maxResultLength, boolean fromStorage) {
            this.sequentialRead = checkSequentialRead(readSegmentStartOffset);
            this.lastReadOffset = readSegmentStartOffset;
            this.lastReadLength = maxResultLength;
            this.lastReadFromStorage = fromStorage;
            this.prefetchedDataLength = 0;
            if (this.sequentialRead) {
                this.prefetchedDataLength = Math.max(this.prefetchEndOffset - (this.lastReadOffset + this.lastReadLength), 0);
            }
            Preconditions.checkState(this.prefetchedDataLength >= 0, "Prefetch data length cannot be negative.");
        }

        /**
         * Whether we are running out of prefetched data for this reader and Segment.
         *
         * @param maxPrefetchReadSize Max prefetch read size.
         * @param consumedPrefetchDataThreshold Fraction of consumed prefetch read size beyond which we can trigger another
         *                                      prefetch read.
         * @return Whether we need to issue another read to refill the prefetched data for this reader and Segment.
         */
        boolean needsToRefillPrefetchedData(int maxPrefetchReadSize, double consumedPrefetchDataThreshold) {
            return this.prefetchedDataLength <= maxPrefetchReadSize * (1 - consumedPrefetchDataThreshold);
        }

        /**
         * Returns whether the last read follows the expected pattern of a sequential read or not. If we detect that the
         * last read does not follow the expected pattern, further prefetch reads may not be issued.
         *
         * @return Whether the last read from the reader and Segment is sequential.
         */
        boolean checkSequentialRead(long newReadStreamSegmentStartOffset) {
            return newReadStreamSegmentStartOffset - this.lastReadOffset >= 0
                    && newReadStreamSegmentStartOffset - this.lastReadOffset < this.maxPrefetchReadLength;
        }
    }

    /**
     * Exception to indicate that prefetching is not possible at this time.
     */
    static class UnableToPrefetchException extends Throwable {
        public UnableToPrefetchException(String d) { //FIXME
        }
    }

    /**
     * Exception to indicate that prefetching is not needed at this time.
     */
    static class PrefetchNotNeededException extends Throwable {
    }

    //endregion
}


