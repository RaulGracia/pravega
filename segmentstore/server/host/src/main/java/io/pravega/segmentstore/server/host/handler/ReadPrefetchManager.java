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
import io.pravega.common.MathHelpers;
import io.pravega.common.concurrent.MultiKeySequentialProcessor;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.SimpleCache;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
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
 * does three main tasks:
 * - Keep track of current Segments being read as well as their prefetched data, if any.
 * - Issue asynchronous reads for Segments that require prefetching data.
 *
 * An asynchronous prefetch read will be issues for a Segment only if the following conditions apply:
 * - Reads are related to a regular Segment (e.g., not a Table Segment).
 * - The amount of already prefetched data for a Segment is X% lower than
 */
@Slf4j
public class ReadPrefetchManager implements AutoCloseable {
    //region Members

    private final Supplier<Boolean> canPrefetch;
    @Getter(AccessLevel.PACKAGE)
    @VisibleForTesting
    private final SimpleCache<UUID, SegmentPrefetchInfo> prefetchingInfoCache;
    private final int prefetchReadLength;
    private final double consumedPrefetchedDataThreshold;
    private final MultiKeySequentialProcessor<UUID> readPrefetchProcessor;
    private final ExecutorService executorService;
    private final Duration timeout = Duration.ofSeconds(10);

    public ReadPrefetchManager(Supplier<Boolean> canPrefetch, ReadPrefetchManagerConfig config, ExecutorService executorService) {
        this.canPrefetch = canPrefetch;
        this.prefetchReadLength = config.getPrefetchReadLength();
        this.consumedPrefetchedDataThreshold = config.getConsumedPrefetchedDataThreshold();
        this.prefetchingInfoCache = new SimpleCache<>(config.getTrackedEntriesMaxCount(), config.getTrackedEntriesEvictionTimeSeconds(),
                (key, value) -> log.debug("Evicting prefetch key: {}, value: {}", key, value));
        this.readPrefetchProcessor = new MultiKeySequentialProcessor<>(executorService);
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
        // Identifier that combines the Segment and the reader (i.e., connection) to perform prefetching.
        UUID prefetchId = createPrefetchId(request.getSegment(), request.getRequestId());
        // Queue task to add the info to the prefetchingInfoCache.
        this.readPrefetchProcessor.add(ImmutableList.of(prefetchId), () -> CompletableFuture.supplyAsync(() -> {
            SegmentPrefetchInfo segmentPrefetchInfo = this.prefetchingInfoCache.get(prefetchId);
            if (segmentPrefetchInfo == null) {
                // Initialize the entry for this reader and Segment.
                this.prefetchingInfoCache.put(prefetchId, new SegmentPrefetchInfo(result.getStreamSegmentStartOffset(),
                        result.getMaxResultLength(), fromStorage));
            } else {
                segmentPrefetchInfo.updateInfoFromRegularRead(result.getStreamSegmentStartOffset(), result.getMaxResultLength(), fromStorage);
            }
            return null;
        }, this.executorService));
    }

    /**
     *
     *
     * @param segmentStore
     * @param segment
     * @param request
     */
    void tryPrefetchData(@NonNull StreamSegmentStore segmentStore, @NonNull String segment, @NonNull WireCommands.ReadSegment request) {
        // Cache is full, so we are not doing performing prefetching.
        if (!canPrefetch.get()) {
            log.warn("Not allowed to trigger prefetch reads (system may be under pressure).");
            return;
        }

        // Now, we need to check if we can issue a prefetch read request for this Segment and Reader.
        final UUID prefetchId = createPrefetchId(segment, request.getRequestId());
        this.readPrefetchProcessor.add(ImmutableList.of(prefetchId), () -> buildPrefetchingReadFuture(segmentStore, segment, prefetchId));
    }

    @VisibleForTesting
    CompletableFuture<Void> buildPrefetchingReadFuture(@NonNull StreamSegmentStore segmentStore, @NonNull String segment, UUID prefetchId) {
        return segmentStore.getStreamSegmentInfo(segment, timeout)
                .thenApply(segmentProperties -> checkPrefetchPreconditions(prefetchId, segmentProperties))
                .thenApply(segmentProperties -> calculatePrefetchReadLength(prefetchId, segmentProperties))
                .thenCompose(offsetAndLength -> segmentStore.read(segment, offsetAndLength.getLeft(), offsetAndLength.getRight(), timeout))
                .handle((prefetchReadResult, ex) -> {
                    if (ex != null) {
                        if (ex instanceof UnableToPrefetchException || ex instanceof PrefetchNotNeededException) {
                            log.debug("Not prefetching this time.");
                        } else {
                            log.error("Problem while performing a prefetch read.", ex);
                        }
                    } else {
                        ImmutablePair<Integer, Boolean> prefetchResultInfo = prefetchReadCallback(prefetchReadResult);
                        // Update the prefetching information for this entry based on the read result.
                        SegmentPrefetchInfo segmentPrefetchInfo = this.prefetchingInfoCache.get(prefetchId);
                        segmentPrefetchInfo.setPrefetchedDataLength(prefetchResultInfo.getLeft());
                        segmentPrefetchInfo.setPrefetchStartOffset(prefetchReadResult.getStreamSegmentStartOffset());
                        segmentPrefetchInfo.setPrefetchEndOffset(prefetchReadResult.getStreamSegmentStartOffset() + prefetchResultInfo.getLeft());
                        segmentPrefetchInfo.setLastReadFromStorage(prefetchResultInfo.getRight());
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
    private UUID createPrefetchId(String segmentName, long readerId) {
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
    private SegmentProperties checkPrefetchPreconditions(UUID prefetchId, SegmentProperties segmentProperties) {
        SegmentPrefetchInfo segmentPrefetchInfo = this.prefetchingInfoCache.get(prefetchId);

        // If we know that the reader has a last read result that is either truncated, end of Segment, or at tail, there
        // is nothing to prefetch.
        if (segmentPrefetchInfo == null || !segmentPrefetchInfo.isLastReadFromStorage()) {
            throw new CompletionException(new UnableToPrefetchException());
        // It may be possible to prefetch data for this reader. We need to check if the read hit a cache miss or if we
        // have already prefetched data for it and enough has been consumed.
        } else if (segmentPrefetchInfo.shouldPrefetchAgain(this.prefetchReadLength, this.consumedPrefetchedDataThreshold)) {
            return segmentProperties;
        }

        // We still have prefetched data for this reader, so do nothing.
        throw new CompletionException(new PrefetchNotNeededException());
    }

    private ImmutablePair<Integer, Boolean> prefetchReadCallback(ReadResult prefetchReadResult) {
        int prefetchedDataLength = 0;
        boolean canPrefetch = true;
        try {
            while (prefetchReadResult.hasNext()) {
                ReadResultEntry entry = prefetchReadResult.next();
                if (entry.getType() == Storage) {
                    // By reading the entry contents, data should be cached to improve latency of the next read.
                    BufferView data = entry.getContent().get(timeout.getSeconds(), TimeUnit.SECONDS);
                    prefetchedDataLength += data.getLength();
                    data.release();
                } else {
                    // Stopped reading data from Storage, so do not continue prefetching.
                    log.debug("Interrupting read prefetch (Cache={}, Truncated={}, EndOfSegment={}, AtTail={}, Storage={})", entry.getType() == Cache,
                            entry.getType() == Truncated, entry.getType() == EndOfStreamSegment, entry.getType() == Future, entry.getType() == Storage);
                    canPrefetch = false;
                }
            }
        } catch (Exception ex) {
            log.error("Error reading prefetched data in callback.", ex);
        }
        return new ImmutablePair<>(prefetchedDataLength, canPrefetch);
    }

    private ImmutablePair<Long, Integer> calculatePrefetchReadLength(UUID prefetchId, SegmentProperties segmentProperties) {
        long previousPrefetchEndOffset = this.prefetchingInfoCache.get(prefetchId).getPrefetchEndOffset();
        long maxAvailableDataToPrefetch = segmentProperties.getLength() - previousPrefetchEndOffset;
        Preconditions.checkState(maxAvailableDataToPrefetch > 0, "Max available data to prefetch cannot be negative.");
        int prefetchReadLength = MathHelpers.minMax(this.prefetchReadLength, 0, (int) maxAvailableDataToPrefetch);
        return new ImmutablePair<>(previousPrefetchEndOffset, prefetchReadLength);
    }

    //region AutoCloseable Implementation

    @Override
    public void close() throws Exception {
        this.readPrefetchProcessor.close();
        this.prefetchingInfoCache.cleanUp();
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

        // Prefetching info for this entry.
        private volatile long prefetchStartOffset;
        private volatile long prefetchEndOffset;
        private volatile long prefetchedDataLength;

        SegmentPrefetchInfo(long lastReadOffset, int lastReadLength, boolean fromStorage) {
            this.lastReadOffset = lastReadOffset;
            this.lastReadLength = lastReadLength;
            this.lastReadFromStorage = fromStorage;
            this.prefetchStartOffset = 0;
            this.prefetchEndOffset = 0;
            this.prefetchedDataLength = 0;
        }

        void updateInfoFromRegularRead(long streamSegmentStartOffset, int maxResultLength, boolean fromStorage) {
            this.lastReadOffset = streamSegmentStartOffset;
            this.lastReadLength = maxResultLength;
            this.lastReadFromStorage = fromStorage;
            if (isSequentialRead()) {
                // TODO: Is this correct?
                this.prefetchedDataLength = this.prefetchEndOffset - (this.lastReadOffset + this.lastReadLength);
            }
        }

        /**
         * Indicates whether, with the current prefetching state of this reader and Segment pair, it is correct to issue
         * a new prefetch read. Specifically, it checks that the last read for the prefetchId continues with the expected
         * sequential progress (i.e., reader is not doing random reads)
         *
         * @param maxPrefetchReadSize Max length of data to be prefetched on a single read.
         * @param consumedPrefetchDataThreshold Fraction of maxPrefetchReadSize that needs to be consumed before
         *                                      triggering the next prefetch read.
         * @return Whether the system needs to trigger another prefetch read for this pair of Reader and Segment.
         */
        boolean shouldPrefetchAgain(int maxPrefetchReadSize, double consumedPrefetchDataThreshold) {
            return isSequentialRead() && needsToRefillPrefetchedData(maxPrefetchReadSize, consumedPrefetchDataThreshold);
        }

        /**
         * Returns whether the last read follows the expected pattern of a sequential read or not. If we detect that the
         * last read does not follow the expected pattern, further prefetch reads may not be issued.
         *
         * @return Whether the last read from the reader and Segment is sequential.
         */
        boolean isSequentialRead() {
            return this.lastReadOffset + this.lastReadLength >= this.prefetchStartOffset
                    && this.lastReadOffset + this.lastReadLength < this.prefetchStartOffset + this.prefetchEndOffset;
        }

        /**
         * Whether we are running out of prefetched data  this reader and Segment and we should prefetch more data.
         *
         * @param maxPrefetchReadSize Max prefetch read size.
         * @param consumedPrefetchDataThreshold Fraction of consumed prefetch read size beyond which we can trigger another
         *                                      prefetch read.
         * @return Whether we need to issue another read to refill the prefetched data for this reader and Segment.
         */
        boolean needsToRefillPrefetchedData(int maxPrefetchReadSize, double consumedPrefetchDataThreshold) {
            return this.prefetchedDataLength <= maxPrefetchReadSize * (1 - consumedPrefetchDataThreshold);
        }
    }

    /**
     * Exception to indicate that prefetching is not possible at this time.
     */
    private static class UnableToPrefetchException extends Throwable {
    }

    /**
     * Exception to indicate that prefetching is not needed at this time.
     */
    private static class PrefetchNotNeededException extends Throwable {
    }

    //endregion
}


