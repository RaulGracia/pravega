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

import io.pravega.common.util.SimpleCache;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import net.jcip.annotations.GuardedBy;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;

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
@Data
@Slf4j
public class ReadPrefetchManager {

    private final Supplier<Boolean> canPrefetch;
    @GuardedBy("lock")
    private final SimpleCache<UUID, SegmentPrefetchInfo> prefetchingInfoCache = new SimpleCache<>(1000, Duration.ofMinutes(1), (a,b) -> {});
    private final Object lock = new Object();
    private final double consumedPrefetchDataThreshold = 0.75; // TODO: This needs to be configurable.
    private final Duration timeout = Duration.ofSeconds(30);

    /**
     * Collect information from external reads on a Segment. This information will help us to decide whether to issue
     * prefetch reads.
     *
     * @param request Read request sent from the client.
     * @param result Read result after performing the read.
     * @param truncated Whether the read process found the Segment to have been truncated.
     * @param endOfSegment Whether the read process reached the end of the Segment.
     * @param atTail Whether the read process detected that the Segment read is a tail read.
     * @param fromStorage Whether the read process detected that we fetched data for this Segment from Storage.
     */
    void collectInfoFromRead(@NonNull WireCommands.ReadSegment request, @NonNull ReadResult result, boolean truncated,
                             boolean endOfSegment, boolean atTail, boolean fromStorage) {
        // Identifier that combines the Segment and the reader (i.e., connection) to perform prefetching.
        UUID prefetchId = createPrefetchId(request.getSegment(), request.getRequestId());
        // Add the info to the cache.
        synchronized (this.lock) {
            SegmentPrefetchInfo segmentPrefetchInfo = prefetchingInfoCache.get(prefetchId);
            if (segmentPrefetchInfo == null) {
                prefetchingInfoCache.put(prefetchId, new SegmentPrefetchInfo(result.getStreamSegmentStartOffset(),
                        result.getMaxResultLength(), truncated, endOfSegment, atTail, fromStorage));
            } else {
                segmentPrefetchInfo.updateInfoFromRead(result.getStreamSegmentStartOffset(), result.getMaxResultLength(),
                        truncated, endOfSegment, atTail, fromStorage);
            }
        }
    }

    void tryPrefetchData(@NonNull StreamSegmentStore segmentStore, @NonNull String segment, @NonNull WireCommands.ReadSegment request) {
        // Cache is full, so we are not doing performing prefetching.
        if (!canPrefetch.get()) {
            log.warn("Not prefetching data a");
            return;
        }

        // Now, we need to check if we can issue a prefetch read request for this Segment and Reader.
        final UUID prefetchId = createPrefetchId(segment, request.getRequestId());
        segmentStore.getStreamSegmentInfo(segment, timeout)
                    .thenApply(segmentProperties -> checkPrefetchPreconditions(prefetchId, segmentProperties))
                    .thenApply(segmentProperties -> calculatePrefetchReadLength(prefetchId, segmentProperties))
                    .thenCompose(result -> segmentStore.read(segment, result.getOffset(), result.getReadLength(), timeout))
                    .handle((prefetchReadResult, ex) -> {
                        if (ex != null) {
                            log.error("Problem while performing a prefetch read.");
                        } else {
                            prefetchReadCallback(prefetchReadResult);
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
     * Segment and reader should have been fetched from storage, ii) Either there is no prefetched data for this reader
     * or the amount of prefetched data is lower than
     *
     * @param prefetchId
     * @return
     */
    private SegmentProperties checkPrefetchPreconditions(UUID prefetchId, SegmentProperties segmentProperties) {
        return segmentProperties;
    }

    private void prefetchReadCallback(ReadResult prefetchReadResult) {
        //prefetchReadResult.
    }

    private ReadOffsetAndSize calculatePrefetchReadLength(UUID prefetchId, SegmentProperties segmentProperties) {
        return new ReadOffsetAndSize(0, 0);
    }

    @AllArgsConstructor
    @Getter
    private static class ReadOffsetAndSize {
        protected long offset;
        protected int readLength;
    }

    /**
     * Encapsulates the information about the current state of reads and prefetched data for a given Segment.
     */
    @Getter
    private static class SegmentPrefetchInfo extends ReadOffsetAndSize {
        private boolean truncated;
        private boolean endOfSegment;
        private boolean atTail;
        private boolean fromStorage;

        public SegmentPrefetchInfo(long offset, int size, boolean truncated, boolean endOfSegment, boolean atTail, boolean fromStorage) {
            super(offset, size);
            this.truncated = truncated;
            this.endOfSegment = endOfSegment;
            this.atTail = atTail;
            this.fromStorage = fromStorage;
        }

        public void updateInfoFromRead(long streamSegmentStartOffset, int maxResultLength, boolean truncated, boolean endOfSegment,
                                       boolean atTail, boolean fromStorage) {
            this.offset = streamSegmentStartOffset;
            this.readLength = maxResultLength;
            this.truncated = truncated;
            this.endOfSegment = endOfSegment;
            this.atTail = atTail;
            this.fromStorage = fromStorage;
        }
    }
}


