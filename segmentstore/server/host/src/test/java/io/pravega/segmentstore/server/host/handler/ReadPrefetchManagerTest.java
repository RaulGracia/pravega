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

import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.reading.CompletableReadResultEntry;
import io.pravega.segmentstore.server.reading.StreamSegmentReadResult;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for ReadPrefetchManager class.
 */
public class ReadPrefetchManagerTest extends ThreadPooledTestSuite {

    /**
     * Tests basic POJO behavior of SegmentPrefetchInfo class.
     */
    @Test
    public void testSegmentPrefetchInfo() {
        ReadPrefetchManager.SegmentPrefetchInfo segmentPrefetchInfo = new ReadPrefetchManager.SegmentPrefetchInfo(1, 2, false, 4 * 1024 * 1024);
        Assert.assertFalse(segmentPrefetchInfo.isLastReadFromStorage());
        Assert.assertEquals(1, segmentPrefetchInfo.getLastReadOffset());
        Assert.assertEquals(2, segmentPrefetchInfo.getLastReadLength());
        segmentPrefetchInfo.updateInfoFromUserRead(10, 20, true);
        Assert.assertTrue(segmentPrefetchInfo.isLastReadFromStorage());
        Assert.assertEquals(10, segmentPrefetchInfo.getLastReadOffset());
        Assert.assertEquals(20, segmentPrefetchInfo.getLastReadLength());
        Assert.assertNotNull(segmentPrefetchInfo.toString());
    }

    /**
     * Test preconditions that dictate when to trigger a prefetch read.
     */
    @Test
    public void testSegmentPrefetchInfoTriggerPrefetchReadPreconditions() {
        long startReadOffset = 10 * 1024 * 1024;
        int readLength = 20;
        int prefetchReadLength = 4 * 1024 * 1024;
        ReadPrefetchManager.SegmentPrefetchInfo segmentPrefetchInfo = new ReadPrefetchManager.SegmentPrefetchInfo(startReadOffset, readLength, false, prefetchReadLength);
        // By default, we are optimistic and assume that Stream reads are sequential (when we don't have observations to decide).
        Assert.assertTrue(segmentPrefetchInfo.isSequentialRead());

        // If offsets for the last read are way before or after the range of prefetching data, it may mean that the
        // reader is doing random reads.
        Assert.assertFalse(segmentPrefetchInfo.checkSequentialRead(1));
        Assert.assertFalse(segmentPrefetchInfo.checkSequentialRead(10 * startReadOffset));

        // Reads within lastReadOffset to lastReadOffset + prefetchReadLength are considered sequential.
        Assert.assertTrue(segmentPrefetchInfo.checkSequentialRead(startReadOffset));
        Assert.assertTrue(segmentPrefetchInfo.checkSequentialRead(startReadOffset + prefetchReadLength - 1));

        // After updating segmentPrefetchInfo with a non-sequential read, isSequentialRead() should evaluate to false.
        segmentPrefetchInfo.updateInfoFromUserRead(1024 * 1024, readLength, true);
        Assert.assertFalse(segmentPrefetchInfo.isSequentialRead());

        // However, if the last read is within the expected bounds of a sequential read, the method should output true.
        segmentPrefetchInfo.updateInfoFromUserRead(2 * 1024 * 1024, readLength, true);
        Assert.assertTrue(segmentPrefetchInfo.isSequentialRead());

        // Now, test whether there is enough prefetched data or not for this reader.
        // Case 1: prefetchStartOffset is behind the last read from the user. In this case, we need to prefetch.
        segmentPrefetchInfo.updateInfoFromUserRead(startReadOffset, readLength, false);
        segmentPrefetchInfo.setPrefetchStartOffset(startReadOffset);
        Assert.assertTrue(segmentPrefetchInfo.needsToRefillPrefetchedData());
        // Case 2: prefetchStartOffset is equal to last user read, we need to prefetch.
        segmentPrefetchInfo.setPrefetchStartOffset(startReadOffset + readLength);
        Assert.assertTrue(segmentPrefetchInfo.needsToRefillPrefetchedData());
        // Case 3: prefetchStartOffset is ahead the last user read, do not prefetch.
        segmentPrefetchInfo.setPrefetchStartOffset(startReadOffset + readLength + 1);
        Assert.assertFalse(segmentPrefetchInfo.needsToRefillPrefetchedData());
        // Case 4: Even if we should prefetch considering the positions of reader and prefetch offsets, the last prefetch
        // read notifies us that either the prefetched data is already in cache or we cannot read further this segment.
        // In this case, we do not prefetch any more data.
        segmentPrefetchInfo.setPrefetchStartOffset(startReadOffset + 3 * 1024 * 1024);
        segmentPrefetchInfo.setContinuePrefetching(false);
        Assert.assertFalse(segmentPrefetchInfo.needsToRefillPrefetchedData());
    }

    /**
     * Basic tests of ReadPrefetchManager constructor.
     */
    @Test
    public void testReadPrefetchManagerConstructor() throws Exception {
        AtomicBoolean isCacheFull = new AtomicBoolean(false);
        Supplier<Boolean> canPrefetchSupplier = isCacheFull::get;
        ReadPrefetchManagerConfig readPrefetchManagerConfig = ReadPrefetchManagerConfig.builder()
                .with(ReadPrefetchManagerConfig.READ_PREFETCH_READ_LENGTH, 123)
                .with(ReadPrefetchManagerConfig.READ_PREFETCH_TRACKED_ENTRY_MAX_COUNT, 100)
                .with(ReadPrefetchManagerConfig.READ_PREFETCH_TRACKED_ENTRY_EVICTION_TIME_SECONDS, 10)
                .build();
        @Cleanup
        ReadPrefetchManager readPrefetchManager = new ReadPrefetchManager(canPrefetchSupplier, readPrefetchManagerConfig, this.executorService());
        Assert.assertEquals(123, readPrefetchManager.getPrefetchReadLength());
        Assert.assertEquals(100, readPrefetchManager.getPrefetchingInfoCache().getMaxSize());
        Assert.assertFalse(readPrefetchManager.getCacheFull().get());
        isCacheFull.set(true);
        Assert.assertTrue(readPrefetchManager.getCacheFull().get());
        WireCommands.ReadSegment request = new WireCommands.ReadSegment("segment", 0, 100, "", 0);
        // Check that we cannot prefetch data if isCacheFull returns true.
        Assert.assertNull(readPrefetchManager.tryPrefetchData(Mockito.mock(StreamSegmentStore.class), "segment", request).join());
    }

    /**
     * Test collection of information from user reads.
     */
    @Test
    public void testCollectInfoFromUser() throws Exception {
        Supplier<Boolean> canPrefetchSupplier = () -> false;
        ReadPrefetchManagerConfig readPrefetchManagerConfig = ReadPrefetchManagerConfig.builder().build();
        @Cleanup
        ReadPrefetchManager readPrefetchManager = new ReadPrefetchManager(canPrefetchSupplier, readPrefetchManagerConfig, this.executorService());
        // First, create a new user read to collect data from that comes from Storage and check that the entry exists in cache.
        WireCommands.ReadSegment request = new WireCommands.ReadSegment("segment", 0, 100, "", 0);
        ReadResult result = new StreamSegmentReadResult(0, 50, new MockNextEntrySupplier(), "");
        readPrefetchManager.collectInfoFromUserRead(request, result, true);
        ReadPrefetchManager.SegmentPrefetchInfo prefetchEntry = readPrefetchManager.getPrefetchingInfoCache().get(readPrefetchManager.createPrefetchId("segment", 0));
        Assert.assertTrue(prefetchEntry.isLastReadFromStorage());
        Assert.assertEquals(0L, prefetchEntry.getLastReadOffset());
        Assert.assertEquals(50L, prefetchEntry.getLastReadLength());

        // Now, let's update the same entry with other values and check that they are reflected in the cache entry.
        result = new StreamSegmentReadResult(50, 100, new MockNextEntrySupplier(), "");
        readPrefetchManager.collectInfoFromUserRead(request, result, false);
        prefetchEntry = readPrefetchManager.getPrefetchingInfoCache().get(readPrefetchManager.createPrefetchId("segment", 0));
        Assert.assertFalse(prefetchEntry.isLastReadFromStorage());
        Assert.assertEquals(50L, prefetchEntry.getLastReadOffset());
        Assert.assertEquals(100L, prefetchEntry.getLastReadLength());
    }

    /**
     * Test to check prefetch read preconditions.
     */
    @Test
    public void testCheckPrefetchPreconditions() throws Exception {
        long readOffset = 5 * 1024 * 1024;
        long segmentLength = 100 * 1024 * 1024;
        int prefetchReadLength = 4 * 1024 * 1024;
        Supplier<Boolean> canPrefetchSupplier = () -> false;
        ReadPrefetchManagerConfig readPrefetchManagerConfig = ReadPrefetchManagerConfig.builder().build();
        @Cleanup
        ReadPrefetchManager readPrefetchManager = new ReadPrefetchManager(canPrefetchSupplier, readPrefetchManagerConfig, this.executorService());
        UUID prefetchId = readPrefetchManager.createPrefetchId("segment", 0);
        SegmentProperties segmentProperties = StreamSegmentInformation.builder().name("segment").length(segmentLength).startOffset(0).storageLength(segmentLength).build();
        // Prefetch tracker cache is empty, so it should throw.
        AssertExtensions.assertThrows(ReadPrefetchManager.UnableToPrefetchException.class,
                () -> readPrefetchManager.checkPrefetchPreconditions(prefetchId, segmentProperties));

        // Let's check that for prefetching data, user should be sending sequential reads.
        WireCommands.ReadSegment request = new WireCommands.ReadSegment("segment", 0, 100, "", 0);
        ReadResult result = new StreamSegmentReadResult(0, 50, new MockNextEntrySupplier(), "");
        readPrefetchManager.collectInfoFromUserRead(request, result, false);
        request = new WireCommands.ReadSegment("segment", readOffset, 100, "", 0);
        result = new StreamSegmentReadResult(readOffset, 50, new MockNextEntrySupplier(), "");
        readPrefetchManager.collectInfoFromUserRead(request, result, true);
        AssertExtensions.assertThrows(ReadPrefetchManager.UnableToPrefetchException.class,
                () -> readPrefetchManager.checkPrefetchPreconditions(prefetchId, segmentProperties));

        // Check that if the last user request comes from storage and is sequential, we issue a prefetch read.
        request = new WireCommands.ReadSegment("segment", readOffset + 1024 * 1024, 100, "", 0);
        result = new StreamSegmentReadResult(readOffset + 1024 * 1024, 50, new MockNextEntrySupplier(), "");
        readPrefetchManager.collectInfoFromUserRead(request, result, true);
        Assert.assertNotNull(readPrefetchManager.checkPrefetchPreconditions(prefetchId, segmentProperties));

        // If the last user read is sequential but does not come from storage, we may need to check if we have enough
        // data prefetched for this Segment and reader.
        readPrefetchManager.collectInfoFromUserRead(request, result, false);
        Assert.assertNotNull(readPrefetchManager.checkPrefetchPreconditions(prefetchId, segmentProperties));

        // If preconditions are satisfied, but we still have enough data prefetched, throw PrefetchNotNeededException.
        ReadPrefetchManager.SegmentPrefetchInfo entry = readPrefetchManager.getPrefetchingInfoCache().get(prefetchId);
        entry.setPrefetchStartOffset(readOffset + 1024 * 1024 + 51);
        entry.setPrefetchEndOffset(readOffset + 1024 * 1024 + prefetchReadLength);
        AssertExtensions.assertThrows(ReadPrefetchManager.PrefetchNotNeededException.class,
                () -> readPrefetchManager.checkPrefetchPreconditions(prefetchId, segmentProperties));
    }

    /**
     * Test calculation of read prefetch offsets.
     */
    @Test
    public void testCalculatePrefetchLength() throws Exception {
        Supplier<Boolean> canPrefetchSupplier = () -> false;
        ReadPrefetchManagerConfig readPrefetchManagerConfig = ReadPrefetchManagerConfig.builder().build();
        @Cleanup
        ReadPrefetchManager readPrefetchManager = new ReadPrefetchManager(canPrefetchSupplier, readPrefetchManagerConfig, this.executorService());
        UUID prefetchId = readPrefetchManager.createPrefetchId("segment", 0);
        SegmentProperties segmentProperties = StreamSegmentInformation.builder().name("segment")
                .length(100).startOffset(0).storageLength(100).build();
        // If there is no prefetch cache entry (i.e., it has been concurrently evicted), throw exception.
        AssertExtensions.assertThrows(ReadPrefetchManager.UnableToPrefetchException.class,
                () -> readPrefetchManager.calculatePrefetchReadLength(prefetchId, segmentProperties));

        // Add a prefetch cache entry for this Segment and Reader and verify prefetch length calculation.
        WireCommands.ReadSegment request = new WireCommands.ReadSegment("segment", 0, 100, "", 0);
        ReadResult result = spy(new StreamSegmentReadResult(0, 50, new MockNextEntrySupplier(), ""));
        when(result.getConsumedLength()).thenReturn(50);
        readPrefetchManager.collectInfoFromUserRead(request, result, true);
        // The Segment has 100 bytes, and the user has already read 50, so the max data to prefetch is 50 bytes from offset 50.
        Assert.assertEquals(50, (int) readPrefetchManager.calculatePrefetchReadLength(prefetchId, segmentProperties).getRight());
        Assert.assertEquals(50, (long) readPrefetchManager.calculatePrefetchReadLength(prefetchId, segmentProperties).getLeft());

        // If the Segment has more data, we have to verify that the prefetch data length should be max.
        SegmentProperties segmentProperties2 = StreamSegmentInformation.builder().name("segment")
                .length(10 * 1024 * 1024).startOffset(0).storageLength(10 * 1024 * 1024).build();
        Assert.assertEquals(4 * 1024 * 1024, (int) readPrefetchManager.calculatePrefetchReadLength(prefetchId, segmentProperties2).getRight());
        Assert.assertEquals(50, (long) readPrefetchManager.calculatePrefetchReadLength(prefetchId, segmentProperties2).getLeft());
    }

    /**
     * Test method that populates the cache with data coming from Storage upon a prefetch read.
     */
    @Test
    public void testReadPrefetchCallback() throws Exception {
        int readLength = 100;
        Supplier<Boolean> canPrefetchSupplier = () -> true;
        ReadPrefetchManagerConfig readPrefetchManagerConfig = ReadPrefetchManagerConfig.builder().build();
        @Cleanup
        ReadPrefetchManager readPrefetchManager = new ReadPrefetchManager(canPrefetchSupplier, readPrefetchManagerConfig, this.executorService());
        ReadResult readResult = Mockito.mock(ReadResult.class);
        Mockito.when(readResult.hasNext()).thenReturn(true).thenReturn(false);
        // Handle a null ReadEntry.
        readPrefetchManager.populateCacheWithPrefetchRead(UUID.randomUUID(), readResult);

        // Now try with a single prefetch Storage read.
        Mockito.when(readResult.hasNext()).thenReturn(true).thenReturn(false);
        ReadResultEntry readResultEntry = Mockito.mock(ReadResultEntry.class);
        Mockito.when(readResultEntry.getType()).thenReturn(ReadResultEntryType.Storage);
        BufferView data = new ByteArraySegment(new byte[readLength]);
        Mockito.when(readResultEntry.getContent()).thenReturn(CompletableFuture.completedFuture(data));
        Mockito.when(readResult.next()).thenReturn(readResultEntry);
        ImmutablePair<Integer, Boolean> result = readPrefetchManager.populateCacheWithPrefetchRead(UUID.randomUUID(), readResult);
        // The prefetched data size should be equal to the data length and canPrefetch should be true, as the last read is from storage.
        Assert.assertEquals(readLength, (int) result.getLeft());
        Assert.assertTrue(result.getRight());

        // Simulated that we got a Storage read and a non-storage read after that.
        Mockito.when(readResult.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        readResultEntry = Mockito.mock(ReadResultEntry.class);
        Mockito.when(readResultEntry.getType()).thenReturn(ReadResultEntryType.Storage).thenReturn(ReadResultEntryType.Future);
        data = new ByteArraySegment(new byte[readLength]);
        Mockito.when(readResultEntry.getContent()).thenReturn(CompletableFuture.completedFuture(data));
        Mockito.when(readResult.next()).thenReturn(readResultEntry);
        result = readPrefetchManager.populateCacheWithPrefetchRead(UUID.randomUUID(), readResult);
        // The prefetched data size should be equal to the data length but canPrefetch should be false now.
        Assert.assertEquals(readLength, (int) result.getLeft());
        Assert.assertFalse(result.getRight());
    }

    /**
     * Tests to the whole method intended to generate a CompletableFuture that attempts to prefetch data if conditions are met.
     */
    @Test
    public void testTryPrefetchData() throws Exception {
        String segmentName = "segment";
        Supplier<Boolean> canPrefetchSupplier = () -> true;
        ReadPrefetchManagerConfig readPrefetchManagerConfig = ReadPrefetchManagerConfig.builder().build();
        @Cleanup
        ReadPrefetchManager readPrefetchManager = new ReadPrefetchManager(canPrefetchSupplier, readPrefetchManagerConfig, this.executorService());
        int readLength = 100;
        StreamSegmentStore store = mock(StreamSegmentStore.class);

        // First, execute a prefetch when the cache is empty and does not contain the expected entry.
        SegmentProperties segmentProperties = StreamSegmentInformation.builder().name("segment").length(100).startOffset(0).storageLength(100).build();
        when(store.getStreamSegmentInfo(anyString(), Mockito.any(Duration.class))).thenReturn(CompletableFuture.completedFuture(segmentProperties));
        WireCommands.ReadSegment request = new WireCommands.ReadSegment(segmentName, 0, 100, "", 0);
        readPrefetchManager.tryPrefetchData(store, segmentName, request).join();

        // Add a prefetch cache entry for this Segment and Reader and verify prefetch length calculation.
        ReadResult result = new StreamSegmentReadResult(0, 50, new MockNextEntrySupplier(), "");
        readPrefetchManager.collectInfoFromUserRead(request, result, true);

        // With the entry added in the prefetch cache, let's run tryPrefetchData again.
        segmentProperties = StreamSegmentInformation.builder().name("segment").length(100).startOffset(0).storageLength(100).build();
        when(store.getStreamSegmentInfo(anyString(), Mockito.any(Duration.class))).thenReturn(CompletableFuture.completedFuture(segmentProperties));
        request = new WireCommands.ReadSegment(segmentName, 0, 100, "", 0);
        ReadResult readResult = Mockito.mock(ReadResult.class);
        Mockito.when(readResult.hasNext()).thenReturn(true).thenReturn(false);
        ReadResultEntry readResultEntry = Mockito.mock(ReadResultEntry.class);
        Mockito.when(readResultEntry.getType()).thenReturn(ReadResultEntryType.Storage);
        BufferView data = new ByteArraySegment(new byte[readLength]);
        Mockito.when(readResultEntry.getContent()).thenReturn(CompletableFuture.completedFuture(data));
        Mockito.when(readResult.next()).thenReturn(readResultEntry);
        when(store.read(anyString(), anyLong(), anyInt(), Mockito.any(Duration.class))).thenReturn(CompletableFuture.completedFuture(readResult));
        readPrefetchManager.tryPrefetchData(store, segmentName, request).join();
    }

    /**
     * Test metrics collection related to read prefetching.
     */
    @Test
    public void testReadPrefetchManagerMetrics() throws Exception {
        MetricsProvider.initialize(MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .build());
        MetricsProvider.getMetricsProvider().startWithoutExporting();
        @Cleanup
        val metrics = new SegmentStoreMetrics.ReadPrefetch();
        metrics.reportPrefetchDataRead(100);
        AssertExtensions.assertEventuallyEquals(true, () -> MetricRegistryUtils.getCounter(MetricsNames.PREFETCH_READ_BYTES).count() == 100, 2000);
        metrics.reportPrefetchDataReadLatency(1000);
        AssertExtensions.assertEventuallyEquals(true, () -> MetricRegistryUtils.getTimer(MetricsNames.PREFETCH_READ_LATENCY).mean(TimeUnit.MILLISECONDS) == 1000, 2000);
        MetricsProvider.getMetricsProvider().close();
    }

    static class MockNextEntrySupplier implements StreamSegmentReadResult.NextEntrySupplier {
        @Override
        public CompletableReadResultEntry apply(Long startOffset, Integer remainingLength, Boolean makeCopy) {
            return null;
        }
    }
}
