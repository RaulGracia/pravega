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

import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.reading.CompletableReadResultEntry;
import io.pravega.segmentstore.server.reading.StreamSegmentReadResult;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class ReadPrefetchManagerTest extends ThreadPooledTestSuite {

    @Test
    public void testSegmentPrefetchInfo() {
        ReadPrefetchManager.SegmentPrefetchInfo segmentPrefetchInfo = new ReadPrefetchManager.SegmentPrefetchInfo(1, 2, false);
        Assert.assertFalse(segmentPrefetchInfo.isLastReadFromStorage());
        Assert.assertEquals(1, segmentPrefetchInfo.getLastReadOffset());
        Assert.assertEquals(2, segmentPrefetchInfo.getLastReadLength());
        segmentPrefetchInfo.updateInfoFromUserRead(10, 20, true);
        Assert.assertTrue(segmentPrefetchInfo.isLastReadFromStorage());
        Assert.assertEquals(10, segmentPrefetchInfo.getLastReadOffset());
        Assert.assertEquals(20, segmentPrefetchInfo.getLastReadLength());
        Assert.assertNotNull(segmentPrefetchInfo.toString());
    }

    @Test
    public void testSegmentPrefetchInfoTriggerPrefetchReadPreconditions() {
        ReadPrefetchManager.SegmentPrefetchInfo segmentPrefetchInfo = new ReadPrefetchManager.SegmentPrefetchInfo(1, 2, false);
        // Test isSequentialRead() method. Let's assume that we have prefetched data for a Segment from ranging from
        // offsets 50 and 100. We want to test the notion of "sequential read" from a prefetching perspective.
        segmentPrefetchInfo.setPrefetchStartOffset(50);
        segmentPrefetchInfo.setPrefetchEndOffset(100);

        // If offsets for the last read are way before or after the range of prefetching data, it may mean that the
        // reader is doing random reads:
        segmentPrefetchInfo.setLastReadOffset(10);
        segmentPrefetchInfo.setLastReadLength(10);
        Assert.assertFalse(segmentPrefetchInfo.checkSequentialRead(10));
        segmentPrefetchInfo.setLastReadOffset(200);
        segmentPrefetchInfo.setLastReadLength(10);
        Assert.assertFalse(segmentPrefetchInfo.isSequentialRead());
        Assert.assertFalse(segmentPrefetchInfo.needsToRefillPrefetchedData(40, 0.75));

        // However, if the last read is within the expected bounds of a sequential read, the method should output true.
        segmentPrefetchInfo.setLastReadOffset(40);
        segmentPrefetchInfo.setLastReadLength(10);
        Assert.assertTrue(segmentPrefetchInfo.isSequentialRead());

        // Now, test whether there is enough prefetched data or not for this reader.
        segmentPrefetchInfo.setPrefetchedDataLength(30);
        Assert.assertFalse(segmentPrefetchInfo.needsToRefillPrefetchedData(40, 0.75));
        segmentPrefetchInfo.setPrefetchedDataLength(20);
        Assert.assertFalse(segmentPrefetchInfo.needsToRefillPrefetchedData(40, 0.75));
        Assert.assertFalse(segmentPrefetchInfo.needsToRefillPrefetchedData(40, 0.75));
        segmentPrefetchInfo.setPrefetchedDataLength(10);
        Assert.assertTrue(segmentPrefetchInfo.needsToRefillPrefetchedData(40, 0.75));

        // When the previous both conditions are met, then shouldPrefetchAgain() should be true.
        Assert.assertTrue(segmentPrefetchInfo.needsToRefillPrefetchedData(40, 0.75));
    }

    @Test
    public void testReadPrefetchManagerConstructor() throws Exception {
        AtomicBoolean canPrefetch = new AtomicBoolean(true);
        Supplier<Boolean> canPrefetchSupplier = canPrefetch::get;
        ReadPrefetchManagerConfig readPrefetchManagerConfig = ReadPrefetchManagerConfig.builder()
                .with(ReadPrefetchManagerConfig.PREFETCH_READ_LENGTH, 123)
                .with(ReadPrefetchManagerConfig.CONSUMED_PREFETCHED_DATA_THRESHOLD, 0.5)
                .with(ReadPrefetchManagerConfig.TRACKED_ENTRY_MAX_COUNT, 100)
                .with(ReadPrefetchManagerConfig.TRACKED_ENTRY_EVICTION_TIME_SECONDS, 10)
                .build();
        @Cleanup
        ReadPrefetchManager readPrefetchManager = new ReadPrefetchManager(canPrefetchSupplier, readPrefetchManagerConfig, this.executorService());
        Assert.assertEquals(123, readPrefetchManager.getPrefetchReadLength());
        Assert.assertEquals(0.5, readPrefetchManager.getConsumedPrefetchedDataThreshold(), 0.0);
        Assert.assertEquals(100, readPrefetchManager.getPrefetchingInfoCache().getMaxSize());
        Assert.assertTrue(readPrefetchManager.getCanPrefetch().get());
        canPrefetch.set(false);
        Assert.assertFalse(readPrefetchManager.getCanPrefetch().get());
    }

    @Test
    public void testCollectInfoFromUser() throws Exception {
        Supplier<Boolean> canPrefetchSupplier = () -> true;
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

    @Test
    public void testCheckPrefetchPreconditions() throws Exception {
        Supplier<Boolean> canPrefetchSupplier = () -> true;
        ReadPrefetchManagerConfig readPrefetchManagerConfig = ReadPrefetchManagerConfig.builder().build();
        @Cleanup
        ReadPrefetchManager readPrefetchManager = new ReadPrefetchManager(canPrefetchSupplier, readPrefetchManagerConfig, this.executorService());
        UUID prefetchId = readPrefetchManager.createPrefetchId("segment", 0);
        SegmentProperties segmentProperties = StreamSegmentInformation.builder().name("segment").length(100).startOffset(0).storageLength(100).build();
        // Prefetch tracker cache is empty, so it should throw.
        AssertExtensions.assertThrows(ReadPrefetchManager.UnableToPrefetchException.class,
                () -> readPrefetchManager.checkPrefetchPreconditions(prefetchId, segmentProperties));

        // Now collect some read info from reads, but still the as reads are not from storage, we should not prefetch.
        WireCommands.ReadSegment request = new WireCommands.ReadSegment("segment", 0, 100, "", 0);
        ReadResult result = new StreamSegmentReadResult(0, 50, new MockNextEntrySupplier(), "");
        readPrefetchManager.collectInfoFromUserRead(request, result, false);
        AssertExtensions.assertThrows(ReadPrefetchManager.UnableToPrefetchException.class,
                () -> readPrefetchManager.checkPrefetchPreconditions(prefetchId, segmentProperties));

        // Let's check that apart from being from storage, the read should be sequential.
        request = new WireCommands.ReadSegment("segment", 5 * 1024 * 1024, 100, "", 0);
        result = new StreamSegmentReadResult(5 * 1024 * 1024, 50, new MockNextEntrySupplier(), "");
        readPrefetchManager.collectInfoFromUserRead(request, result, true);
        AssertExtensions.assertThrows(ReadPrefetchManager.UnableToPrefetchException.class,
                () -> readPrefetchManager.checkPrefetchPreconditions(prefetchId, segmentProperties));

        // Check that if the request meets all the criteria, preconditions pass.
        request = new WireCommands.ReadSegment("segment", 6 * 1024 * 1024, 100, "", 0);
        result = new StreamSegmentReadResult(6 * 1024 * 1024, 50, new MockNextEntrySupplier(), "");
        readPrefetchManager.collectInfoFromUserRead(request, result, true);
        Assert.assertNotNull(readPrefetchManager.checkPrefetchPreconditions(prefetchId, segmentProperties));
    }

    @Test
    public void testCalculatePrefetchLength() throws Exception {

    }

    class MockNextEntrySupplier implements StreamSegmentReadResult.NextEntrySupplier {
        @Override
        public CompletableReadResultEntry apply(Long startOffset, Integer remainingLength, Boolean makeCopy) {
            return null;
        }
    }
}
