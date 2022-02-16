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

import org.junit.Assert;
import org.junit.Test;

public class ReadPrefetchManagerTest {

    @Test
    public void testSegmentPrefetchInfo() {
        ReadPrefetchManager.SegmentPrefetchInfo segmentPrefetchInfo = new ReadPrefetchManager.SegmentPrefetchInfo(1, 2, false);
        Assert.assertFalse(segmentPrefetchInfo.isLastReadFromStorage());
        Assert.assertEquals(1, segmentPrefetchInfo.getLastReadOffset());
        Assert.assertEquals(2, segmentPrefetchInfo.getLastReadLength());
        segmentPrefetchInfo.updateInfoFromRegularRead(10, 20, true);
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
        Assert.assertFalse(segmentPrefetchInfo.isSequentialRead());
        segmentPrefetchInfo.setLastReadOffset(200);
        segmentPrefetchInfo.setLastReadLength(10);
        Assert.assertFalse(segmentPrefetchInfo.isSequentialRead());
        Assert.assertFalse(segmentPrefetchInfo.shouldPrefetchAgain(40, 0.75));

        // However, if the last read is within the expected bounds of a sequential read, the method should output true.
        segmentPrefetchInfo.setLastReadOffset(40);
        segmentPrefetchInfo.setLastReadLength(10);
        Assert.assertTrue(segmentPrefetchInfo.isSequentialRead());

        // Now, test whether there is enough prefetched data or not for this reader.
        segmentPrefetchInfo.setPrefetchedDataLength(30);
        Assert.assertFalse(segmentPrefetchInfo.needsToRefillPrefetchedData(40, 0.75));
        segmentPrefetchInfo.setPrefetchedDataLength(20);
        Assert.assertFalse(segmentPrefetchInfo.needsToRefillPrefetchedData(40, 0.75));
        Assert.assertFalse(segmentPrefetchInfo.shouldPrefetchAgain(40, 0.75));
        segmentPrefetchInfo.setPrefetchedDataLength(10);
        Assert.assertTrue(segmentPrefetchInfo.needsToRefillPrefetchedData(40, 0.75));

        // When the previous both conditions are met, then shouldPrefetchAgain() should be true.
        Assert.assertTrue(segmentPrefetchInfo.shouldPrefetchAgain(40, 0.75));
    }

}
