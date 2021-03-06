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
package io.pravega.segmentstore.contracts;

/**
 * Represents an exception that is thrown when a StreamSegment that already exists is attempted to be created again.
 */
public class StreamSegmentExistsException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the StreamSegmentExistsException.
     *
     * @param streamSegmentName The name of the Segment.
     */
    public StreamSegmentExistsException(String streamSegmentName) {
        super(streamSegmentName, "The StreamSegment exists already.");
    }

    /**
     * Creates a new instance of the StreamSegmentExistsException.
     *
     * @param streamSegmentName The name of the Segment.
     * @param cause             Actual cause of the exception.
     */
    public StreamSegmentExistsException(String streamSegmentName, Throwable cause) {
        super(streamSegmentName, "The StreamSegment exists already", cause);
    }
}