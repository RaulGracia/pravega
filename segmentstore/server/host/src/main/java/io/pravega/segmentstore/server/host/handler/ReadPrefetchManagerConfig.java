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

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Configuration class for {@link ReadPrefetchManager}.
 */
public class ReadPrefetchManagerConfig {

    //region Members

    public static final Property<Integer> PREFETCH_READ_LENGTH = Property.named("prefetch.read.length.bytes", 4 * 1024 * 1024);
    public static final Property<Integer> TRACKED_ENTRY_MAX_COUNT = Property.named("tracked.entry.count.max", 5000);
    public static final Property<Integer> TRACKED_ENTRY_EVICTION_TIME_SECONDS = Property.named("tracked.entry.eviction.time.seconds", 60);

    private static final String COMPONENT_CODE = "readprefetch";

    @Getter
    private final int prefetchReadLength;

    @Getter
    private final int trackedEntriesMaxCount;

    @Getter
    private final Duration trackedEntriesEvictionTimeSeconds;

    //endregion

    //region Constructor

    private ReadPrefetchManagerConfig(TypedProperties properties) throws ConfigurationException {
        this.prefetchReadLength = properties.getPositiveInt(PREFETCH_READ_LENGTH);
        this.trackedEntriesMaxCount = properties.getPositiveInt(TRACKED_ENTRY_MAX_COUNT);
        this.trackedEntriesEvictionTimeSeconds = properties.getDuration(TRACKED_ENTRY_EVICTION_TIME_SECONDS, ChronoUnit.SECONDS);
    }

    public static ConfigBuilder<ReadPrefetchManagerConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ReadPrefetchManagerConfig::new);
    }

    //endregion
}
