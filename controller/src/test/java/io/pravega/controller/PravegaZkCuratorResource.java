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
package io.pravega.controller;

import io.pravega.controller.store.client.StoreClient;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.test.common.TestUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.rules.ExternalResource;
import java.util.Arrays;

@Slf4j
public class PravegaZkCuratorResource extends ExternalResource {

    private static final String LOOPBACK_ADDRESS = "127.0.0.1";

    public CuratorFramework client;

    public ZooKeeperServiceRunner zkTestServer;
    public StoreClient storeClient;
    public RetryPolicy retryPolicy;
    public int sessionTimeoutMs;
    public int connectionTimeoutMs;

    public PravegaZkCuratorResource() {
       this(new ExponentialBackoffRetry(200, 10, 5000));
    }

    public PravegaZkCuratorResource(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public PravegaZkCuratorResource(int sessionTimeoutMs, int connectionTimeoutMs, RetryPolicy retryPolicy) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.retryPolicy = retryPolicy;
    }

    public void cleanupZookeeperData() {
        Arrays.asList("/pravega", "/hostIndex", "/store", "/taskIndex", "/hostTxnIndex", "/hostRequestIndex", "/watermarks", "/buckets",
                "/txnCommitOrderer", "/lastActiveStreamSegment", "/transactions", "/completedTxnGC", "/counter").forEach(s -> {
            try {
                client.delete().deletingChildrenIfNeeded().forPath(s);
            } catch (Exception e) {
                // Do nothing.
            }
        });
    }

    @Override
    public void before() throws Exception {
        //Instantiate test ZK service
        int zkPort = TestUtils.getAvailableListenPort();
        zkTestServer = new ZooKeeperServiceRunner(zkPort, false, "", "", "");
        // Start or resume ZK.
        zkTestServer.initialize();
        zkTestServer.start();
        ZooKeeperServiceRunner.waitForServerUp(zkPort);
        log.info("ZooKeeper started.");

        String connectionString = LOOPBACK_ADDRESS + ":" + zkPort;

        //Initialize ZK client
        if (sessionTimeoutMs == 0 && connectionTimeoutMs == 0) {
            client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
        } else {
            client = CuratorFrameworkFactory.newClient(connectionString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        }
        client.start();
        client.blockUntilConnected();
        storeClient = StoreClientFactory.createZKStoreClient(client);
    }

    @Override
    @SneakyThrows
    public void after() {
        storeClient.close();
        client.close();
        zkTestServer.stop();
        zkTestServer.close();
    }
}