/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;

import java.util.HashMap;
import java.util.Map;

@Disabled(
        "OBS is not available in CI, if you want to run this test, please set up your own obs environment")
public class ObsFileCheckpointTest extends AbstractFileCheckPointTest {
    @BeforeAll
    public static void setup() throws CheckpointStorageException {
        Map<String, String> config = new HashMap<>();
        config.put("storage.type", "obs");
        config.put("fs.obs.access.key", "your access key id");
        config.put("fs.obs.secret.key", "your access key secret");
        config.put("fs.obs.endpoint", "obs.cn-east-3.myhuaweicloud.com");
        config.put("obs.bucket", "obs://seatunnel-test/");
        STORAGE = new HdfsStorage(config);
        initStorageData();
    }
}
