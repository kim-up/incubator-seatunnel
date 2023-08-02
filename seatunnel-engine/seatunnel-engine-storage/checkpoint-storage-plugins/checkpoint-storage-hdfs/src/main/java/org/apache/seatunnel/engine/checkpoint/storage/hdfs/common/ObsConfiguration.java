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

package org.apache.seatunnel.engine.checkpoint.storage.hdfs.common;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

public class ObsConfiguration extends AbstractConfiguration {

    /** ************** OBS required keys ************** */
    public static final String OBS_BUCKET_KEY = "obs.bucket";

    /* OBS constants */
    private static final String OBS_IMPL_KEY = "fs.obs.impl";
    private static final String HDFS_OBS_IMPL = "org.apache.hadoop.fs.obs.OBSFileSystem";
    private static final String OBS_KEY = "fs.obs.";

    @Override
    public Configuration buildConfiguration(Map<String, String> config) {
        checkConfiguration(config, OBS_BUCKET_KEY);
        Configuration hadoopConf = new Configuration();
        hadoopConf.set(FS_DEFAULT_NAME_KEY, config.get(OBS_BUCKET_KEY));
        hadoopConf.set(OBS_IMPL_KEY, HDFS_OBS_IMPL);
        setExtraConfiguration(hadoopConf, config, OBS_KEY);
        return hadoopConf;
    }
}
