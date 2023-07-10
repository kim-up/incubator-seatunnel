/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.seatunnel.http.service.impl;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.core.starter.seatunnel.http.client.ClientInstance;
import org.apache.seatunnel.core.starter.seatunnel.http.request.JobSubmitRequest;
import org.apache.seatunnel.core.starter.seatunnel.http.response.JobSubmitResponse;
import org.apache.seatunnel.core.starter.seatunnel.http.service.JobService;
import org.apache.seatunnel.core.starter.seatunnel.http.utils.HostAddressUtil;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;

import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Base64;

@Service
@Slf4j
public class JobServiceImpl implements JobService {

    private static String JOB_CONFIG_DIR = "jobConfig";
    private static String FILE_EXTENSION = ".conf";

    @Override
    public JobSubmitResponse submit(final JobSubmitRequest request) throws Exception {
        // write into config file
        String jobNameBase64 = Base64.getEncoder().encodeToString(request.getJobName().getBytes());
        String configBasePath =
                Common.appRootDir().toString() + File.separator + JOB_CONFIG_DIR + File.separator;
        File confDir = new File(configBasePath + jobNameBase64);
        if (!confDir.exists()) {
            confDir.mkdirs();
        }
        File file =
                new File(
                        configBasePath
                                + jobNameBase64
                                + File.separator
                                + System.currentTimeMillis()
                                + FILE_EXTENSION);
        if (!file.exists()) {
            file.createNewFile();
        }
        try (FileOutputStream ous = new FileOutputStream(file)) {
            ous.write(request.getJobConfig().getBytes());
        }
        // submit job
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(request.getJobName());
        JobExecutionEnvironment executionContext =
                ClientInstance.getInstance()
                        .createExecutionContext(file.getPath(), new JobConfig());

        ClientJobProxy clientJobProxy = executionContext.execute();
        JobSubmitResponse response =
                new JobSubmitResponse(
                        clientJobProxy.getJobId(),
                        file.getPath(),
                        HostAddressUtil.getHostAddress());
        return response;
    }

    @Override
    public String jobMetric(final Long jobId) {
        return jobClient().getJobMetrics(jobId);
    }

    @Override
    public JobDAGInfo jobInfo(final Long jobId) {
        return jobClient().getJobInfo(jobId);
    }

    @Override
    public String listJobStatus(boolean format) {
        return jobClient().listJobStatus(format);
    }

    private JobClient jobClient() {
        return ClientInstance.getInstance().getJobClient();
    }
}
