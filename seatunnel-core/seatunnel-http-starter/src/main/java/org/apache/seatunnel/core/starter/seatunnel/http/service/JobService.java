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

package org.apache.seatunnel.core.starter.seatunnel.http.service;

import org.apache.seatunnel.core.starter.seatunnel.http.request.JobSubmitRequest;
import org.apache.seatunnel.core.starter.seatunnel.http.response.JobSubmitResponse;
import org.apache.seatunnel.engine.client.job.JobMetricsRunner;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;

public interface JobService {

    JobSubmitResponse submit(JobSubmitRequest request) throws Exception;

    String getRunningJobMetrics();

    String getJobMetrics(Long jobId);

    String getJobStatus(Long jobId);

    String getJobDetailStatus(Long jobId);

    Boolean savePointJob(Long jobId);

    Boolean cancelJob(Long jobId);

    JobDAGInfo getJobInfo(Long jobId);

    String listJobStatus(boolean format);

    JobMetricsRunner.JobMetricsSummary getJobMetricsSummary(Long jobId);
}
