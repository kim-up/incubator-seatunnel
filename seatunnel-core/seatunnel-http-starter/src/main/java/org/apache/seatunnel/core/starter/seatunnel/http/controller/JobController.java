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

package org.apache.seatunnel.core.starter.seatunnel.http.controller;

import org.apache.seatunnel.core.starter.seatunnel.http.request.JobSubmitRequest;
import org.apache.seatunnel.core.starter.seatunnel.http.response.ApiResult;
import org.apache.seatunnel.core.starter.seatunnel.http.response.JobSubmitResponse;
import org.apache.seatunnel.core.starter.seatunnel.http.service.JobService;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/job")
public class JobController {

    @Autowired JobService jobService;

    @PostMapping("/submit")
    public ApiResult<JobSubmitResponse> submit(@RequestBody JobSubmitRequest request) {
        try {
            return ApiResult.success(jobService.submit(request));
        } catch (Exception e) {
            return ApiResult.failure(e.getMessage());
        }
    }

    @PostMapping("/restore")
    public ApiResult<JobSubmitResponse> restore(@RequestBody JobSubmitRequest request) {
        try {
            return ApiResult.success(jobService.restore(request));
        } catch (Exception e) {
            return ApiResult.failure(e.getMessage());
        }
    }

    @GetMapping("/cancel/{jobId}")
    public ApiResult<Boolean> cancelJob(@PathVariable("jobId") Long jobId) {
        return ApiResult.success(jobService.cancelJob(jobId));
    }

    @GetMapping("/savePoint/{jobId}")
    public ApiResult<Boolean> savePointJob(@PathVariable("jobId") Long jobId) {
        return ApiResult.success(jobService.savePointJob(jobId));
    }

    @GetMapping("/metric/{jobId}")
    public ApiResult<String> jobMetric(@PathVariable("jobId") Long jobId) {
        return ApiResult.success(jobService.getJobMetrics(jobId));
    }

    @GetMapping("/jobInfo/{jobId}")
    public ApiResult<JobDAGInfo> jobInfo(@PathVariable("jobId") Long jobId) {
        return ApiResult.success(jobService.getJobInfo(jobId));
    }

    @GetMapping("/jobStatus/{jobId}")
    public ApiResult<String> jobStatus(@PathVariable("jobId") Long jobId) {
        return ApiResult.success(jobService.getJobStatus(jobId));
    }

    @GetMapping("/jobDetailStatus/{jobId}")
    public ApiResult<String> jobDetailStatus(@PathVariable("jobId") Long jobId) {
        return ApiResult.success(jobService.getJobDetailStatus(jobId));
    }

    @GetMapping("/listJobStatus")
    public ApiResult<String> listJobStatus(@RequestParam("format") boolean format) {
        return ApiResult.success(jobService.listJobStatus(format));
    }

    @GetMapping("/runningJobMetrics")
    public ApiResult<String> runningJobMetrics() {
        return ApiResult.success(jobService.getRunningJobMetrics());
    }
}
