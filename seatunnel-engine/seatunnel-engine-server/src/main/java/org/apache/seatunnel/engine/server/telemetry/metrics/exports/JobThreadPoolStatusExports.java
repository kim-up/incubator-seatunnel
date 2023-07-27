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

package org.apache.seatunnel.engine.server.telemetry.metrics.exports;

import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.telemetry.metrics.AbstractCollector;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.ThreadPoolStatus;

import com.hazelcast.instance.impl.Node;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;

import java.util.ArrayList;
import java.util.List;

public class JobThreadPoolStatusExports extends AbstractCollector {

    private static String HELP =
            "The %s of seatunnel coordinator job's executor cached thread pool";

    public JobThreadPoolStatusExports(Node node) {
        super(node);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList();
        CoordinatorService coordinatorService = getCoordinatorService();

        List<String> labelValues = labelValues(localAddress());

        ThreadPoolStatus threadPoolStatusMetrics = coordinatorService.getThreadPoolStatusMetrics();
        List<String> labelNames = clusterLabelNames(ADDRESS);

        GaugeMetricFamily activeCount =
                new GaugeMetricFamily(
                        "job_thread_pool_activeCount",
                        String.format(HELP, "activeCount"),
                        labelNames);
        activeCount.addMetric(labelValues, threadPoolStatusMetrics.getActiveCount());
        mfs.add(activeCount);

        CounterMetricFamily completedTask =
                new CounterMetricFamily(
                        "job_thread_pool_completedTask",
                        String.format(HELP, "completedTask"),
                        labelNames);
        completedTask.addMetric(labelValues, threadPoolStatusMetrics.getCompletedTaskCount());
        mfs.add(completedTask);

        GaugeMetricFamily corePoolSize =
                new GaugeMetricFamily(
                        "job_thread_pool_corePoolSize",
                        String.format(HELP, "corePoolSize"),
                        labelNames);
        corePoolSize.addMetric(labelValues, threadPoolStatusMetrics.getCorePoolSize());
        mfs.add(corePoolSize);

        GaugeMetricFamily maximumPoolSize =
                new GaugeMetricFamily(
                        "job_thread_pool_maximumPoolSize",
                        String.format(HELP, "maximumPoolSize"),
                        labelNames);
        maximumPoolSize.addMetric(labelValues, threadPoolStatusMetrics.getMaximumPoolSize());
        mfs.add(maximumPoolSize);

        GaugeMetricFamily poolSize =
                new GaugeMetricFamily(
                        "job_thread_pool_poolSize", String.format(HELP, "poolSize"), labelNames);
        poolSize.addMetric(labelValues, threadPoolStatusMetrics.getPoolSize());
        mfs.add(poolSize);

        CounterMetricFamily taskCount =
                new CounterMetricFamily(
                        "job_thread_pool_task", String.format(HELP, "taskCount"), labelNames);
        taskCount.addMetric(labelValues, threadPoolStatusMetrics.getTaskCount());
        mfs.add(taskCount);

        return mfs;
    }
}
