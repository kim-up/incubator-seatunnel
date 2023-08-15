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

package org.apache.seatunnel.connectors.seatunnel.starrocks;

import org.apache.seatunnel.connectors.seatunnel.starrocks.client.StarRocksSinkManager;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SinkConfig;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StartRocksManagerTest {

    @Test
    public void test() throws IOException {
        SinkConfig sinkConfig = new SinkConfig();
        List<String> fileNames = new ArrayList<>();
        sinkConfig.setBatchMaxBytes(1);
        sinkConfig.setBatchMaxSize(0);
        sinkConfig.setMaxRetries(1);
        List<String> nodeUrls = new ArrayList<>();
        nodeUrls.add(":8030");
        sinkConfig.setNodeUrls(nodeUrls);
        sinkConfig.setDatabase("access_ads");
        sinkConfig.setTable("ads_brand_report_seller_analysis_md_i");
        //        sinkConfig.setTable("ads_polaris_member_report_all_dd_i");
        sinkConfig.setLoadFormat(SinkConfig.StreamLoadFormat.JSON);
        sinkConfig.setUsername("root");
        sinkConfig.setPassword("");
        StarRocksSinkManager startRocksManager = new StarRocksSinkManager(sinkConfig, fileNames);
        startRocksManager.write(
                "{\"date\":\"2023-08-01\",\"act_assc_shipment_amt_old_seller\":27455.3500,\"avg_act_assc_gift_shipment_amt\":0.0000,\"act_assc_gift_shipment_amt_new_seller\":0.0000,\"act_assc_gift_shipment_rate_new_seller\":0.0000,\"avg_act_assc_gift_shipment_amt_new_seller\":0.0000,\"share_gift_seller_cnt_old_seller\":0,\"share_user_cnt\":329,\"share_user_cnt_new_seller\":27,\"act_assc_gift_shipment_amt\":0.0000,\"act_assc_gift_shipment_rate\":0.0000,\"avg_act_assc_gift_shipment_amt_old_seller\":0.0000,\"share_gift_seller_cnt_new_seller\":0,\"avg_share_user_cnt_old_seller\":1.02,\"share_seller_cnt_old_seller\":296,\"avg_act_assc_shipment_amt_old_seller\":38.9438,\"share_seller_cnt\":323,\"share_gift_seller_cnt\":0,\"act_assc_shipment_amt_new_seller\":2308.7500,\"avg_share_user_cnt_new_seller\":1.00,\"act_assc_shipment_amt\":29764.1000,\"brand_name\":\"空刻\",\"avg_share_user_cnt_cnt\":1.02,\"share_seller_cnt_new_seller\":27,\"share_user_cnt_old_seller\":302,\"brand_id\":\"168\",\"avg_act_assc_shipment_amt\":40.6613,\"share_seller_exclude_self_cnt\":110,\"share_seller_exclude_self_cnt_old_seller\":101,\"month\":\"2023-08\",\"avg_act_assc_shipment_amt_new_seller\":85.5093,\"act_assc_gift_shipment_rate_old_seller\":0.0000,\"act_assc_gift_shipment_amt_old_seller\":0.0000,\"share_seller_exclude_self_cnt_new_seller\":9}");
    }
}
