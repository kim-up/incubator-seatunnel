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

package org.apache.seatunnel.core.starter.seatunnel.http.client;

import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;

public class ClientInstance {

    private ClientInstance() {}

    private static class InnerClass {
        private static SeaTunnelClient seaTunnelClient =
                new SeaTunnelClient(ConfigProvider.locateAndGetClientConfig());
    }

    public static SeaTunnelClient getInstance() {
        if (!InnerClass.seaTunnelClient.clientIsActive()) {
            synchronized (ClientInstance.class) {
                if (!InnerClass.seaTunnelClient.clientIsActive()) {
                    InnerClass.seaTunnelClient =
                            new SeaTunnelClient(ConfigProvider.locateAndGetClientConfig());
                }
            }
        }
        return InnerClass.seaTunnelClient;
    }
}
