/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dubbo.config;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class MetricsConfigTest {

    @Test
    public void testPort() throws Exception{
        MetricsConfig metrics = new MetricsConfig();
        metrics.setPort("8080");
        assertThat(metrics.getPort(), equalTo("8080"));
    }

    @Test
    public void testProtocol() throws Exception{
        MetricsConfig metrics = new MetricsConfig();
        metrics.setProtocol("protocol");
        assertThat(metrics.getProtocol(), equalTo("protocol"));
    }
}
