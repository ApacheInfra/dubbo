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
package org.apache.dubbo.registry.dubbo;

import org.apache.dubbo.common.Constants;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.RegistryService;
import org.apache.dubbo.registry.support.FailbackRegistry;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.dubbo.DubboProtocol;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.mock;

public class DubboRegistryTest {

    private static final Logger logger = LoggerFactory.getLogger(DubboRegistryTest.class);

    private DubboRegistry dubboRegistry;

    private URL registryURL;

    private URL serviceURL;

    private NotifyListener notifyListener;

    private Invoker<RegistryService> invoker;

    private RegistryService registryService;

    @Before
    public void setUp() {
        registryURL = new URL(Constants.REGISTRY_PROTOCOL, NetUtils.getLocalHost(), NetUtils.getAvailablePort())
                .addParameter(Constants.CHECK_KEY, false)
                .setServiceInterface(RegistryService.class.getName());
        serviceURL = new URL(DubboProtocol.NAME, NetUtils.getLocalHost(), NetUtils.getAvailablePort())
                .addParameter(Constants.CHECK_KEY, false)
                .setServiceInterface(RegistryService.class.getName());

        registryService = new MockDubboRegistry(registryURL);

        invoker = mock(Invoker.class);
        given(invoker.getUrl()).willReturn(serviceURL);
        given(invoker.getInterface()).willReturn(RegistryService.class);
        given(invoker.invoke(new RpcInvocation())).willReturn(null);

        dubboRegistry = new DubboRegistry(invoker, registryService);
        notifyListener = mock(NotifyListener.class);
    }

    @Test
    public void testRegister() {
        dubboRegistry.register(serviceURL);
        assertEquals(1, getRegistereds());
    }

    @Test
    public void testUnRegister() {
        assertEquals(0, getRegistereds());
        dubboRegistry.register(serviceURL);
        assertEquals(1, getRegistereds());
        dubboRegistry.unregister(serviceURL);
        assertEquals(0, getRegistereds());
    }

    @Test
    public void testSubscribe() {
        dubboRegistry.register(serviceURL);
        assertEquals(1, getRegistereds());
        dubboRegistry.subscribe(serviceURL, notifyListener);
        assertEquals(1, getSubscribeds());
        assertEquals(1, getNotifiedListeners());
    }

    @Test
    public void testUnsubscribe() {
        dubboRegistry.subscribe(serviceURL, notifyListener);
        assertEquals(1, getSubscribeds());
        assertEquals(1, getNotifiedListeners());
        dubboRegistry.unsubscribe(serviceURL, notifyListener);
        assertEquals(0, getNotifiedListeners());
    }

    private class MockDubboRegistry extends FailbackRegistry {

        private volatile boolean isAvaliable = false;

        public MockDubboRegistry(URL url) {
            super(url);
        }

        @Override
        public void doRegister(URL url) {
            logger.info("Begin to register: " + url);
            isAvaliable = true;
        }

        @Override
        public void doUnregister(URL url) {
            logger.info("Begin to ungister: " + url);
            isAvaliable = false;
        }

        @Override
        public void doSubscribe(URL url, NotifyListener listener) {
            logger.info("Begin to subscribe: " + url);
        }

        @Override
        public void doUnsubscribe(URL url, NotifyListener listener) {
            logger.info("Begin to unSubscribe: " + url);
        }

        @Override
        public boolean isAvailable() {
            return isAvaliable;
        }
    }

    private int getNotifiedListeners() {
        return dubboRegistry.getSubscribed().get(serviceURL).size();
    }

    private int getRegistereds() {
        return dubboRegistry.getRegistered().size();
    }

    private int getSubscribeds() {
        return dubboRegistry.getSubscribed().size();
    }
}
