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

package org.apache.dubbo.configcenter.consul;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.configcenter.ConfigChangeEvent;
import org.apache.dubbo.configcenter.ConfigurationListener;
import org.apache.dubbo.configcenter.DynamicConfiguration;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.apache.dubbo.common.Constants.CONFIG_NAMESPACE_KEY;
import static org.apache.dubbo.common.Constants.PATH_SEPARATOR;

/**
 * config center implementation for consul
 */
public class ConsulDynamicConfiguration implements DynamicConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(ConsulDynamicConfiguration.class);

    private static final int DEFAULT_PORT = 8500;
    private static final int DEFAULT_WATCH_TIMEOUT = 60 * 1000;
    private static final String WATCH_TIMEOUT = "consul-watch-timeout";

    private URL url;
    private String rootPath;
    private ConsulClient client;
    private int watchTimeout = -1;
    private ConcurrentMap<String, ConsulKVWatcher> watchers = new ConcurrentHashMap<>();
    private ConcurrentMap<String, Long> consulIndexes = new ConcurrentHashMap<>();
    private ExecutorService watcherService = newCachedThreadPool(
            new NamedThreadFactory("dubbo-consul-configuration-watcher", true));

    public ConsulDynamicConfiguration(URL url) {
        this.url = url;
        this.rootPath = PATH_SEPARATOR + url.getParameter(CONFIG_NAMESPACE_KEY, DEFAULT_GROUP) + PATH_SEPARATOR + "config";
        this.watchTimeout = buildWatchTimeout(url);
        String host = url.getHost();
        int port = url.getPort() != 0 ? url.getPort() : DEFAULT_PORT;
        client = new ConsulClient(host, port);
    }

    @Override
    public void addListener(String key, String group, ConfigurationListener listener) {
        logger.info("register listener " + listener.getClass() + " for config with key: " + key + ", group: " + group);
        ConsulKVWatcher watcher = watchers.putIfAbsent(key, new ConsulKVWatcher(key));
        if (watcher == null) {
            watcher = watchers.get(key);
            watcherService.submit(watcher);
        }
        watcher.addListener(listener);
    }

    @Override
    public void removeListener(String key, String group, ConfigurationListener listener) {
        logger.info("unregister listener " + listener.getClass() + " for config with key: " + key + ", group: " + group);
        ConsulKVWatcher watcher = watchers.get(key);
        if (watcher != null) {
            watcher.removeListener(listener);
        }
    }

    @Override
    public String getConfig(String key, String group, long timeout) throws IllegalStateException {
        if (StringUtils.isNotEmpty(group)) {
            key = group + PATH_SEPARATOR + key;
        } else {
            int i = key.lastIndexOf(".");
            key = key.substring(0, i) + PATH_SEPARATOR + key.substring(i + 1);
        }

        return (String) getInternalProperty(rootPath + PATH_SEPARATOR + key);
    }

    @Override
    public Object getInternalProperty(String key) {
        logger.info("get config from: " + key);
        Long currentIndex = consulIndexes.computeIfAbsent(key, k -> -1L);
        Response<GetValue> response = client.getKVValue(key, new QueryParams(watchTimeout, currentIndex));
        GetValue value = response.getValue();
        consulIndexes.put(key, response.getConsulIndex());
        return value != null ? value.getDecodedValue() : null;
    }

    private int buildWatchTimeout(URL url) {
        return url.getParameter(WATCH_TIMEOUT, DEFAULT_WATCH_TIMEOUT) / 1000;
    }

    private class ConsulKVWatcher implements Runnable {
        private String key;
        private Set<ConfigurationListener> listeners;
        private boolean running = true;

        public ConsulKVWatcher(String key) {
            this.key = convertKey(key);
            this.listeners = new HashSet<>();
        }

        @Override
        public void run() {
            while (running) {
                Long lastIndex = consulIndexes.computeIfAbsent(key, k -> -1L);
                Response<GetValue> response = client.getKVValue(key, new QueryParams(watchTimeout, lastIndex));

                Long currentIndex = response.getConsulIndex();
                if (currentIndex == null || currentIndex <= lastIndex) {
                    continue;
                }

                consulIndexes.put(key, currentIndex);
                String value = response.getValue().getDecodedValue();
                logger.info("notify change for key: " + key + ", the value is: " + value);
                ConfigChangeEvent event = new ConfigChangeEvent(key, value);
                for (ConfigurationListener listener : listeners) {
                    listener.process(event);
                }
            }
        }

        private void addListener(ConfigurationListener listener) {
            this.listeners.add(listener);
        }

        private void removeListener(ConfigurationListener listener) {
            this.listeners.remove(listener);
        }

        private String convertKey(String key) {
            int index = key.lastIndexOf('.');
            return rootPath + PATH_SEPARATOR + key.substring(0, index) + PATH_SEPARATOR + key.substring(index + 1);
        }
    }
}
