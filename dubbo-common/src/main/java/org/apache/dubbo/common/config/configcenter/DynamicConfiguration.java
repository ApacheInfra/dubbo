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
package org.apache.dubbo.common.config.configcenter;

import org.apache.dubbo.common.config.Configuration;
import org.apache.dubbo.common.config.Environment;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.dubbo.common.extension.ExtensionLoader.getExtensionLoader;

/**
 * Dynamic Configuration
 * <br/>
 * From the use scenario internally in framework, there're mainly three kinds of methods:
 * <ul>
 * <li>1. getConfig, get governance rules or single config item from Config Center.</li>
 * <li>2. getConfigFile, get configuration file from Config Center at start up.</li>
 * <li>3. addListener/removeListener, add or remove listeners for governance rules or config items that need to watch.</li>
 * </ul>
 */
public interface DynamicConfiguration extends Configuration, AutoCloseable {

    String DEFAULT_GROUP = "dubbo";

    /**
     * {@link #addListener(String, String, ConfigurationListener)}
     *
     * @param key      the key to represent a configuration
     * @param listener configuration listener
     */
    default void addListener(String key, ConfigurationListener listener) {
        addListener(key, DEFAULT_GROUP, listener);
    }


    /**
     * {@link #removeListener(String, String, ConfigurationListener)}
     *
     * @param key      the key to represent a configuration
     * @param listener configuration listener
     */
    default void removeListener(String key, ConfigurationListener listener) {
        removeListener(key, DEFAULT_GROUP, listener);
    }

    /**
     * Register a configuration listener for a specified key
     * The listener only works for service governance purpose, so the target group would always be the value user
     * specifies at startup or 'dubbo' by default. This method will only register listener, which means it will not
     * trigger a notification that contains the current value.
     *
     * @param key      the key to represent a configuration
     * @param group    the group where the key belongs to
     * @param listener configuration listener
     */
    void addListener(String key, String group, ConfigurationListener listener);

    /**
     * Stops one listener from listening to value changes in the specified key.
     *
     * @param key      the key to represent a configuration
     * @param group    the group where the key belongs to
     * @param listener configuration listener
     */
    void removeListener(String key, String group, ConfigurationListener listener);

    /**
     * Get the configuration mapped to the given key
     *
     * @param key the key to represent a configuration
     * @return target configuration mapped to the given key
     */
    default String getConfig(String key) {
        return getConfig(key, null, -1L);
    }

    /**
     * Get the configuration mapped to the given key and the given group
     *
     * @param key   the key to represent a configuration
     * @param group the group where the key belongs to
     * @return target configuration mapped to the given key and the given group
     */
    default String getConfig(String key, String group) {
        return getConfig(key, group, -1L);
    }

    /**
     * Get the configuration mapped to the given key and the given group. If the
     * configuration fails to fetch after timeout exceeds, IllegalStateException will be thrown.
     *
     * @param key     the key to represent a configuration
     * @param group   the group where the key belongs to
     * @param timeout timeout value for fetching the target config
     * @return target configuration mapped to the given key and the given group, IllegalStateException will be thrown
     * if timeout exceeds.
     */
    String getConfig(String key, String group, long timeout) throws IllegalStateException;

    /**
     * {@see #getConfig(String, String, long)}
     * <p>
     * This method are mostly used to get a compound config file, such as a complete dubbo.properties file.
     */
    default String getConfigs(String key, String group) throws IllegalStateException {
        return getConfigs(key, group, -1L);
    }

    /**
     * {@see #getConfig(String, String, long)}
     * <p>
     * This method are mostly used to get a compound config file, such as a complete dubbo.properties file.
     */
    String getConfigs(String key, String group, long timeout) throws IllegalStateException;


    /**
     * Publish Config mapped to the given key and the given group.
     *
     * @param key     the key to represent a configuration
     * @param content the content of configuration
     * @return <code>true</code> if success, or <code>false</code>
     * @throws UnsupportedOperationException If the under layer does not support
     * @since 2.7.4
     */
    default boolean publishConfig(String key, String content) throws UnsupportedOperationException {
        return publishConfig(key, DEFAULT_GROUP, content);
    }

    /**
     * Publish Config mapped to the given key and the given group.
     *
     * @param key     the key to represent a configuration
     * @param group   the group where the key belongs to
     * @param content the content of configuration
     * @return <code>true</code> if success, or <code>false</code>
     * @throws UnsupportedOperationException If the under layer does not support
     * @since 2.7.4
     */
    default boolean publishConfig(String key, String group, String content) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("No support");
    }

    /**
     * Get the config keys by the specified group
     *
     * @param group the specified group
     * @return the read-only non-null sorted {@link Set set} of config keys
     * @throws UnsupportedOperationException If the under layer does not support
     * @since 2.7.4
     */
    default Set<String> getConfigKeys(String group) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("No support");
    }

    /**
     * Get the {@link Map} with with config keys and contents value by the specified group
     *
     * @param group the specified group
     * @return the read-only non-null sorted {@link Map map}
     * @throws UnsupportedOperationException If the under layer does not support
     * @since 2.7.4
     */
    default Map<String, String> getConfigs(String group) throws UnsupportedOperationException {
        return getConfigs(group, -1);
    }

    /**
     * Get the {@link Map} with with config keys and content value by the specified group
     *
     * @param group   the specified group
     * @param timeout the millisecond for timeout
     * @return the read-only non-null sorted {@link Map map}
     * @throws UnsupportedOperationException If the under layer does not support
     * @throws IllegalStateException         If timeout exceeds
     * @since 2.7.4
     */
    default Map<String, String> getConfigs(String group, long timeout) throws UnsupportedOperationException,
            IllegalStateException {
        Map<String, String> configs = new LinkedHashMap<>();
        Set<String> configKeys = getConfigKeys(group);
        configKeys.forEach(key -> configs.put(key, getConfig(key, group, timeout)));
        return Collections.unmodifiableMap(configs);
    }

    /**
     * Close the configuration
     *
     * @throws Exception
     * @since 2.7.4
     */
    @Override
    default void close() throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * Find DynamicConfiguration instance
     *
     * @return DynamicConfiguration instance
     */
    static DynamicConfiguration getDynamicConfiguration() {
        Optional<Configuration> optional = Environment.getInstance().getDynamicConfiguration();
        return (DynamicConfiguration) optional.orElseGet(() -> getExtensionLoader(DynamicConfigurationFactory.class)
                .getDefaultExtension()
                .getDynamicConfiguration(null));
    }
}
