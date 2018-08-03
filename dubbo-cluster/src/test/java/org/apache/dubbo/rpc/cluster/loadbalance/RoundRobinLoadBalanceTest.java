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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.AtomicPositiveInteger;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class RoundRobinLoadBalanceTest extends LoadBalanceBaseTest {
    @Test
    public void testRoundRobinLoadBalanceSelect() {
        int runs = 10000;
        Map<Invoker, AtomicLong> counter = getInvokeCounter(runs, RoundRobinLoadBalance.NAME);
        for (Invoker minvoker : counter.keySet()) {
            Long count = counter.get(minvoker).get();
            Assert.assertTrue("abs diff should < 1", Math.abs(count - runs / (0f + invokers.size())) < 1f);
        }
    }

    @Test
    public void testSelectByWeight() {
        int sumInvoker1 = 0;
        int sumInvoker2 = 0;
        int sumInvoker3 = 0;
        int loop = 100000;

        MyRoundRobinLoadBalance lb = new MyRoundRobinLoadBalance();
        for (int i = 0; i < loop; i++) {
            Invoker selected = lb.select(weightInvokers, null, null);

            if (selected.getUrl().getProtocol().equals("test1")) {
                sumInvoker1++;
            }

            if (selected.getUrl().getProtocol().equals("test2")) {
                sumInvoker2++;
            }

            if (selected.getUrl().getProtocol().equals("test3")) {
                sumInvoker3++;
            }
        }

        // 1 : 9 : 6
        System.out.println(sumInvoker1);
        System.out.println(sumInvoker2);
        System.out.println(sumInvoker3);
        Assert.assertEquals("select failed!", sumInvoker1 + sumInvoker2 + sumInvoker3, loop);
    }

    class MyRoundRobinLoadBalance extends AbstractLoadBalance {

        public static final String NAME = "roundrobin";

        private final ConcurrentMap<String, AtomicPositiveInteger> sequences = new ConcurrentHashMap<String, AtomicPositiveInteger>();

        @Override
        protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
            String key = "method1";
            int length = invokers.size(); // Number of invokers
            int maxWeight = 0; // The maximum weight
            int minWeight = Integer.MAX_VALUE; // The minimum weight
            final LinkedHashMap<Invoker<T>, IntegerWrapper> invokerToWeightMap = new LinkedHashMap<Invoker<T>, IntegerWrapper>();
            int weightSum = 0;
            for (int i = 0; i < length; i++) {

                int weight = invokers.get(i).getUrl().getPort();

                maxWeight = Math.max(maxWeight, weight); // Choose the maximum weight
                minWeight = Math.min(minWeight, weight); // Choose the minimum weight
                if (weight > 0) {
                    invokerToWeightMap.put(invokers.get(i), new IntegerWrapper(weight));
                    weightSum += weight;
                }
            }
            AtomicPositiveInteger sequence = sequences.get(key);
            if (sequence == null) {
                sequences.putIfAbsent(key, new AtomicPositiveInteger());
                sequence = sequences.get(key);
            }
            int currentSequence = sequence.getAndIncrement();
            if (maxWeight > 0 && minWeight < maxWeight) {
                int mod = currentSequence % weightSum;
                for (int i = 0; i < maxWeight; i++) {
                    for (Map.Entry<Invoker<T>, IntegerWrapper> each : invokerToWeightMap.entrySet()) {
                        final Invoker<T> k = each.getKey();
                        final IntegerWrapper v = each.getValue();
                        if (mod == 0 && v.getValue() > 0) {
                            return k;
                        }
                        if (v.getValue() > 0) {
                            v.decrement();
                            mod--;
                        }
                    }
                }
            }
            // Round robin
            return invokers.get(currentSequence % length);
        }

        private final class IntegerWrapper {
            private int value;

            public IntegerWrapper(int value) {
                this.value = value;
            }

            public int getValue() {
                return value;
            }

            public void setValue(int value) {
                this.value = value;
            }

            public void decrement() {
                this.value--;
            }
        }
    }
}
