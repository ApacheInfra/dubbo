/*
 * Copyright 1999-2011 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.remoting.p2p.exchange;

import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.extension.ExtensionLoader;
import com.alibaba.dubbo.remoting.RemotingException;
import com.alibaba.dubbo.remoting.exchange.ExchangeHandler;

/**
 * Networkers
 * 
 * @author william.liangf
 */
public class ExchangeNetworkers {

	public static ExchangePeer join(String group, String peer, ExchangeHandler handler) throws RemotingException {
		return join(URL.valueOf(group), URL.valueOf(peer), handler);
	}

	public static ExchangePeer join(URL group, URL peer, ExchangeHandler handler) throws RemotingException {
		return lookup(group).join(peer, handler);
	}

	public static ExchangeGroup lookup(String group) throws RemotingException {
		return lookup(URL.valueOf(group));
	}

	public static ExchangeGroup lookup(URL group) throws RemotingException {
		ExchangeNetworker networker = ExtensionLoader.getExtensionLoader(ExchangeNetworker.class).getExtension(
				group.getProtocol());
		return networker.lookup(group);
	}
}