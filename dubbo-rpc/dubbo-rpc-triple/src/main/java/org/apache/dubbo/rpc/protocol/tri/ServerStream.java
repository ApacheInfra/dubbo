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
package org.apache.dubbo.rpc.protocol.tri;

import java.io.IOException;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2Headers;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.model.ServiceRepository;
import org.apache.dubbo.rpc.protocol.tri.GrpcStatus.Code;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public abstract class ServerStream extends AbstractStream implements Stream {
    protected ServiceDescriptor serviceDescriptor;
    protected static final String TOO_MANY_REQ = "Too many requests";
    protected static final String MISSING_REQ = "Missing request";
    protected static final ExecutorRepository EXECUTOR_REPOSITORY =
        ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension();
    private final ProviderModel providerModel;
    private MethodDescriptor md;
    private final Invoker<?> invoker;
    private Processor processor;

    public void setProcessor(Processor processor) {
        this.processor = processor;
    }

    public Processor getProcessor() {
        return processor;
    }

    public ProviderModel getProviderModel() {
        return providerModel;
    }

    public MethodDescriptor getMd() {
        return md;
    }

    public Invoker<?> getInvoker() {
        return invoker;
    }

    protected ServerStream(Invoker<?> invoker, URL url, MethodDescriptor md, ChannelHandlerContext ctx) {
        super(url, ctx);
        ServiceRepository repo = ApplicationModel.getServiceRepository();
        this.providerModel = repo.lookupExportedService(getUrl().getServiceKey());
        if (providerModel != null) {
            ClassLoadUtil.switchContextLoader(providerModel.getServiceInterfaceClass().getClassLoader());
        }
        this.invoker = invoker;
        this.md = md;
    }

    @Override
    public void streamCreated(boolean endStream) throws Exception {
        if (endStream) {
            halfClose();
        }
    }

    protected RpcInvocation buildInvocation() {
        RpcInvocation inv = new RpcInvocation();
        if (md.isNeedWrap()) {
            loadFromURL(getUrl());
        }

        inv.setMethodName(md.getMethodName());
        inv.setServiceName(serviceDescriptor.getServiceName());
        inv.setTargetServiceUniqueName(getUrl().getServiceKey());
        final Map<String, Object> attachments = parseHeadersToMap(getHeaders());
        inv.setObjectAttachments(attachments);
        return inv;
    }

    protected void writeData(ByteBuf buf, Map<String, Object> attachments) throws IOException {
        Http2Headers http2Headers = new DefaultHttp2Headers()
            .status(OK.codeAsText())
            .set(HttpHeaderNames.CONTENT_TYPE, TripleConstant.CONTENT_PROTO);
        getCtx().write(new DefaultHttp2HeadersFrame(http2Headers));
        final DefaultHttp2DataFrame data = new DefaultHttp2DataFrame(buf);
        getCtx().write(data);
        final Http2Headers trailers = new DefaultHttp2Headers()
            .setInt(TripleConstant.STATUS_KEY, Code.OK.code);

        if (attachments != null) {
            convertAttachment(trailers, attachments);
        }
        getCtx().writeAndFlush(new DefaultHttp2HeadersFrame(trailers, true));
    }
}
