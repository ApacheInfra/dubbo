package org.apache.dubbo.rpc.protocol.tri;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.QueryStringEncoder;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.util.AttributeKey;

import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class TripleUtil {

    public static final AttributeKey<ServerStream> SERVER_STREAM_KEY = AttributeKey.newInstance("tri_server_stream");
    public static final AttributeKey<ClientStream> CLIENT_STREAM_KEY = AttributeKey.newInstance("tri_client_stream");

    public static ServerStream getServerStream(ChannelHandlerContext ctx) {
        return ctx.channel().attr(TripleUtil.SERVER_STREAM_KEY).get();
    }
    public static void setClientStream(Channel channel, ClientStream clientStream) {
        channel.attr(TripleUtil.CLIENT_STREAM_KEY).set(clientStream);
    }
    public static void setClientStream(ChannelHandlerContext ctx,ClientStream clientStream) {
        setClientStream( ctx.channel(),clientStream);
    }

    public static ClientStream getClientStream(ChannelHandlerContext ctx) {
        return ctx.channel().attr(TripleUtil.CLIENT_STREAM_KEY).get();
    }

    /**
     * must starts from application/grpc
     */
    public static boolean supportContentType(String contentType) {
        if (contentType == null) {
            return false;
        }
        return contentType.startsWith(TripleConstant.APPLICATION_GRPC);
    }

    public static void responseErr(ChannelHandlerContext ctx, GrpcStatus status) {
        Http2Headers trailers = new DefaultHttp2Headers()
                .status(OK.codeAsText())
                .set(HttpHeaderNames.CONTENT_TYPE, TripleConstant.CONTENT_PROTO)
                .setInt(TripleConstant.STATUS_KEY,status.code.code)
                .set(TripleConstant.MESSAGE_KEY, percentEncode(status.description));
        ctx.write(new DefaultHttp2HeadersFrame(trailers, true));
    }

    public static String percentDecode(CharSequence corpus) {
        if(corpus==null){
            return "";
        }
        QueryStringDecoder decoder = new QueryStringDecoder("?=" + corpus);
        for (Map.Entry<String, List<String>> e : decoder.parameters().entrySet()) {
            return e.getKey();
        }
        return "";
    }

    public static String percentEncode(String corpus) {
        QueryStringEncoder encoder = new QueryStringEncoder("");
        encoder.addParam("", corpus);
        // ?=
        return encoder.toString().substring(2);
    }

    public static void responsePlainTextError(ChannelHandlerContext ctx, int code, GrpcStatus status) {
        Http2Headers headers = new DefaultHttp2Headers(true)
                .status("" + code)
                .setInt(TripleConstant.STATUS_KEY,status.code.code)
                .set(TripleConstant.MESSAGE_KEY,status.description)
                .set(TripleConstant.CONTENT_TYPE_KEY, "text/plain; encoding=utf-8");
        ctx.write(new DefaultHttp2HeadersFrame(headers));
        ByteBuf buf = ByteBufUtil.writeUtf8(ctx.alloc(),status.description);
        ctx.write(new DefaultHttp2DataFrame(buf, true));
    }
}