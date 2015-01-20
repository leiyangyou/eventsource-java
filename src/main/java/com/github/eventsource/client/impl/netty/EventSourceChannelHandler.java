package com.github.eventsource.client.impl.netty;

import com.github.eventsource.client.EventSourceException;
import com.github.eventsource.client.EventSourceHandler;
import com.github.eventsource.client.impl.ConnectionHandler;
import com.github.eventsource.client.impl.EventStreamParser;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.*;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ChannelHandler.Sharable
public class EventSourceChannelHandler extends ChannelInboundHandlerAdapter implements ConnectionHandler {
    private final EventSourceHandler eventSourceHandler;
    private final Bootstrap bootstrap;
    private final URI uri;
    private final EventStreamParser messageDispatcher;

    private static final Timer TIMER = new HashedWheelTimer();
    private Channel channel;
    private boolean reconnectOnClose = true;
    private long reconnectionTimeMillis;
    private String lastEventId;
    private boolean eventStreamOk;
    private AtomicBoolean reconnecting = new AtomicBoolean(false);
    private Map<String, String> headers = new HashMap<String, String>();

    public EventSourceChannelHandler(EventSourceHandler eventSourceHandler, long reconnectionTimeMillis, Bootstrap bootstrap, URI uri) {
        this.eventSourceHandler = eventSourceHandler;
        this.reconnectionTimeMillis = reconnectionTimeMillis;
        this.bootstrap = bootstrap;
        this.uri = uri;
        this.messageDispatcher = new EventStreamParser(uri.toString(), eventSourceHandler, this);
    }

    @Override
    public void channelActive(ChannelHandlerContext context) {
        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri.toString());
        
        request.headers().add(Names.ACCEPT, "text/event-stream");
        request.headers().add(Names.HOST, uri.getHost());
        request.headers().add(Names.ORIGIN, uri.getScheme() + uri.getHost());
        request.headers().add(Names.CACHE_CONTROL, "no-cache");

        for (Map.Entry<String, String> e : headers.entrySet()) {
            request.headers().add(e.getKey(), e.getValue());
        }

        if (lastEventId != null) {
            request.headers().add("Last-Event-ID", lastEventId);
        }

        channel = context.channel();
        channel.writeAndFlush(request);
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        channel = null;

        if (eventStreamOk) {
            eventSourceHandler.onClosed(reconnectOnClose);
        }

        if (reconnectOnClose) {
            reconnect();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpResponse) {
            HttpResponse httpMessage = (HttpResponse)msg;

            if (!HttpVersion.HTTP_1_1.equals(httpMessage.protocolVersion())) {
                eventSourceHandler.onError(new EventSourceException("Not HTTP 1.1 " + uri));
            }

            if (!HttpResponseStatus.OK.equals(httpMessage.status())) {
                eventSourceHandler.onError(new EventSourceException("Bad status from " + uri + ": " + httpMessage.getStatus()));
                reconnect();
                return;
            }

            if (!"text/event-stream".equals(httpMessage.headers().get(HttpHeaders.Names.CONTENT_TYPE))) {
                eventSourceHandler.onError(new EventSourceException("Not event stream: " + uri + " (expected Content-Type: text/event-stream"));
                reconnect();
                return;
            }

            eventStreamOk = true;
            eventSourceHandler.onConnect();
        } else if (msg instanceof String) {
            messageDispatcher.line((String)msg);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable error) throws Exception {
        if(error instanceof ConnectException) {
            error = new EventSourceException("Failed to connect to " + uri, error);
        }
        eventSourceHandler.onError(error);
        context.channel().close();
    }

    @Override
    public void setReconnectionTimeMillis(long reconnectionTimeMillis) {
        this.reconnectionTimeMillis = reconnectionTimeMillis;
    }

    @Override
    public void setLastEventId(String lastEventId) {
        this.lastEventId = lastEventId;
    }

    public void withHeader(String name, String value) {
        this.headers.put(name, value);
    }

    public void setReconnectOnClose(boolean reconnectOnClose) {
        this.reconnectOnClose = reconnectOnClose;
    }

    public EventSourceChannelHandler close() {
        if (channel != null) {
            channel.close();
        }
        return this;
    }

    public EventSourceChannelHandler join() throws InterruptedException {
        if (channel != null) {
            channel.closeFuture().await();
        }
        return this;
    }

    private void reconnect() {
        if(reconnecting.compareAndSet(false, true)) {
            eventStreamOk = false;
            TIMER.newTimeout(new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    reconnecting.set(false);
                    bootstrap.remoteAddress(new InetSocketAddress(uri.getHost(), uri.getPort()));
                    bootstrap.connect().await();
                }
            }, reconnectionTimeMillis, TimeUnit.MILLISECONDS);
        }
    }
}
