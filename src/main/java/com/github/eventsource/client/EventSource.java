package com.github.eventsource.client;

import com.github.eventsource.client.impl.AsyncEventSourceHandler;
import com.github.eventsource.client.impl.EventSourceAggregator;
import com.github.eventsource.client.impl.netty.EventSourceChannelHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.util.concurrent.Future;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class EventSource  {
    public static final long DEFAULT_RECONNECTION_TIME_MILLIS = 10000;

    public static final int CONNECTING = 0;
    public static final int OPEN = 1;
    public static final int CLOSED = 2;

    private final Bootstrap bootstrap;
    private final EventSourceHandler eventSourceHandler;
    private final EventSourceChannelHandler clientHandler;

    private AtomicInteger readyState = new AtomicInteger(CLOSED);
    private OnReadyStateChangeListener onReadyStateChangedListener;

    /**
     * Creates a new <a href="http://dev.w3.org/html5/eventsource/">EventSource</a> client. The client will reconnect on 
     * lost connections automatically, unless the connection is closed explicitly by a call to 
     * {@link com.github.eventsource.client.EventSource#close()}.
     *
     * For sample usage, see examples at <a href="https://github.com/aslakhellesoy/eventsource-java/tree/master/src/test/java/com/github/eventsource/client">GitHub</a>.
     * 
     * @param executor the executor that will receive events
     * @param reconnectionTimeMillis delay before a reconnect is made - in the event of a lost connection
     * @param uri where to connect
     * @param eventSourceHandler receives events
     * @see #close()
     */
    public EventSource(Executor executor, long reconnectionTimeMillis, final URI uri, EventSourceHandler eventSourceHandler) {
        EventLoopGroup group = new NioEventLoopGroup();

        bootstrap = new Bootstrap();

        this.eventSourceHandler = eventSourceHandler;

        clientHandler = new EventSourceChannelHandler(new AsyncEventSourceHandler(executor, eventSourceHandler), reconnectionTimeMillis, bootstrap, uri);

        bootstrap.
            group(group).
            channel(NioSocketChannel.class).
            option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000).
            option(ChannelOption.SO_KEEPALIVE, true).
            remoteAddress(new InetSocketAddress(uri.getHost(), uri.getPort())).
            handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel channel) throws Exception {
                    ChannelPipeline pipeline = channel.pipeline();
                    pipeline.addLast("decoder", new HttpResponseDecoder());
                    pipeline.addLast("aggregator", new EventSourceAggregator());
                    pipeline.addLast("line", new DelimiterBasedFrameDecoder(Integer.MAX_VALUE, Delimiters.lineDelimiter()));
                    pipeline.addLast("string", new StringDecoder());
                    pipeline.addLast("encoder", new HttpRequestEncoder());
                    pipeline.addLast("es-handler", clientHandler);
                }
            });
    }

    private static int getPort(URI uri) {
        String scheme = uri.getScheme();
        if (scheme.equals("http")) {
            return 80;
        } else if (scheme.equals("https")) {
            return 443;
        }
        return -1;
    }

    private static URI createURI(String url) {
        URI uri = URI.create(url);
        try {
            return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), getPort(uri), uri.getPath(), uri.getQuery(), uri.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public EventSource(String url, EventSourceHandler eventSourceHandler) {
        this(createURI(url), eventSourceHandler);
    }

    public EventSource(URI uri, EventSourceHandler eventSourceHandler) {
        this(Executors.newSingleThreadExecutor(), DEFAULT_RECONNECTION_TIME_MILLIS, uri, eventSourceHandler);
    }

    public EventSource connect() {
        clientHandler.setReconnectOnClose(true);

        if (readyState.compareAndSet(CLOSED, CONNECTING)) {
            dispatchOnReadyStateChanged();
            final ChannelFuture cf = bootstrap.connect();

            cf.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if(future.isSuccess()) {
                        readyState.set(OPEN);
                        dispatchOnReadyStateChanged();
                    } else {
                        readyState.set(CLOSED);
                        dispatchOnReadyStateChanged();
                        if(future.cause() != null) {
                            eventSourceHandler.onError(future.cause());
                        }
                    }
                }
            });

            cf.syncUninterruptibly();
        }

        return this;
    }

    /**
     * Close the connection
     *
     * @return self
     */
    public EventSource close() {
        clientHandler.setReconnectOnClose(false);
        readyState.set(CLOSED);
        clientHandler.close();
        return this;
    }

    /**
     * Wait until the connection is closed
     *
     * @return self
     * @throws InterruptedException if waiting was interrupted
     */
    public EventSource join() throws InterruptedException {
        clientHandler.join();
        return this;
    }

    public EventSource setLastEventId(String id) {
        clientHandler.setLastEventId(id);
        return this;
    }

    public EventSource shutdown() {
        bootstrap.group().shutdownGracefully().syncUninterruptibly();
        return this;
    }

    public EventSource withHeader(String header, String value) {
        clientHandler.withHeader(header, value);
        return this;
    }

    public int getReadyState() {
        return readyState.get();
    }

    private void dispatchOnReadyStateChanged() {
        if (this.onReadyStateChangedListener != null) {
            this.onReadyStateChangedListener.onReadyStateChanged(getReadyState());
        }
    }
    public void setOnReadyStateChangedListener(OnReadyStateChangeListener listener) {
        this.onReadyStateChangedListener = listener;
    }

    public interface OnReadyStateChangeListener {
        void onReadyStateChanged(int readyState);
    }
}
