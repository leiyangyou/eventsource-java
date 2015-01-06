package com.github.eventsource.client.impl;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpChunkedInput;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.ReferenceCountUtil;

public class EventSourceAggregator extends ChannelInboundHandlerAdapter {
    private boolean receivedResponse = false;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpResponse) {
            receivedResponse = true;
            ctx.fireChannelRead(msg);
        } else if (msg instanceof HttpContent) {
            if (!receivedResponse) {
                throw new IllegalStateException("received " + HttpChunkedInput.class.getSimpleName() + " without HttpResponse");
            }
            ctx.fireChannelRead(((HttpContent) msg).content());
        } else {
            ctx.fireChannelRead(msg);
        }
    }
}
