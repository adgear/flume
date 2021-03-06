/*
 * Copyright 2012 Cloudera.
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
package com.cloudera.flume.handlers.avro;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.apache.avro.ipc.NettyTransportCodec.NettyDataPack;
import org.apache.avro.ipc.NettyTransportCodec.NettyFrameDecoder;
import org.apache.avro.ipc.NettyTransportCodec.NettyFrameEncoder;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Netty-based RPC {@link Server} implementation.
 */
public class AvroNettyServer implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(AvroNettyServer.class
      .getName());

  private final Responder responder;

  private final Channel serverChannel;
  private final ChannelGroup allChannels = new DefaultChannelGroup(
      "avro-netty-server");
  private final ChannelFactory channelFactory;
  private final CountDownLatch closed = new CountDownLatch(1);
  private final ExecutionHandler executionHandler;

  private final String logicalName;

  public AvroNettyServer(Responder responder, InetSocketAddress addr, String logicalName) {
    this(responder, addr, new NioServerSocketChannelFactory
         (Executors.newCachedThreadPool(), Executors.newCachedThreadPool()), logicalName);
  }

  public AvroNettyServer(Responder responder, InetSocketAddress addr,
                     ChannelFactory channelFactory, String logicalName) {
      this(responder, addr, channelFactory, null, logicalName);
  }

    /**
     *
     * @param executionHandler if not null, will be inserted into the Netty
     *                         pipeline. Use this when your responder does
     *                         long, non-cpu bound processing (see Netty's
     *                         ExecutionHandler javadoc).
     */
  public AvroNettyServer(Responder responder, InetSocketAddress addr,
                     ChannelFactory channelFactory, final ExecutionHandler executionHandler, final String logicalName) {
      this.responder = responder;
      this.channelFactory = channelFactory;
      this.executionHandler = executionHandler;
      this.logicalName = logicalName;
      ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
          @Override
          public ChannelPipeline getPipeline() throws Exception {
              ChannelPipeline p = Channels.pipeline();
              p.addLast("frameDecoder", new NettyFrameDecoder());
              p.addLast("frameEncoder", new NettyFrameEncoder());
              if (executionHandler != null) {
                  p.addLast("executionHandler", executionHandler);
              }
              p.addLast("handler", new NettyServerAvroHandler(logicalName));
              return p;
          }
      });
      bootstrap.setOption("child.tcpNoDelay", true);
      bootstrap.setOption("child.keepAlive", true);
      serverChannel = bootstrap.bind(addr);
      allChannels.add(serverChannel);
  }

  @Override
  public void start() {
    // No-op.
  }

  @Override
  public void close() {
    ChannelGroupFuture future = allChannels.close();
    future.awaitUninterruptibly();
    channelFactory.releaseExternalResources();
    closed.countDown();
  }

  @Override
  public int getPort() {
    return ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
  }

  @Override
  public void join() throws InterruptedException {
    closed.await();
  }

  /**
   * Avro server handler for the Netty transport
   */
  class NettyServerAvroHandler extends SimpleChannelUpstreamHandler {

    private final AvroNettyTransceiver connectionMetadata;

    public NettyServerAvroHandler(String logicalName) {
        super();
        this.connectionMetadata = new AvroNettyTransceiver(logicalName);
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
        throws Exception {
      if (e instanceof ChannelStateEvent) {
        LOG.info(e.toString());
      }
      super.handleUpstream(ctx, e);
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
        throws Exception {
      allChannels.add(e.getChannel());
      super.channelOpen(ctx, e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      try {
        NettyDataPack dataPack = (NettyDataPack) e.getMessage();
        List<ByteBuffer> req = dataPack.getDatas();
        List<ByteBuffer> res = responder.respond(req, connectionMetadata);
        // response will be null for oneway messages.
        if(res != null) {
          dataPack.setDatas(res);
          e.getChannel().write(dataPack);
        }
      } catch (IOException ex) {
        LOG.warn("unexpect error");
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      LOG.warn("Unexpected exception from downstream for [" + this.connectionMetadata.getLogicalName() + "]", e.getCause());
      e.getChannel().close();
    }

  }

}