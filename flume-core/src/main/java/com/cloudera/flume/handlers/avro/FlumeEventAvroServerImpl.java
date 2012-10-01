/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.*;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
/**
 * This implements the AvroEventServer.
 */
public class FlumeEventAvroServerImpl implements FlumeEventAvroServer {
  private Server server;
  private final String logicalName;
  private final int port;
  private final boolean blocking;

  /**
   * This just sets the port for this AvroServer
   */
  public FlumeEventAvroServerImpl(String logicalName, int port, boolean blocking) {
    this.logicalName = logicalName;
    this.port = port;
    this.blocking = blocking;
  }

  /**
   * This just sets the port for this AvroServer
   */
  public FlumeEventAvroServerImpl(String logicalName, int port) {
    this(logicalName, port, false);
  }

  /**
   * This blocks till the server starts.
   */
  public void start() throws IOException {
    SpecificResponder res = new SpecificResponder(FlumeEventAvroServer.class,
        this);
    if (blocking) {
        this.server = new HttpServer(res, port);
    }
    else {
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT);
        ExecutorService bossExecutorService = new ThreadPoolExecutor(1, Integer.MAX_VALUE, 60, TimeUnit.SECONDS,
				new SynchronousQueue<Runnable>(),
				new AvroNettyTransceiver.NettyTransceiverThreadFactory("[" + this.logicalName + "] Avro " + FlumeEventAvroServerImpl.class.getSimpleName() + " Boss"));

		ExecutorService workerExecutorService = new ThreadPoolExecutor(1, Integer.MAX_VALUE, 60, TimeUnit.SECONDS,
				new SynchronousQueue<Runnable>(),
				new AvroNettyTransceiver.NettyTransceiverThreadFactory("[" + this.logicalName + "] Avro " + FlumeEventAvroServerImpl.class.getSimpleName() + " I/O Worker"));

		ChannelFactory factory = new NioServerSocketChannelFactory(
				bossExecutorService,
		        workerExecutorService);

        this.server = new AvroNettyServer(res, new InetSocketAddress(port), factory);
    }
    this.server.start();
  }

  @Override
  public void append(AvroFlumeEvent evt) {
  }

  /**
   * Stops the FlumeEventAvroServer, called only from the server.
   */
  public void close() throws AvroRemoteException {
    if (server != null) {
        server.close();
    }
  }
}
