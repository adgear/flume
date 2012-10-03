package com.cloudera.flume.handlers.avro;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SinkFactory;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.Pair;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.ThreadNameDeterminer;
import org.jboss.netty.util.ThreadRenamingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a sink that sends events to a remote host/port using Avro.
 */
public class AvroNonBlockingEventSink extends EventSink.Base {

	static final Logger LOG = LoggerFactory.getLogger(AvroNonBlockingEventSink.class);

	final public static String A_SERVERHOST = "serverHost";
	final public static String A_SERVERPORT = "serverPort";
	final public static String A_SENTBYTES = "sentBytes";

    String logicalName;
	String host;
	int port;
	Transceiver transport;
	FlumeEventAvroServer avroClient;

	public AvroNonBlockingEventSink(String logicalName, String host, int port) {
        this.logicalName = logicalName;
		this.host = host;
		this.port = port;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void append(Event e) throws IOException, InterruptedException {
		// convert the flumeEvent to AvroEevent
		AvroFlumeEvent afe = AvroEventConvertUtil.toAvroEvent(e);
		// Make sure client side is initialized.
		this.ensureInitialized();
		try {
			avroClient.append(afe);
			super.append(e);
		} catch (Exception e1) {
			if (e1 instanceof IOException) {
				throw (IOException)e1;
			}
			else {
				throw new IOException("Append failed " + e1.getMessage(), e1);
			}
		}
	}

	private void ensureInitialized() throws IOException {
		if (this.avroClient == null || this.transport == null) {
			throw new IOException(
					"Append called while not connected to sink");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void open() throws IOException {
        ThreadRenamingRunnable.setThreadNameDeterminer(ThreadNameDeterminer.CURRENT);
		ExecutorService bossExecutorService = new ThreadPoolExecutor(1, 10, 30, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(10000),
				new AvroNettyTransceiver.NettyTransceiverThreadFactory("[" + this.logicalName + "] Avro " + AvroNettyTransceiver.class.getSimpleName() + " Boss"),
				new ThreadPoolExecutor.DiscardPolicy());

		ExecutorService workerExecutorService = new ThreadPoolExecutor(1, 10, 30, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(10000),
				new AvroNettyTransceiver.NettyTransceiverThreadFactory("[" + this.logicalName + "] Avro " + AvroNettyTransceiver.class.getSimpleName() + " I/O Worker"),
				new ThreadPoolExecutor.DiscardPolicy());

		ChannelFactory factory = new NioClientSocketChannelFactory(
				bossExecutorService,
		        workerExecutorService);

		try {
			transport = new AvroNettyTransceiver(new InetSocketAddress(host,
					port), factory);

			this.avroClient = (FlumeEventAvroServer) SpecificRequestor
					.getClient(FlumeEventAvroServer.class, transport);
		} catch (Exception e) {
			factory.releaseExternalResources();
			throw new IOException("Failed to open Avro event sink at " + host
					+ ":" + port + " : " + e.getMessage());
		}
		LOG.info("[logicalNode " + this.logicalName + "] to " + host + ":" + port + " opened");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		if (transport != null) {
			transport.close();
			transport = null;
			LOG.info("[logicalNode " + this.logicalName + "] to " + host + ":" + port + " closed");
		}
	}

	public long getSentBytes() {
		return 0L;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ReportEvent getMetrics() {
		ReportEvent rpt = super.getMetrics();
		rpt.setStringMetric(A_SERVERHOST, host);
		rpt.setLongMetric(A_SERVERPORT, port);
		return rpt;
	}

	public static SinkBuilder builder() {
		return new SinkBuilder() {
			@Override
			public EventSink build(Context context, String... args) {
				if (args.length > 2) {
					throw new IllegalArgumentException(
							"usage: avroNbSink([hostname, [portno]]) ");
				}
				String host = FlumeConfiguration.get().getCollectorHost();
				int port = FlumeConfiguration.get().getCollectorPort();
				if (args.length >= 1) {
					host = args[0];
				}

				if (args.length >= 2) {
					port = Integer.parseInt(args[1]);
				}
				return new AvroNonBlockingEventSink(context.getValue(LogicalNodeContext.C_LOGICAL), host, port);
			}
		};
	}

	@SuppressWarnings("unchecked")
	public static List<Pair<String, SinkFactory.SinkBuilder>> getSinkBuilders() {
		return Arrays.asList(new Pair<String, SinkFactory.SinkBuilder>(
				"avroNbSink", builder()));
	}
}
