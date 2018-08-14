package org.apache.bookkeeper.proto;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.ExtensionRegistry;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;

import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.proto.checksum.DummyDigestManager;
import org.apache.bookkeeper.proto.checksum.MacDigestManager;
import org.apache.bookkeeper.util.ByteBufList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumbFastClient {

    static class Args {
        @Parameter(names = { "--bookie", "-b" }, description = "Bookie to target")
        String bookie = "localhost:3181";

        @Parameter(names = { "--rate", "-r" }, description = "Target rate")
        int rate = 1000;

        @Parameter(names = { "--latency-file" }, description = "File to write latencies")
        String latencyFile = "/tmp/dumb-fast-client.latences";
    }

    static final Logger LOG = LoggerFactory.getLogger(DumbFastClient.class);

    static class LoggingHandler extends ChannelInboundHandlerAdapter {
        final DataOutputStream os;
        final long initialTimestamp;

        LoggingHandler(String file) throws Exception {
            os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            initialTimestamp = System.nanoTime();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof BookieProtocol.Response) {
                BookieProtocol.Response response = (BookieProtocol.Response) msg;
                if (response.errorCode == 0) {
                    long startTime = response.entryId;
                    long latency = System.nanoTime() - startTime;

                    os.writeLong(startTime - initialTimestamp);
                    os.writeLong(latency);
                }
            }
        }
    }

    public static void main(String[] argv) throws Exception {
        Args args = new Args();
        JCommander jCommander = new JCommander(args);
        jCommander.setProgramName("DumbFastClient");
        jCommander.parse(argv);

        RateLimiter limiter = RateLimiter.create(args.rate);
        ThreadFactory threadFactory = new DefaultThreadFactory("dumb-fast-io");
        ExtensionRegistry extRegistry = ExtensionRegistry.newInstance();
        EventLoopGroup eventLoopGroup = new EpollEventLoopGroup(4, threadFactory);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        bootstrap.channel(EpollSocketChannel.class);

        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, false);
        bootstrap.option(ChannelOption.SO_SNDBUF, 1*1024*1024);
        bootstrap.option(ChannelOption.SO_RCVBUF, 1*1024*1024);

        LoggingHandler handler = new LoggingHandler(args.latencyFile);
        bootstrap.handler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                pipeline.addLast("bytebufList", ByteBufList.ENCODER_WITH_SIZE);
                pipeline.addLast("lengthbasedframedecoder",
                        new LengthFieldBasedFrameDecoder(10*1024*1024, 0, 4, 0, 4));
                pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
                pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.RequestEncoder(extRegistry));
                pipeline.addLast(
                    "bookieProtoDecoder",
                    new BookieProtoEncoding.ResponseDecoder(extRegistry, true));
                pipeline.addLast("mainhandler", handler);
            }
        });
        HostAndPort addr = HostAndPort.fromString(args.bookie).withDefaultPort(3181);
        ChannelFuture future = bootstrap.connect(
                new InetSocketAddress(addr.getHost(), addr.getPort()));
        Channel channel = future.sync().channel();

        ByteBuf payload = PooledByteBufAllocator.DEFAULT.directBuffer(1024);
        int[] pattern = {0xde, 0xad, 0xbe, 0xef};
        for (int i = 0; i < payload.capacity(); i++) {
            payload.writeByte(pattern[i % pattern.length]);
        }

        payload.retain(); // don't let it get cleaned up

        long ledgerId = 123456L;
        DigestManager digestManager = new DummyDigestManager(ledgerId, true);
        byte[] passwd = MacDigestManager.genDigest("ledger", new byte[0]);
        while (true) {
            limiter.acquire();

            long entryId = System.nanoTime();

            ByteBufList toSend = digestManager.computeDigestAndPackageForSending(
                    entryId, 0L, 0L, payload);
            Object req = BookieProtocol.AddRequest.create(
                    BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId,
                    (short) 0, passwd, toSend);
            channel.writeAndFlush(req);
        }
    }
}
