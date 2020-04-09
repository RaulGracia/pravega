package io.pravega.client.nonetty.impl;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import io.pravega.client.netty.impl.AppendBatchSizeTrackerImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TcpClientConnection implements ClientConnection {
    
    private final Socket socket;
    private final CommandEncoder encoder;
    private final ConnectionReader reader;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    
    @RequiredArgsConstructor
    private static class ConnectionReader {
        private final InputStream inputStream;
        private final ReplyProcessor callback;
        private final ScheduledExecutorService thread;
        private final AppendBatchSizeTracker batchSizeTracker;
        private final AtomicBoolean stop = new AtomicBoolean(false);
        
        public void start() {
            thread.submit(() -> {
                byte[] header = new byte[8];
                while(!stop.get()) {
                    try {
                        inputStream.readNBytes(header, 0, 8);
                        ByteBuffer headerReadingBuffer = ByteBuffer.wrap(header);
                        int t = headerReadingBuffer.getInt();
                        WireCommandType type = WireCommands.getType(t);
                        if (type == null) {
                            throw new InvalidMessageException("Unknown wire command: " + t);
                        }
                        
                        int length = headerReadingBuffer.getInt();
                        if (length < 0 || length > WireCommands.MAX_WIRECOMMAND_SIZE) {
                            throw new InvalidMessageException("Event of invalid length: " + length);
                        }
                        
                        byte[] bytes = inputStream.readNBytes(length);
                        WireCommand command = type.readFrom(new ByteBufInputStream(Unpooled.wrappedBuffer(bytes)), length);
                        if (command instanceof WireCommands.DataAppended) {
                            WireCommands.DataAppended dataAppended = (WireCommands.DataAppended) command;
                            batchSizeTracker.recordAck(dataAppended.getEventNumber());
                        }
                       
                        callback.process((Reply) command);
                        
                    } catch (Exception e) {
                        log.error("Error processing data from from server ", e);
                        stop();
                    }
                }
            });
        }
        
        
        public void stop() {
            stop.set(true);
            thread.shutdown();
        }
    }

    @SneakyThrows(IOException.class)
    public TcpClientConnection(String host, int port, ReplyProcessor callback) {
        socket = new Socket(host, port);
        InputStream inputStream = socket.getInputStream();
        ScheduledExecutorService pool = ExecutorServiceHelpers.newScheduledThreadPool(1, "Reading from " + host);
        AppendBatchSizeTrackerImpl batchSizeTracker = new AppendBatchSizeTrackerImpl();
        this.reader = new ConnectionReader(inputStream, callback, pool, batchSizeTracker);
        this.encoder = new CommandEncoder(l -> batchSizeTracker, null, socket.getOutputStream(), pool);
        this.reader.start();
    }

    @Override
    public void send(WireCommand cmd) throws ConnectionFailedException {
        if (closed.get()) {
            throw new ConnectionFailedException("Connection is closed");
        }
        try {
            encoder.write(cmd);
        } catch (IOException e) {
            log.warn("Error writing to connection");
            close();
            throw new ConnectionFailedException(e);
        }
    }

    @Override
    public void send(Append append) throws ConnectionFailedException {
        if (closed.get()) {
            throw new ConnectionFailedException("Connection is closed");
        }
        try {
            encoder.write(append);
        } catch (IOException e) {
            log.warn("Error writing to connection");
            close();
            throw new ConnectionFailedException(e);
        }
    }

    @Override
    public void close() {
        closed.set(true);
        reader.stop();
        try {
            socket.close();
        } catch (IOException e) {
            log.warn("Error closing socket", e);
        }
    }

    @Override
    public void sendAsync(List<Append> appends) {
        try {
            for (Append append : appends) {
                encoder.write(append);
            }
        } catch (IOException e) {
            log.warn("Error writing to connection");
            close();
        }
    }

}
