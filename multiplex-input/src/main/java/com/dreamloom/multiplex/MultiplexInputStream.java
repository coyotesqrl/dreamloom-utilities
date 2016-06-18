/*
 Copyright (C) 2016 R.A. Porter
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.dreamloom.multiplex;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Creates a multiplexed input stream, capable of parallel, concurrent reads by multiple consumers.
 * <p>
 * Input streams are depleted as they are consumed; this decorator allows multiple consumers to read
 * from the same stream without making multiple copies and overfilling memory. Because it operates
 * in parallel, it also provides the performance benefit of concurrent stream reading without adding
 * complexity to client code.
 * <p>
 * If any of the {@code Consumer} threads throw an exception or take longer than the configured time
 * to read a chunk of data from memory ({@link #AWAIT_SECONDS} seconds), the multiplexer will fail
 * all the consumer threads.
 */
public class MultiplexInputStream extends InputStream {
    private static final int DEFAULT_BUFFER_SIZE = 2048;

    private static final int AWAIT_SECONDS = 10;

    private static final ThreadLocal<Integer> INDEX = new ThreadLocal<Integer>() {
        @Override
        protected Integer initialValue() {
            return 0;
        }
    };

    private static final String MULTIPLEX_THREAD_PREFIX = "Multiplex";

    private final BufferedInputStream bis;

    private final byte[] buffer;

    private Phaser phaser;

    private int currentReadBytes;

    private ExecutorService service;

    private final AtomicInteger factoryIdx = new AtomicInteger(0);

    /**
     * Creates a new {@code MultiplexInputStream} instance with an in-memory buffer of default
     * size, 2048 bytes.
     *
     * @param source the underlying input stream to decorate and multiplex
     * @throws IOException if there is a problem reading the underlying stream
     */
    public MultiplexInputStream(InputStream source) throws IOException {
        this(source, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new {@code MultiplexInputStream} instance with an in-memory buffer of
     * {@code bufferSize)} bytes.
     *
     * @param source     the underlying input stream to decorate and multiplex
     * @param bufferSize the size of the in-memory buffer
     * @throws IOException if there is a problem reading the underlying stream
     */
    public MultiplexInputStream(InputStream source, int bufferSize) throws IOException {
        buffer = new byte[bufferSize];
        bis = new BufferedInputStream(source);

        currentReadBytes = bis.read(buffer);
        if (currentReadBytes == -1) {
            throw new IllegalStateException(
                    "Error initializing stream; no content found in source.");
        }
    }

    /**
     * Starts the multiplexer, running the list of provided {@code Consumer}s to read the stream.
     * <p>
     * It is the responsibility of the caller to ensure that each of the provided {@code Consumer}s
     * correctly ingests and consumes the {@code InputStream} and does not block.
     *
     * @param consumers the list of processors reading the {@code InputStream}
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void invoke(List<Consumer<InputStream>> consumers)
            throws ExecutionException, InterruptedException {
        phaser = new Phaser(consumers.size()) {
            @Override
            protected boolean onAdvance(int phase, int registeredParties) {
                if (registeredParties == 0) {
                    return true;
                }
                try {
                    currentReadBytes = bis.read(buffer);
                } catch (IOException e) {
                    // Failure to read the underlying stream should result in a failure of all running
                    // consumer threads.
                    throw new UncheckedIOException(e);
                }

                return false;
            }
        };

        service = Executors.newFixedThreadPool(consumers.size(),
                r -> new Thread(r, getConsumerThreadName()));

        Set<Future> futures = consumers.stream()
                .map(consumer -> service.submit(() -> consumer.accept(MultiplexInputStream.this)))
                .collect(Collectors.toSet());

        for (Future future : futures) {
            future.get();
        }
    }

    @Override
    public int read() throws IOException {
        Integer index = INDEX.get();
        // Block if this thread has gotten the last byte of the current buffer
        if (index == currentReadBytes) {
            try {
                phaser.awaitAdvanceInterruptibly(phaser.arrive(), AWAIT_SECONDS, TimeUnit.SECONDS);

                index = 0;
                INDEX.set(index);
            } catch (InterruptedException | TimeoutException e) {
                throw new IOException("Error processing multiplexed input", e);
            }
        }

        if (currentReadBytes == -1) {
            return -1;
        }

        byte b = buffer[index++];
        INDEX.set(index);
        return b;
    }

    @Override
    public void close() throws IOException {
        // If caller is one of the consumer threads, arriveAndDeregister on its behalf;
        // else, close down resources
        synchronized (MULTIPLEX_THREAD_PREFIX) {
            if (Thread.currentThread()
                    .getName()
                    .startsWith(MULTIPLEX_THREAD_PREFIX)) {
                phaser.arriveAndDeregister();
            } else {
                bis.close();
                service.shutdownNow();
            }
        }
    }

    private String getConsumerThreadName() {
        return String.format("%s-%d-%s", MULTIPLEX_THREAD_PREFIX, factoryIdx.getAndIncrement(),
                UUID.randomUUID()
                        .toString());
    }
}
