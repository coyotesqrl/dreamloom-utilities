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
package com.dreamloom.ioutils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Creates a splittable input stream, capable of parallel, concurrent reads by multiple consumers.
 * <p>
 * Input streams are depleted as they are consumed; this decorator allows multiple consumers to read
 * from the same stream without making multiple copies and overfilling memory. Because it operates
 * in parallel, it also provides the performance benefit of concurrent stream reading without adding
 * complexity to client code.
 * <p>
 * If any of the {@code Consumer} threads throw an exception or take longer than the configured time
 * to read a chunk of data from memory, all the consumer threads will fail.
 */
public class SplittableInputStream extends InputStream {
    private static final int DEFAULT_BUFFER_SIZE = 4096;

    private static final int DEFAULT_AWAIT = 10;

    private static final ThreadLocal<Integer> INDEX = new ThreadLocal<Integer>() {
        @Override
        protected Integer initialValue() {
            return 0;
        }
    };

    private static final String MULTIPLEX_THREAD_PREFIX = "Multiplex";

    private final Lock lock = new ReentrantLock(true);

    private final BufferedInputStream bis;

    private final byte[] buffer;

    private final int waitSeconds;

    private Phaser phaser;

    private volatile int currentReadBytes;

    private ExecutorService service;

    private final AtomicInteger factoryIdx = new AtomicInteger(0);

    private final ConcurrentSkipListSet<String> closedConsumers = new ConcurrentSkipListSet<>();

    /**
     * Creates a new {@code SplittableInputStream} instance with an in-memory buffer
     * {@link #DEFAULT_BUFFER_SIZE} bytes and a timeout of {@link #DEFAULT_AWAIT} seconds.
     *
     * @param source the underlying input stream to decorate and split
     * @throws IOException if there is a problem reading the underlying stream
     */
    public SplittableInputStream(InputStream source) throws IOException {
        this(source, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Creates a new {@code SplittableInputStream} instance with an in-memory buffer of
     * {@code bufferSize)} bytes and a timeout of {@link #DEFAULT_AWAIT} seconds.
     *
     * @param source     the underlying input stream to decorate and split
     * @param bufferSize the size of the in-memory buffer
     * @throws IOException if there is a problem reading the underlying stream
     */
    public SplittableInputStream(InputStream source, int bufferSize) throws IOException {
        this(source, bufferSize, DEFAULT_AWAIT);
    }

    /**
     * Creates a new {@code SplittableInputStream} instance with an in-memory buffer of
     * {@code bufferSize)} bytes and a timeout of {@code waitSeconds} seconds.
     *
     * @param source      the underlying input stream to decorate and split
     * @param bufferSize  the size of the in-memory buffer
     * @param waitSeconds the number of seconds to allow any consumer to block processing before the
     *                    entire batch fails
     * @throws IOException if there is a problem reading the underlying stream
     */
    public SplittableInputStream(InputStream source, int bufferSize, int waitSeconds)
            throws IOException {
        buffer = new byte[bufferSize];
        bis = new BufferedInputStream(source);
        this.waitSeconds = waitSeconds;
    }

    /**
     * Starts the stream, running the list of provided {@code Consumer}s to read the stream.
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
        // Register main thread to prevent phaser from advancing too soon
        phaser = new Phaser(1) {
            @Override
            protected boolean onAdvance(int phase, int registeredParties) {
                if (registeredParties == 0) {
                    return true;
                }
                try {
                    currentReadBytes = bis.read(buffer);
                    return false;
                } catch (IOException e) {
                    // Failure to read the underlying stream should result in a failure of all running
                    // consumer threads.
                    throw new UncheckedIOException(e);
                }
            }
        };

        service = Executors.newFixedThreadPool(consumers.size(),
                r -> new Thread(r, getConsumerThreadName()));

        CountDownLatch countDownLatch = new CountDownLatch(consumers.size());
        Set<Future> futures = consumers.stream()
                .map(consumer -> service.submit(() -> {
                    phaser.register();
                    countDownLatch.countDown();
                    consumer.accept(SplittableInputStream.this);
                }))
                .collect(Collectors.toSet());

        // Don't release phaser lock until all consumer threads have registered
        // with the phaser
        countDownLatch.await();
        // Main thread now deregisters and lets the others engage with the phaser
        phaser.arriveAndDeregister();

        for (Future future : futures) {
            future.get();
        }
    }

    @Override
    public int read() throws IOException {
        if (phaser.isTerminated() || currentReadBytes == -1
                || closedConsumers.contains(Thread.currentThread()
                .getName())) {
            return -1;
        }

        if (currentReadBytes == 0) {
            awaitInterruptibly(null);
        }

        Integer index = INDEX.get();
        byte b = buffer[index++];
        INDEX.set(index);

        if (b == -1) {
            phaser.arriveAndDeregister();
            return -1;
        }

        // Block if this thread has gotten the last byte of the current buffer
        // or if we have not read any bytes yet
        if (index == currentReadBytes) {
            awaitInterruptibly(() -> INDEX.set(0));
        }

        return b;
    }

    @Override
    public void close() throws IOException {
        // If caller is one of the consumer threads, arriveAndDeregister on its behalf;
        // else, close down resources
        lock.lock();
        try {
            if (Thread.currentThread()
                    .getName()
                    .startsWith(MULTIPLEX_THREAD_PREFIX)) {
                phaser.arriveAndDeregister();
                closedConsumers.add(Thread.currentThread()
                        .getName());
            } else {
                bis.close();
                service.shutdownNow();
            }
        } finally {
            lock.unlock();
        }
    }

    private String getConsumerThreadName() {
        return String.format("%s-%d-%s",
                MULTIPLEX_THREAD_PREFIX,
                factoryIdx.getAndIncrement(),
                UUID.randomUUID()
                        .toString());
    }

    private void awaitInterruptibly(Runnable postwaitTask) throws RuntimeException {
        try {
            if (waitSeconds > 0) {
                phaser.awaitAdvanceInterruptibly(phaser.arrive(), DEFAULT_AWAIT, TimeUnit.SECONDS);
            } else {
                phaser.awaitAdvanceInterruptibly(phaser.arrive());
            }

            if (postwaitTask != null) {
                postwaitTask.run();
            }
        } catch (InterruptedException | TimeoutException e) {
            phaser.forceTermination();
            throw new RuntimeException("Error processing split input", e);
        }
    }
}
