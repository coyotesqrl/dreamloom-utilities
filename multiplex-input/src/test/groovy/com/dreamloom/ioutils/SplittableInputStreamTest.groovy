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
package com.dreamloom.ioutils

import com.google.common.io.CharStreams
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

class SplittableInputStreamTest extends Specification {
    interface StreamConsumer {
        void consume(InputStream inputStream);
    }

    class QuickConsumer implements StreamConsumer {
        private String consumedString

        public void consume(InputStream inputStream) {
            consumedString = CharStreams.toString(new InputStreamReader(inputStream))
        }

        String getString() {
            return consumedString
        }
    }

    class SlowConsumer implements StreamConsumer {
        private long sleepMillis

        private String consumedString

        SlowConsumer(long sleepMillis) {
            this.sleepMillis = sleepMillis
        }

        public void consume(InputStream inputStream) {
            def buffer = new byte[4096]
            int idx = 0;
            for (int b = inputStream.read(); b != -1; b = inputStream.read()) {
                buffer[idx++] = b;
                TimeUnit.MILLISECONDS.sleep(sleepMillis);
            }

            consumedString = new String(buffer, 0, idx, 'UTF-8')
        }

        String getString() {
            return consumedString
        }
    }

    class ClosingConsumer implements StreamConsumer {
        boolean reread = false

        def consumedString

        ClosingConsumer(boolean reread) {
            this.reread = reread
        }

        @Override
        void consume(InputStream inputStream) {
            def buffer = new byte[10]

            // Read 10 bytes
            0.upto(9, { buffer[it] = inputStream.read() })
            // and then close
            inputStream.close()

            if (reread) {
                if (inputStream.read() != -1) {
                    throw new RuntimeException('Reread after stream close should return -1')
                }
                consumedString = 'CLOSED STREAM'
            } else {
                consumedString = new String(buffer, 'UTF-8')
            }
        }

        String getString() {
            return consumedString
        }
    }

    def 'simplest single test'() {
        setup:
        def stream = new ByteArrayInputStream('hello world'.bytes)
        def splitStream = new SplittableInputStream(stream)
        def consumer = new QuickConsumer()
        def consumerList = [({ consumer.consume(it) } as Consumer<InputStream>)]

        when:
        splitStream.invoke(consumerList)

        then:
        consumer.string == 'hello world'
    }

    def 'three consumers'() {
        setup:
        def stream = new ByteArrayInputStream('hello world'.bytes)
        def splitStream = new SplittableInputStream(stream)
        def consumers = [new QuickConsumer(), new QuickConsumer()]
        def consumerList = consumers.collect {
            ({ is -> it.consume(is) } as Consumer<InputStream>)
        }

        when:
        splitStream.invoke(consumerList)

        then:
        consumers.each {
            assert it.string == 'hello world'
        }
    }

    @Unroll
    def 'test internal buffer sizes'() {
        setup:
        def splitStream = new SplittableInputStream(new ByteArrayInputStream(input.bytes))

        def consumers = []
        consumerCount.times {
            consumers.add(new QuickConsumer())
        }
        def consumerList = consumers.collect {
            ({ is -> it.consume(is) } as Consumer<InputStream>)
        }

        when:
        splitStream.invoke(consumerList)

        then:
        consumers.each {
            assert it.string == input
        }

        where:
        input              | bufferSize | consumerCount
        'a'.multiply(100)  | 100        | 1
        'a'.multiply(4096) | 4096       | 2
        'a'.multiply(2048) | 4096       | 2
        'a'.multiply(4096) | 2048       | 1
        'a'.multiply(4096) | 2048       | 4
        'a'.multiply(100)  | 17         | 2
        'a'.multiply(100)  | 1          | 1
        'a'.multiply(100)  | 1          | 3
        'a'.multiply(100)  | 99         | 2
        'a'.multiply(100)  | 101        | 2
    }

    @Unroll
    def 'test timeout for too-slow consumers'() {
        setup:
        def splitStream = new SplittableInputStream(new ByteArrayInputStream(input.bytes), 4096, 1)
        def consumers = [new QuickConsumer(), new QuickConsumer(), new SlowConsumer(pauseTime)]
        def consumerList = consumers.collect {
            ({ is -> it.consume(is) } as Consumer<InputStream>)
        }

        when:
        splitStream.invoke(consumerList)

        then:
        thrown ExecutionException

        where:
        input             | pauseTime
        'a'.multiply(100) | 700
        'a'.multiply(100) | 1000
        'a'.multiply(100) | 1200
    }

    @Unroll
    def 'test timeout for consumers with forgiving splitter'() {
        setup:
        def splitStream = new SplittableInputStream(new ByteArrayInputStream(input.bytes), 4096, 20)
        def consumers = [new QuickConsumer(), new QuickConsumer(), new SlowConsumer(pauseTime)]
        def consumerList = consumers.collect {
            ({ is -> it.consume(is) } as Consumer<InputStream>)
        }

        when:
        splitStream.invoke(consumerList)

        then:
        consumers.each {
            assert it.string == input
        }

        where:
        input            | pauseTime
        'a'.multiply(32) | 10
        'a'.multiply(32) | 20
        'a'.multiply(32) | 30
    }

    def 'test premature close'() {
        setup:
        def input = 'abc'.multiply(800)
        def splitStream = new SplittableInputStream(new ByteArrayInputStream(input.bytes), 4096)
        def consumers = [new QuickConsumer(), new QuickConsumer(), new ClosingConsumer(false)]
        def consumerList = consumers.collect {
            ({ is -> it.consume(is) } as Consumer<InputStream>)
        }

        when:
        splitStream.invoke(consumerList)

        then:
        notThrown ExecutionException
        consumers.each {
            if (it instanceof QuickConsumer) {
                assert it.string == input
            } else {
                assert it.string == input.substring(0, 10)
            }
        }

    }

    def 'test premature close with reread'() {
        setup:
        def input = 'abc'.multiply(800)
        def splitStream = new SplittableInputStream(new ByteArrayInputStream(input.bytes), 4096)
        def consumers = [new QuickConsumer(), new QuickConsumer(), new ClosingConsumer(true)]
        def consumerList = consumers.collect {
            ({ is -> it.consume(is) } as Consumer<InputStream>)
        }

        when:
        splitStream.invoke(consumerList)

        then:
        notThrown ExecutionException
        consumers.each {
            if (it instanceof QuickConsumer) {
                assert it.string == input
            } else {
                assert it.string == 'CLOSED STREAM'
            }
        }
    }
}
