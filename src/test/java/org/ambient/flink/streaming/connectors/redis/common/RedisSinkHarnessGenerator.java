package org.ambient.flink.streaming.connectors.redis.common;

import java.io.Closeable;

import org.apache.flink.streaming.api.operators.StreamSink;
import org.ambient.flink.streaming.connectors.redis.RedisSink;
import org.ambient.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.ambient.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

public class RedisSinkHarnessGenerator<IN> implements Closeable {
	private OneInputStreamOperatorTestHarness<IN, Object> harness = null;

    public OneInputStreamOperatorTestHarness<IN, Object> getHarness(FlinkJedisPoolConfig config, RedisMapper<IN> mapper) throws Exception {
        RedisSink<IN> sink = new RedisSink<>(config, mapper);
		OneInputStreamOperatorTestHarness<IN, Object> harness = new OneInputStreamOperatorTestHarness<>(new StreamSink<IN>(sink));
	    harness.open();
        return harness;
    }
    public void close() {
        try {
            harness.close();
        } 
        catch(Exception e) {}
    }
}
