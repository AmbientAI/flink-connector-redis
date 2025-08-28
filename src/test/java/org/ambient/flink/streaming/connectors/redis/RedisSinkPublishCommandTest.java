/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ambient.flink.streaming.connectors.redis;

import org.ambient.flink.streaming.connectors.redis.common.RedisSinkHarnessGenerator;
import org.ambient.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.ambient.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.ambient.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.ambient.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.NetUtils.Port;

import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import redis.clients.jedis.JedisPubSub;
import redis.embedded.RedisServer;

import static org.apache.flink.util.NetUtils.getAvailablePort;
import static org.junit.Assert.assertEquals;

public class RedisSinkPublishCommandTest {

	private static final String REDIS_CHANNEL = "CHANNEL";

	private static final List<String> sourceList = new ArrayList<>();
	private Thread sinkThread;
	private PubSub pubSub;

	private static final Port redisPortGenerator = getAvailablePort();
	public static final int REDIS_PORT = redisPortGenerator.getPort();
	public static final String REDIS_HOST = "127.0.0.1";

	private static RedisServer redisServer;

	@BeforeClass
	public static void createRedisServer() throws IOException, InterruptedException {
		redisServer = new RedisServer(REDIS_PORT);
		redisServer.start();
	}

	@AfterClass
	public static void stopRedisServer(){
		try {
		redisServer.stop();
		redisPortGenerator.close();
		} catch(Exception e) {}
	}

	@Before
	public void before() throws Exception {
		pubSub = new PubSub();
		sinkThread = new Thread(new Subscribe(pubSub));
	}

	@Test
	public void redisSinkTest() throws Exception {
		sinkThread.start();
		FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
			.setHost(REDIS_HOST)
			.setPort(REDIS_PORT).build();

		RedisSinkHarnessGenerator<String> harnessGenerator = new RedisSinkHarnessGenerator<>();
		OneInputStreamOperatorTestHarness<String, Object> harness = harnessGenerator.getHarness(jedisPoolConfig, new RedisTestMapper());
		harness.processElement("Redis Sink Test", 0);

		// sleep so that the sink thread processes the pushed element
		Thread.sleep(100);

		assertEquals(1, sourceList.size());
		harnessGenerator.close();
	}

	@After
	public void after() throws Exception {
		pubSub.unsubscribe();
		sinkThread.join();
		sourceList.clear();
	}

	private class Subscribe implements Runnable {
		private PubSub localPubSub;
		private Subscribe(PubSub pubSub){
			this.localPubSub = pubSub;
		}

		@Override
		public void run() {
			try(JedisPool pool = new JedisPool(REDIS_HOST, REDIS_PORT)) {
				pool.getResource().subscribe(localPubSub, REDIS_CHANNEL);
			}
		}
	}


	public static class PubSub extends JedisPubSub {

		@Override
		public void onMessage(String channel, String message) {
			sourceList.add(message);
		}

	}

	private static class RedisTestMapper implements RedisMapper<String>{

		@Override
		public RedisCommandDescription getCommandDescription() {
			return new RedisCommandDescription(RedisCommand.PUBLISH);
		}

		@Override
		public String getKeyFromData(String data) {
			return REDIS_CHANNEL;
		}

		@Override
		public String getValueFromData(String data) {
			return data;
		}
	}
}
