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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

import static org.apache.flink.util.NetUtils.getAvailablePort;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

public class RedisSinkCommandsTest {

	private FlinkJedisPoolConfig jedisPoolConfig;
	private static final Long NUM_ELEMENTS = 1L;
	private static final String REDIS_KEY = "TEST_KEY";
	private static final String REDIS_ADDITIONAL_KEY = "TEST_ADDITIONAL_KEY";



	private Jedis jedis;

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
	public void setUp(){
		jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
			.setHost(REDIS_HOST)
			.setPort(REDIS_PORT).build();
		jedis = new Jedis(REDIS_HOST, REDIS_PORT);
	}

	@Test
	public void testRedisListDataType() throws Exception {
		RedisSinkHarnessGenerator<String> harnessGenerator = new RedisSinkHarnessGenerator<>();
		OneInputStreamOperatorTestHarness<String, Object> harness = harnessGenerator.getHarness(jedisPoolConfig, new RedisCommandMapper(RedisCommand.LPUSH));
		harness.processElement("Test Redis List Data Type", 0);

		assertEquals(NUM_ELEMENTS, jedis.llen(REDIS_KEY));
		assertEquals("Test Redis List Data Type", jedis.lindex(REDIS_KEY, 0));

		jedis.del(REDIS_KEY);
		harnessGenerator.close();
	}

	@Test
	public void testRedisSetDataType() throws Exception {
		RedisSinkHarnessGenerator<String> harnessGenerator = new RedisSinkHarnessGenerator<>();
		OneInputStreamOperatorTestHarness<String, Object> harness = harnessGenerator.getHarness(jedisPoolConfig, new RedisCommandMapper(RedisCommand.SADD));
		harness.processElement("Test Redis Set Data Type", 0);

		assertEquals(NUM_ELEMENTS, jedis.scard(REDIS_KEY));

		jedis.del(REDIS_KEY);
		harnessGenerator.close();
	}

	@Test
	public void testRedisHyperLogLogDataType() throws Exception {
		RedisSinkHarnessGenerator<String> harnessGenerator = new RedisSinkHarnessGenerator<>();
		OneInputStreamOperatorTestHarness<String, Object> harness = harnessGenerator.getHarness(jedisPoolConfig, new RedisCommandMapper(RedisCommand.PFADD));
		harness.processElement("Test Redis Hyper Log Log Data Type", 0);

		assertEquals(NUM_ELEMENTS, Long.valueOf(jedis.pfcount(REDIS_KEY)));

		jedis.del(REDIS_KEY);
		harnessGenerator.close();
	}

	@Test
	public void testRedisSortedSetDataType() throws Exception {
		RedisSinkHarnessGenerator<String> harnessGenerator = new RedisSinkHarnessGenerator<>();
		OneInputStreamOperatorTestHarness<String, Object> harness = harnessGenerator.getHarness(jedisPoolConfig, new RedisSortedSetDataMapper(RedisCommand.ZADD));
		harness.processElement("Test Redis Sorted Set Data Type", 0);

		assertEquals(NUM_ELEMENTS, jedis.zcard(REDIS_ADDITIONAL_KEY));

		jedis.del(REDIS_ADDITIONAL_KEY);
		harnessGenerator.close();
	}

	@Test
	public void testRedisHashDataType() throws Exception {
				RedisSinkHarnessGenerator<String> harnessGenerator = new RedisSinkHarnessGenerator<>();
		OneInputStreamOperatorTestHarness<String, Object> harness = harnessGenerator.getHarness(jedisPoolConfig, new RedisHashDataMapper(RedisCommand.HSET));
		harness.processElement("Test Redis Hash Data Type", 0);

		assertEquals(NUM_ELEMENTS, jedis.hlen(REDIS_ADDITIONAL_KEY));

		jedis.del(REDIS_ADDITIONAL_KEY);
		harnessGenerator.close();
	}

	@After
	public void tearDown(){
		if(jedis != null){
			jedis.close();
		}
	}

	public static class RedisCommandMapper implements RedisMapper<String>{

		private RedisCommand redisCommand;

		public RedisCommandMapper(RedisCommand redisCommand){
			this.redisCommand = redisCommand;
		}

		@Override
		public RedisCommandDescription getCommandDescription() {
			return new RedisCommandDescription(redisCommand);
		}

		@Override
		public String getKeyFromData(String data) {
			return REDIS_KEY;
		}

		@Override
		public String getValueFromData(String data) {
			return data;
		}
	}

	public static class RedisSortedSetDataMapper implements RedisMapper<String>{

		private RedisCommand redisCommand;

		public RedisSortedSetDataMapper(RedisCommand redisCommand){
			this.redisCommand = redisCommand;
		}

		@Override
		public RedisCommandDescription getCommandDescription() {
			return new RedisCommandDescription(redisCommand, REDIS_ADDITIONAL_KEY);
		}

		@Override
		public String getKeyFromData(String data) {
			return data;
		}

		@Override
		public String getValueFromData(String data) {
			return "1";
		}
	}

		public static class RedisHashDataMapper implements RedisMapper<String>{

		private RedisCommand redisCommand;

		public RedisHashDataMapper(RedisCommand redisCommand){
			this.redisCommand = redisCommand;
		}

		@Override
		public RedisCommandDescription getCommandDescription() {
			return new RedisCommandDescription(redisCommand, REDIS_ADDITIONAL_KEY);
		}

		@Override
		public String getKeyFromData(String data) {
			return "1";
		}

		@Override
		public String getValueFromData(String data) {
			return data;
		}
	}
}
