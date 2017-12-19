package ru.ivi;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.orc.OrcTableSource;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;

/**
 * Implements the "Test" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink program.
 * <li>use Tuple data types.
 * <li>write and use user-defined functions.
 * </ul>
 *
 */
public class Test {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
		Configuration conf = new Configuration();
		String fileName = "hdfs://master.host/data/groot.db/events/dt=2017-12-01/subsite=1013/000085_0";

		conf.set("fs.defaultFS","hdfs://master.host");

		OrcTableSource orcTableSource = OrcTableSource.builder()
				.path(fileName)
				.forOrcSchema(orcSchemaEvents)
				.withConfiguration(conf)
				.build();

		tEnv.registerTableSource("events", orcTableSource);

		Table events = tEnv.sqlQuery("select * from events");

		events.writeToSink(new CsvTableSink(
				"/tmp/table_res",
				"|"));

		env.execute();
	}

	public static String orcSchemaEvents = "struct<id:bigint," +
			"name:string," +
			"subsite_id:int," +
			"user_id:string," +
			"ivi_id:bigint," +
			"ts:timestamp," +
			"lib:string," +
			"added:timestamp," +
			"browser:string," +
			"browser_version:string," +
			"flash_version:string," +
			"os:string," +
			"os_version:string," +
			"screen:string," +
			"referrer_domain:string," +
			"url:string," +
			"carrier:string," +
			"manufacturer:string," +
			"device_model:string," +
			"brand:string," +
			"app_version:int," +
			"watch_id:string," +
			"watch_id_hash:string," +
			"content_id:int," +
			"compilation_id:int," +
			"g_source:string," +
			"g_campaign:string," +
			"g_medium:string," +
			"g_term:string," +
			"g_content:string," +
			"payment_id:int," +
			"payment_method:string," +
			"monetization_avod:int," +
			"monetization_est:int," +
			"monetization_tvod:int," +
			"monetization_svod:int," +
			"purchase_id:int," +
			"credit_id:int," +
			"duration:int," +
			"adv_video_id:int," +
			"mnc:int," +
			"mcc:int," +
			"radio:string," +
			"abt:string," +
			"client_time:timestamp," +
			"block_id:string," +
			"page:string," +
			"ref_page:string," +
			"is_personalizable:int," +
			"svod_active:int," +
			"collection_id:int," +
			"from:string," +
			"authorizeduser:int," +
			"hascard:int," +
			"source:string," +
			"show_duration:bigint," +
			"place_id:int" +
			">";


}
