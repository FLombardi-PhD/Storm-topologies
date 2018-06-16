/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package at.illecker.sentistorm.bolt;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import at.illecker.sentistorm.components.Tokenizer;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TokenizerBolt extends BaseRichBolt {
	public static final String ID = "tokenizer-bolt";
	public static final String CONF_LOGGING = ID + ".logging";
	private static final long serialVersionUID = -2447717633925641497L;

	private static final Logger LOG = Logger.getLogger(TokenizerBolt.class);
	private boolean m_logging = false;

	private OutputCollector collector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// key of output tuples
		declarer.declare(new Fields("text", "tokens"));
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map config, TopologyContext context, OutputCollector collector) {

		this.collector = collector;

		// Optional set logging
		if (config.get(CONF_LOGGING) != null) {
			m_logging = (Boolean) config.get(CONF_LOGGING);
		} else {
			m_logging = false;
		}

	}

	@Override
	public void execute(Tuple tuple) {
		String text = tuple.getStringByField("text");

		List<String> tokens = Tokenizer.tokenize(text);

		if (m_logging) {
			LOG.info("Tweet: \"" + text + "\" Tokenized: " + tokens);
		}

		// Emit and ack new tuples
		collector.emit(tuple, new Values(text, tokens));
		collector.ack(tuple);
	}

}
