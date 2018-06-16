package storm.starter.bolt;

import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordGenerator extends BaseRichBolt {

	private static final long serialVersionUID = -6446214127432815845L;
	
	private Logger logger;
	private OutputCollector collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		logger = Logger.getLogger(WordGenerator.class);
	}

	@Override
	public void execute(Tuple input) {
		JSONObject json = null;
		try {
			json = new JSONObject(input.getString(0));
			String body = json.getString("body");
			body = body.substring(2, body.length() - 2); // remove [" and "]
			String words[] = body.split("[ ,.]+"); // split at (and remove) spaces, commas and points
			for (String word : words)
				collector.emit(input, new Values(word));
			collector.ack(input);
		} catch (JSONException e) {
			logger.error("Error parsing tweet " + input.getString(0));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
