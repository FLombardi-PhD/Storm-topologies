package at.illecker.sentistorm.bolt;

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
			// try to parse json; if some quotations there exist, replace them and try to parse again
			try {
				json = new JSONObject(input.getString(0));
			} catch (JSONException e) {
				logger.warn("Reparsing tweet " + input.getString(0));
				String inputNoQuotations = input.getString(0).replaceAll("\"", "'");
				String inputBodyQuotations = inputNoQuotations
						.replaceFirst("'body':", "\"body\":")
						.replace("['","[\"").replace("']", "\"]");
				json = new JSONObject(inputBodyQuotations);
			}
			String body = json.getString("body");
			body = body.substring(2, body.length() - 2); // remove [" and "]
			String words[] = body.split("[ ,.]+"); // split at (and remove) spaces, commas and points
			for (String word : words)
				collector.emit(input, new Values(word));
		} catch (JSONException e) {
			logger.error("Error parsing tweet " + input.getString(0));
			// TODO modifica aggiunte due righe per riportare failure
			collector.reportError(e);
			collector.fail(input);
		} finally {
			collector.ack(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
