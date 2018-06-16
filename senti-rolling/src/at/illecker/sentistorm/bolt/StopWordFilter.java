package at.illecker.sentistorm.bolt;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StopWordFilter extends BaseRichBolt {
	
	private static final long serialVersionUID = 8714336818478825643L;
	
	private Logger logger;
	private Map<String, String> stopWordMap;
	private OutputCollector collector;
	
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		logger = Logger.getLogger(StopWordFilter.class);
		this.collector = collector;
		stopWordMap = new HashMap<String, String>();
		
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("stop-words_italian.txt")));
			String word;
			while ((word = br.readLine()) != null)
				stopWordMap.put(word, word);
			br.close();
		} catch (Exception e) {
			logger.error("Error reading stop word list from file", e);
		}
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		if (stopWordMap.get(word) == null)
			collector.emit(input, new Values(word));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
