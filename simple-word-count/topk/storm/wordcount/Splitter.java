package storm.wordcount;

import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Splitter extends BaseRichBolt {

	private static final long serialVersionUID = 6085751458188690203L;
	
	private Logger logger;
	
	private OutputCollector collector;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		logger = Logger.getLogger(Splitter.class);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	@Override
	public void execute(Tuple input) {
		try {
			String wordList[] = input.getString(0).split("[\\p{P} \\t\\n\\r]");
			for (String word : wordList)
				if (word.length() > 0)
					collector.emit(input, new Values(word.toLowerCase()));
			collector.ack(input);
		} catch (Exception e) {
			logger.error("Error splitting tuple " + input.toString(), e);
		}
	}

}

