package storm.wordcount;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StopWordFilter extends BaseRichBolt {

	private static final long serialVersionUID = 4061283311472978590L;
	
	private Logger logger;
	private OutputCollector collector;
	private Set<String> stopWordSet;
	
	public StopWordFilter() {
		stopWordSet = new HashSet<String>();
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		logger = Logger.getLogger(StopWordFilter.class);
		this.collector = collector;
		
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("stop-words_italian.txt")));
			String word;
			while ((word = br.readLine()) != null)
				stopWordSet.add(word);
			br.close();
		} catch (Exception e) {
			logger.error("Error loading stop words", e);
		}
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		if (!stopWordSet.contains(word) && word.length() > 1)
			collector.emit(input, new Values(word));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}

