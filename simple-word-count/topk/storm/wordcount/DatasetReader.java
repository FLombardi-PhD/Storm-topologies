package storm.wordcount;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class DatasetReader extends BaseRichSpout {
	
	private static final long serialVersionUID = -160600778428845924L;
	private static final int DEFAULT_INPUT_RATE = 10;
	
	private final String dataset;
	
	private final int sendDelay;
	
	private Logger logger;
	
	private BufferedReader br;
	
	private SpoutOutputCollector collector;
	
	private long msgId;
	
	private boolean running;
	
	/**
	 * timestamp of last tuple emission, used in nextTuple() to control input rate
	 */
	private long last;
	
	public DatasetReader(String dataset, int inputRate) {
		this.dataset = dataset;
		this.sendDelay = 1000 / inputRate;
	}
	
	public DatasetReader(String dataset) {
		this(dataset, DEFAULT_INPUT_RATE);
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		logger = Logger.getLogger(DatasetReader.class);
		running = true;
		
		try {
			br = new BufferedReader(new FileReader(dataset));
		} catch (FileNotFoundException e) {
			logger.error("Error opening dataset from file " + dataset, e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

	@Override
	public void nextTuple() {
		if (running) {
			long now = System.currentTimeMillis();
			if (last == 0)
				last = now;
			if (now - last >= sendDelay) {
				try {
					String line = br.readLine();
					if (line == null) {
						running = false;
					} else {
						String tweet = line.substring(line.indexOf('{'));
						JSONObject json = (JSONObject)JSONValue.parse(tweet);
						String tuple = (String)((JSONArray)json.get("body")).get(0);
						collector.emit(new Values(tuple), msgId++);
					}
				} catch (Exception e) {
					logger.error("Error reading a tweet from the dataset", e);
				}
				last = now;
			}
			Thread.yield();
		} else {
			logger.info("Finished reading dataset, sleeping for 10 seconds");
			Utils.sleep(10000);
		}
	}
	
}

