package storm.starter.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SimpleTweetReader extends BaseRichSpout {
	
	private static final long serialVersionUID = 6218280345583579250L;
	
	private final String filename;
	private BufferedReader br;
	private SpoutOutputCollector collector;
	private Logger logger;
	private long msgId;
	private boolean running;
	
	public SimpleTweetReader(String filename) {
		this.filename = filename;
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		logger = Logger.getLogger(SimpleTweetReader.class);
		running = true;
		try {
			br = new BufferedReader(new FileReader(filename));
		} catch (FileNotFoundException e) {
			logger.error("Error opening dataset from file " + filename, e);
		}
	}

	private long last;
	
	@Override
	public void nextTuple() {
		if (running) {
			// Utils.sleep(100);
			long now = System.currentTimeMillis();
			if (last == 0)
				last = now;
			if (now - last >= 100) {
				// read ts, tweet from file
				String tweet = null;
				try {
					if (br != null)
						tweet = br.readLine();
					if (tweet == null) {
						logger.info("Dataset reading completed");
						running = false;
					}
				} catch (IOException e) {
					logger.error("Error reading from file " + filename, e);
				}
				
				// remove ts and send
				if (tweet != null) {
					tweet = tweet.substring(tweet.indexOf('\t') + 1).replace('\n', ' ');
					collector.emit(new Values(tweet), msgId++);
				}
				
				last = now;
			}
			Thread.yield();
		} else {
			logger.info("Dataset reading completed, sleep for one second");
			Utils.sleep(1000);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

	@Override
    public void close() {
		if (br != null)
			try {
				br.close();
			} catch (IOException e) {
				logger.error("Error closing file " + filename, e);
			}
    }
}
