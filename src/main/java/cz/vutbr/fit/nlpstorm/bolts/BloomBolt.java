package cz.vutbr.fit.nlpstorm.bolts;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cz.vutbr.fit.monitoring.Monitoring;
import cz.vutbr.fit.nlpstorm.util.LongFastBloomFilter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BloomBolt implements IRichBolt {
	
	private static final Logger log = LoggerFactory.getLogger(BloomBolt.class);
	OutputCollector collector;
	String hostname;
	Monitoring monitor;
	long expectedNumberOfElements = 150000000;
    double falsePosProb = .01;
    LongFastBloomFilter longFastBloomFilter = LongFastBloomFilter.getFilter(expectedNumberOfElements, falsePosProb);
	
	public BloomBolt(String id){
		try {
			monitor=new Monitoring(id, "knot28.fit.vutbr.cz", "nlpstorm", "nlpstormdb88pass", "nlpstormdb");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static byte[] longToByteArray(long value) {
	    return new byte[] {
	        (byte) (value >> 56),
	        (byte) (value >> 48),
	        (byte) (value >> 40),
	        (byte) (value >> 32),
	        (byte) (value >> 24),
	        (byte) (value >> 16),
	        (byte) (value >> 8),
	        (byte) value
	    };
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector=collector;
		try{
			hostname=InetAddress.getLocalHost().getHostName();
		}
		catch(UnknownHostException e){
			hostname="-unknown-";
		}
		
		
	}

	@Override
	public void execute(Tuple input) {
		long startTime = System.nanoTime();
		log.info("Entering bloom bolt");
		
		long hash=(Long) input.getValue(0);
		String id=(String) input.getValue(1);
		String docId=(String) input.getValue(2);
		int parPos=(Integer) input.getValue(3);

		byte[] byteHash=longToByteArray(hash);
		
		if (longFastBloomFilter.contains(byteHash)){
			Long estimatedTime = System.nanoTime() - startTime;
			 
			 try {
					monitor.MonitorTuple("BloomBolt", id, 1,hostname, estimatedTime);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			 collector.emit("bloom",new Values(id,docId,parPos,true));
		}
		else{
			longFastBloomFilter.add(byteHash);
			Long estimatedTime = System.nanoTime() - startTime;
			 
			 try {
					monitor.MonitorTuple("BloomBolt", id, 1,hostname, estimatedTime);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			 collector.emit("bloom",new Values(id,docId,parPos,false));//include paragraph to result
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("bloom", new Fields("id","doc_id","par_pos","duplicate"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
