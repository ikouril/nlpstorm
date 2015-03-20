package cz.vutbr.fit.nlpstorm.bolts;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cz.vutbr.fit.monitoring.Monitoring;
import cz.vutbr.fit.nlpstorm.util.Document;
import cz.vutbr.fit.nlpstorm.util.LongFastBloomFilter;


public class DedupBolt implements IRichBolt {
	
	private static final Logger log = LoggerFactory.getLogger(DedupBolt.class);
	OutputCollector collector;
	String hostname;
	Monitoring monitor;
	

	
	
	public DedupBolt(String id){
		try {
			monitor=new Monitoring(id, "knot28.fit.vutbr.cz", "nlpstorm", "nlpstormdb88pass", "nlpstormdb");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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
		log.info("Entering dedup bolt");
		String id=(String) input.getValue(0);
		List<Document> block=(List<Document>) input.getValue(1);

		for (Document d:block){
			d.computeHashes();
		}
		

		 Long estimatedTime = System.nanoTime() - startTime;
		 
		 try {
				monitor.MonitorTuple("DedupBolt", id, block.size(),hostname, estimatedTime);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		 collector.emit("documents",new Values(id,block));
		
		 for (Document d:block){
			 	int i=0;
				for (long hash:d.getHashes()){
					collector.emit("hashes",new Values(hash,id,d.getId(),i));
					i++;
				}
		 }
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("documents", new Fields("id","hashed"));
		declarer.declareStream("hashes", new Fields("hash","chunk_id","doc_id","par_pos"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}

