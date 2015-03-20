package cz.vutbr.fit.nlpstorm.bolts;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

import cz.vutbr.fit.monitoring.Monitoring;
import cz.vutbr.fit.nlpstorm.spouts.WarcSpout;
import cz.vutbr.fit.nlpstorm.util.Document;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FilterBolt implements IRichBolt {
	
	private static final Logger log = LoggerFactory.getLogger(FilterBolt.class);
	OutputCollector collector;
	String hostname;
	Monitoring monitor;
	
	public FilterBolt(String id){
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
		
		try {
			DetectorFactory.loadProfile("/usr/share/langdetect/langdetect-03-03-2014/profiles");
		} catch (LangDetectException e) {

		}
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
		log.info("Entering filter bolt");
		String id=(String) input.getValue(0);
		List<Document> block=(List<Document>) input.getValue(1);

		List<Document> results=new ArrayList<Document>();
		String lang = null;
		for (Document d:block){
			try{
	           	 Detector detector = DetectorFactory.create();
	           	 detector.append(d.getContent());
	           	 lang = detector.detect();
	           	 log.info("Detected language: "+lang);
	       	 }
	       	 catch (Exception e){
	       		 e.printStackTrace();
	       	 }
			
			 if (lang!=null && lang.equals("en")){
				 results.add(d);
			 }
		}
		
		if (results.size()>0){
			 Long estimatedTime = System.nanoTime() - startTime;
			 
			 try {
					monitor.MonitorTuple("FilterBolt", id, results.size(),hostname, estimatedTime);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
			 collector.emit("filter",new Values(id,results));
		}
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("filter", new Fields("id","filtered"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
