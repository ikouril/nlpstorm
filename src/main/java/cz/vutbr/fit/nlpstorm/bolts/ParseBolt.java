package cz.vutbr.fit.nlpstorm.bolts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import de.dfki.lt.mdparser.outputformat.ConllOutput;
import de.dfki.lt.mdparser.test.MDParser;

/**
 * A bolt for parsing tagged documents
 * Accepts: List of tagged documents to be tagged 
 * Emits: Nothing, writes results directly to HDD
 * @author ikouril
 */
public class ParseBolt implements IRichBolt {
	
	private static final Logger log = LoggerFactory.getLogger(ParseBolt.class);
	
	//Average number documents in one file, can be made configurable
	private static final int docsPerFile=10000;
	OutputCollector collector;
	String hostname;
	Monitoring monitor;
	
	BufferedWriter writer=null;
	int docsWritten=0;
	
	/**
     * Creates a new ParseBolt.
     * @param id the id of actual nlpstorm run
     */
	public ParseBolt(String id){
		try {
			monitor=new Monitoring(id, "knot28.fit.vutbr.cz", "nlpstorm", "nlpstormdb88pass", "nlpstormdb");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	/**
     * Prepares new file to write to, after limit of documents per file is reached
     */
	private void newFile(){
		String uuid=UUID.randomUUID().toString();
		FileWriter fstream=null;
		try {
			if (writer!=null)
				writer.close();
			
			//document output path is hardcoded, make sure path is writable
			//TODO: Make configurable
			fstream = new FileWriter("/mnt/data/stormData/"+uuid+".parsed");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    writer = new BufferedWriter(fstream);
	    docsWritten=0;
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
		newFile();
	}

	@Override
	public void execute(Tuple input) {
		long startTime = System.nanoTime();
		log.info("Entering parse bolt");
		String id=(String) input.getValue(0);
		List<Document> block=(List<Document>) input.getValue(1);
		
		MDParser parser=null;
		try {
			//Creating new parser instance each time - class is not serializable
			parser=new MDParser();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			for (Document d:block){
				if (docsWritten==docsPerFile)
					newFile();
				
				String parsed = parser.parseText(d.getContent(),"en","conll");
				writer.write(new ConllOutput(parsed).getOutput());
				docsWritten++;
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		Long estimatedTime = System.nanoTime() - startTime;
		 
		try {
			monitor.MonitorTuple("ParseBolt", id, block.size(),hostname, estimatedTime);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
