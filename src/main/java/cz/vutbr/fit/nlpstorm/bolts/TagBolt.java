package cz.vutbr.fit.nlpstorm.bolts;


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.annolab.tt4j.TokenHandler;
import org.annolab.tt4j.TreeTaggerException;
import org.annolab.tt4j.TreeTaggerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.vutbr.fit.nlpstorm.util.Tags;
import cz.vutbr.fit.monitoring.Monitoring;
import cz.vutbr.fit.nlpstorm.util.Document;
import cz.vutbr.fit.nlpstorm.util.LongFastBloomFilter;
import cz.vutbr.fit.nlpstorm.util.ParagraphDuplicate;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cz.vutbr.fit.nlpstorm.util.DocumentHolder;
import edu.northwestern.at.utils.StringUtils;

/**
 * A bolt for tagging incoming documents with deduplicated paragraphs.
 * Bolt is responsible for maintaining document-paragraph cache (data need to be cached until every part of document went through deduplication process)
 * Accepts: List of documents and deduplicated paragraph hashes to be linked back together
 * Emits: List of tagged documents
 * @author ikouril
 */
public class TagBolt implements IRichBolt {
	
	private static final Logger log = LoggerFactory.getLogger(TagBolt.class);
	OutputCollector collector;
	String hostname;
	Monitoring monitor;
	TreeTaggerWrapper<String> tt;
	Map<String,DocumentHolder> data=new HashMap<String,DocumentHolder>();
	
	int tagCounter;
	int processingCounter;
	List<String> docBuffer=new ArrayList<String>();
	Map<Integer,Tags> tags=new HashMap<Integer,Tags>();
	
	StringBuilder actualTags=new StringBuilder();

	/**
     * Creates a new TagBolt.
     * @param id the id of actual nlpstorm run
     */
	public TagBolt(String id){
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
		//make sure TreeTagger is installed under given path
		System.setProperty("treetagger.home", "/opt/TreeTagger");
	    tt = new TreeTaggerWrapper<String>();
	    
	    try {
	    	//english parameter file and encoding for TreeTagger
			tt.setModel("/opt/TreeTagger/lib/english-utf8.par:utf8");
			tt.setHandler(new TokenHandler<String>() {
	        public void token(String token, String pos, String lemma) {
	        	if (tags.containsKey(processingCounter)){
		        	  Tags t=tags.get(processingCounter);
		        	  actualTags.append(lemma+"\t"+token + "\t"+t.before+"\t"+t.after+"\t"+pos+"\n");
		        	  if (t.after.indexOf("</s>")>=0)
		        		  actualTags.append("\n");
		        	  else if (t.after.indexOf("</head>")>=0)
		        		  actualTags.append("\n");
		          }
		          else{
		        	  actualTags.append(lemma+"\t"+token + "\t_\t_\t"+pos+"\n");
		          }
	          processingCounter++;
	        }
	      });
	    } catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	

	@Override
	public void execute(Tuple input) {
		long startTime = System.nanoTime();
		log.info("Entering tag bolt");
		String id=(String) input.getValue(0);
		
		List<Document> results=new ArrayList<Document>();
		
		if (input.getSourceStreamId().equals("bloom")){
			
			String docId=(String) input.getValue(1);
			int parPos=(Integer) input.getValue(2);
			boolean isDuplicate=(Boolean) input.getValue(3);
			
			DocumentHolder holder=data.get(docId);
			
			ParagraphDuplicate dup=new ParagraphDuplicate(parPos, isDuplicate, docId);
			
			if (holder!=null){
				holder.addParagraph(dup);
			}
			else{
				holder=new DocumentHolder(dup);
				data.put(docId, holder);
			}
			
			if (holder.isFinalized()){
				data.remove(docId);
				Document doc=holder.getDocument();
				List<String> tokens=doc.toTokens(tags);
				processingCounter=0;
				try {
					tt.process(tokens);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TreeTaggerException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				doc.clear();
				String result=actualTags.toString();
				doc.setContent(result);
				actualTags.setLength(0);
				tags.clear();
				results.add(doc);
			}
		}
		else{
			List<Document> block=(List<Document>) input.getValue(1);
			
			
			for (Document d:block){
				String docId=d.getId();
				DocumentHolder holder=data.get(docId);
				
				if (holder!=null){
					holder.addDocument(d);
				}
				else{
					holder=new DocumentHolder(d);
					data.put(docId, holder);
				}
				
				if (holder.isFinalized()){
					data.remove(docId);
					Document doc=holder.getDocument();
					processingCounter=0;
					try {
						tt.process(doc.toTokens(tags));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (TreeTaggerException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					doc.clear();
					tags.clear();
					doc.setContent(actualTags.toString());
					actualTags.setLength(0);
					results.add(doc);
				}
			}
		}
		
		int resultsSize=results.size();
		if (resultsSize>0){
			Long estimatedTime = System.nanoTime() - startTime;
			try {
				monitor.MonitorTuple("TagBolt", id, resultsSize,hostname, estimatedTime);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		 collector.emit("tag",new Values(id,results));
			
		}
		

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("tag", new Fields("id","tagged"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
