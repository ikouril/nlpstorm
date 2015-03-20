package cz.vutbr.fit.nlpstorm.spouts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.jwat.common.HeaderLine;
import org.jwat.common.HttpHeader;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cz.vutbr.fit.monitoring.Monitoring;
import cz.vutbr.fit.nlpstorm.topologies.NLPStormTopology;
import cz.vutbr.fit.nlpstorm.util.Document;
import de.l3s.boilerpipe.extractors.ArticleExtractor;


public class WarcSpout extends BaseRichSpout{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3171275129739579120L;
	private SpoutOutputCollector collector;
	private static final Logger log = LoggerFactory.getLogger(WarcSpout.class);

	private int blockSize;
	private int spoutId;
	WarcReader warcReader;
	private static int spoutCounter=0;
	String hostname;
	private Monitoring monitor;
	private Queue<String> files=new LinkedList<String>();
	String prefix;
	boolean isRunning=true;
	static final Pattern pattern=Pattern.compile("<head>.*?<title>(.*?)</title>.*?</head>",Pattern.DOTALL|Pattern.CASE_INSENSITIVE);

	
	/**
	 * Creates a new WarcSpout.
	 * @param id the id of actual nlpstorm run
	 */
	public WarcSpout(int blockSize,String id){
		this.blockSize=blockSize;
		try {
			monitor=new Monitoring(id, "knot28.fit.vutbr.cz", "nlpstorm", "nlpstormdb88pass", "nlpstormdb");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector=collector;
		spoutId=spoutCounter;
		spoutCounter+=1;
		log.info("Warc spout id: "+String.valueOf(spoutId));
		
		
		BufferedReader reader=null;
		try {
			String path="http://"+NLPStormTopology.FILES[spoutId];
			reader=new BufferedReader(new InputStreamReader(new URL(path).openStream()));
			prefix=path.substring(0, path.lastIndexOf('/'))+"/warcfiles/";
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String line=null;
		try {
			while ((line=reader.readLine())!=null){
				files.add(line);	
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		
		try{
			hostname=InetAddress.getLocalHost().getHostName();
		}
		catch(UnknownHostException e){
			hostname="-unknown-";
		}
		
		newDump();
	}
	
	private boolean newDump(){
		
		String file=files.poll();
		
		if (file==null)
			return false;
		
		try {
			InputStream gzipStream=new GZIPInputStream(new URL(prefix+file).openStream());
			warcReader = WarcReaderFactory.getReader( gzipStream );
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return true;
	}
	

	@Override
	public void nextTuple() {
		long startTime = System.nanoTime();
		log.info("Entering warc spout");
		
		if (!isRunning)
			return;
		String id=UUID.randomUUID().toString();
		List<Document> blockRecords=new ArrayList<Document>();
		
        try {
        	while (blockRecords.size()!=blockSize){
        	
			 WarcRecord record=warcReader.getNextRecord();
			 
			 if (record==null){
				 if (!newDump()){
					 
					 if (blockRecords.size()>0){
						 Long estimatedTime = System.nanoTime() - startTime;
						 
						 try {
								monitor.MonitorTuple("WarcSpout", id, blockRecords.size(),hostname, estimatedTime);
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						collector.emit("warc",new Values(id,blockRecords));
					 }
					 isRunning=false;
					 return;
				 }
				 else{
					 record=warcReader.getNextRecord();
				 }
			 }
			 
			 InputStream payload=record.getPayloadContent();
             BufferedReader rdr=new BufferedReader(new InputStreamReader(payload,"UTF-8"));
             HttpHeader header=record.getHttpHeader();
             String contentType="unknown";
             if (header!=null){
            	 if (header.contentType!=null)
            		 contentType=header.contentType;
             }
             String line=rdr.readLine();
             if (line!=null){
	             boolean isHtml=line.startsWith("<!DOCTYPE html") || contentType.indexOf("text/html")>=0;
	             if (isHtml){
		             StringBuilder sb=new StringBuilder();
	            	 sb.append(line);
	            	 sb.append("\n");
		             while (line!=null){
		            	 sb.append(line);
		            	 sb.append("\n");
		            	 line=rdr.readLine();
		             }
		             String data=sb.toString();
		             boolean isOK=true;
		             String res=null;
		             if (data.length()>0){
			             
			             try{
			            	 res=ArticleExtractor.INSTANCE.getText(data);
			             }
			             catch (Exception e){
			            	 isOK=false;
			             }
		             }
		             if (isOK && res!=null && !res.isEmpty()){
		            	 Document d=new Document(res);
		            	 
		            	 String uri="Unknown";
			             String title="Unknown";
			             HeaderLine l=record.getHeader("WARC-Target-URI");
			             
			             if (l!=null){
			            	 uri=l.value;
			             }
			             Matcher m=pattern.matcher(data);
			             if (m.find()){
			            	 title=m.group(1).replace("\"","'").replace("\n","");
			             }
			             d.setTitle(title);
			             d.setUri(uri);
		            	 d.setId(id+"-"+String.valueOf(blockRecords.size()));
		            	 blockRecords.add(d);
		             }
	             }
             }
           }
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
         Long estimatedTime = System.nanoTime() - startTime;

		 try {
				monitor.MonitorTuple("WarcSpout", id, blockRecords.size(),hostname, estimatedTime);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		 collector.emit("warc",new Values(id,blockRecords));

	}	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("warc", new Fields("id","documents"));
		
	}
}
