package cz.vutbr.fit.nlpstorm.topologies;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

import cz.vutbr.fit.nlpstorm.bolts.BloomBolt;
import cz.vutbr.fit.nlpstorm.bolts.DedupBolt;
import cz.vutbr.fit.nlpstorm.bolts.FilterBolt;
import cz.vutbr.fit.nlpstorm.bolts.ParseBolt;
import cz.vutbr.fit.nlpstorm.bolts.TagBolt;
import cz.vutbr.fit.nlpstorm.bolts.TokenizerBolt;
import cz.vutbr.fit.nlpstorm.spouts.WarcSpout;


/**
 * Distributed NLPStorm topology
 * @author ikouril
 */
public class NLPStormTopologyDistr {
	
	public static void main(String[] params) throws JSAPException, AlreadyAliveException, InvalidTopologyException{
		SimpleJSAP jsap = new SimpleJSAP( NLPStormTopologyDistr.class.getName(), "Processes commoncrawl documents.",
				new Parameter[] {
					new FlaggedOption( "blockSize", JSAP.INTEGER_PARSER, "10", JSAP.NOT_REQUIRED, 'b', "blockSize", "Number of documents to be processed in one block." ),
					new FlaggedOption( "parallelism", JSAP.INTEGER_PARSER, "30", JSAP.NOT_REQUIRED, 'p', "paralelism", "Number parallel bolts." ),
					new FlaggedOption( "files", JSAP.STRING_PARSER, "athena1.fit.vutbr.cz/athena1.warcfiles;athena2.fit.vutbr.cz/athena2.warcfiles;athena3.fit.vutbr.cz/athena3.warcfiles;athena4.fit.vutbr.cz/athena4.warcfiles;athena5.fit.vutbr.cz/athena5.warcfiles;athena6.fit.vutbr.cz/athena6.warcfiles;knot01.fit.vutbr.cz/knot01.warcfiles;knot02.fit.vutbr.cz/knot02.warcfiles;knot03.fit.vutbr.cz/knot03.warcfiles;knot04.fit.vutbr.cz/knot04.warcfiles;knot05.fit.vutbr.cz/knot05.warcfiles;knot06.fit.vutbr.cz/knot06.warcfiles;knot07.fit.vutbr.cz/knot07.warcfiles;knot08.fit.vutbr.cz/knot08.warcfiles;knot10.fit.vutbr.cz/knot10.warcfiles;knot11.fit.vutbr.cz/knot11.warcfiles;knot12.fit.vutbr.cz/knot12.warcfiles;knot13.fit.vutbr.cz/knot13.warcfiles;knot14.fit.vutbr.cz/knot14.warcfiles;knot15.fit.vutbr.cz/knot15.warcfiles;knot16.fit.vutbr.cz/knot16.warcfiles;knot17.fit.vutbr.cz/knot17.warcfiles;knot18.fit.vutbr.cz/knot18.warcfiles;knot19.fit.vutbr.cz/knot19.warcfiles;knot20.fit.vutbr.cz/knot20.warcfiles;knot21.fit.vutbr.cz/knot21.warcfiles;knot22.fit.vutbr.cz/knot22.warcfiles;knot23.fit.vutbr.cz/knot23.warcfiles;knot24.fit.vutbr.cz/knot24.warcfiles;knot25.fit.vutbr.cz/knot25.warcfiles", JSAP.NOT_REQUIRED, 'f', "Adresses of warc files." )
				}
		);
		
		//parse arguments
		JSAPResult jsapResult = jsap.parse( params );
		
		NLPStormTopology.BLOCK=jsapResult.getInt("blockSize");
		NLPStormTopology.PARALLELISM=jsapResult.getInt("parallelism");
		//NLPStormTopology.PARALLELISM should be equal to length of files array
		NLPStormTopology.FILES=jsapResult.getString("files").split(";");
		
		
		Logger logger = LoggerFactory.getLogger(NLPStormTopology.class);
        logger.debug("TOPOLOGY START");
        
        String deploymentId=UUID.randomUUID().toString();
        
        logger.info("Topology configuraed with given deploymentID: "+deploymentId);

        WarcSpout warc=new WarcSpout(NLPStormTopology.BLOCK,deploymentId);
        FilterBolt filter=new FilterBolt(deploymentId);
        TokenizerBolt tokenizer=new TokenizerBolt(deploymentId);
        DedupBolt dedup=new DedupBolt(deploymentId);
        BloomBolt bloom=new BloomBolt(deploymentId);
        TagBolt tag=new TagBolt(deploymentId);
        ParseBolt parse=new ParseBolt(deploymentId);
        

        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("warc_spout", warc, NLPStormTopology.PARALLELISM);

        builder.setBolt("filter_bolt", filter, NLPStormTopology.PARALLELISM).shuffleGrouping("warc_spout", "warc");
        builder.setBolt("tokenizer_bolt", tokenizer,NLPStormTopology.PARALLELISM).shuffleGrouping("filter_bolt", "filter");
        builder.setBolt("dedup_bolt",dedup,NLPStormTopology.PARALLELISM).shuffleGrouping("tokenizer_bolt", "tokenizer");
        builder.setBolt("bloom_bolt", bloom, NLPStormTopology.PARALLELISM).fieldsGrouping("dedup_bolt", "hashes", new Fields("hash"));
        builder.setBolt("tag_bolt", tag, NLPStormTopology.PARALLELISM)
        	.fieldsGrouping("bloom_bolt", "bloom", new Fields("id"))
        	.fieldsGrouping("dedup_bolt", "documents", new Fields("id"));
        builder.setBolt("parse_bolt", parse,NLPStormTopology.PARALLELISM).shuffleGrouping("tag_bolt","tag");
        
        
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(60);
        

        StormSubmitter.submitTopology("nlpstorm",conf, builder.createTopology());
		
		
	}

}
