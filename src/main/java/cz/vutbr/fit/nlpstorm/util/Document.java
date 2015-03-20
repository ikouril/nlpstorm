package cz.vutbr.fit.nlpstorm.util;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

import com.clearnlp.nlp.NLPGetter;
import com.clearnlp.reader.AbstractReader;
import com.clearnlp.segmentation.AbstractSegmenter;
import com.clearnlp.tokenization.AbstractTokenizer;
import com.google.common.base.CharMatcher;

public class Document {
	
	private String content=null;
	private List<String> paragraphs=null;
	private String title=null;
	private String uri=null;
	private String id;
	private String head;
	private static AbstractTokenizer tokenizer  = NLPGetter.getTokenizer(AbstractReader.LANG_EN);
	private static AbstractSegmenter segmenter = NLPGetter.getSegmenter(AbstractReader.LANG_EN, tokenizer);
	private static XXHashFactory factory = XXHashFactory.fastestInstance();
	private static XXHash64 hasher=factory.hash64();
	private static long seed = 0x9747b28c;
	private long[] hashes;
	private int[] hashMarkers;

	public void computeHashes(){
		hashes=new long[paragraphs.size()];
		hashMarkers=new int[paragraphs.size()];
		for (int i=0;i<hashMarkers.length;i++){
			byte[] data=paragraphs.get(i).getBytes();
			hashes[i]=hasher.hash(data, 0,data.length,seed);
			hashMarkers[i]=0;//hash confirmation not yet arrived
		}
	}
	
	public void clear(){
		hashes=null;
		hashMarkers=null;
		content=null;
		paragraphs=null;
		head=null;
	}
	
	public void setMarker(int pos,boolean dupl){
		hashMarkers[pos]=dupl?1:2;
		//1 - duplicate
		//2 - no duplicate
	}
	
	public long[] getHashes(){
		return hashes;
	}
	
	public boolean isCompleted(){
		for (int i=0;i<hashMarkers.length;i++){
			if (hashMarkers[i]==0)
				return false;
		}
		return true;
	}
	
	public Document(String content){
		this.content=content;
		
	}
	
	public String getHead(){
		return head;
	}
	
	public void setId(String id){
		this.id=id;
	}
	
	public String getId(){
		return id;
	}
	
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public List<String> getParagraphs() {
		return paragraphs;
	}
	public void setParagraphs(List<String> paragraphs) {
		this.paragraphs = paragraphs;
	}
	
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}
	
	public List<String> toTokens(Map<Integer,Tags> tags){
		int tagCounter=0;
		List<String> rc=new ArrayList<String>();
		
		String line="<doc id=\""+id+"\" uri=\""+uri+"\" title=\""+title+"\">";
		rc.add(line);
		Tags t=new Tags();
		t.addBefore(line);
		tags.put(tagCounter, t);
		
		if (head!=null && !head.isEmpty()){
			
			
			t.addBefore("<head>");
			String[] headTokens=head.split("\n");
			for (String token:headTokens){
				rc.add(token);
				if (!token.startsWith("<"))
					tagCounter++;
			}
			
			t=new Tags();
			t.addAfter("</head>");
			tags.put(tagCounter-1, t);
		}
		
		for (int i=0;i<paragraphs.size();i++){
			if (hashMarkers[i]==2){
				String[] paragraphTokens=paragraphs.get(i).split("\n");
				for (String token:paragraphTokens){
					 rc.add(token);
					 if (token.startsWith("</")){
			        	 int num=tagCounter-1;
			        	 t=null;
			        	 if (tags.containsKey(num)){
			        		 t=tags.get(num);
			        		 t.addAfter(token);
			        	 }
			        	 else{
			        		 t=new Tags();
			        		 t.addAfter(token);
			        		 tags.put(num, t);
			        	 }
		        		 
		        		 
			         }
			         else if (token.startsWith("<")){
			        	 t=null;
			        	 if (tags.containsKey(tagCounter)){
			        		 t=tags.get(tagCounter);
			        		 t.addBefore(token);
			        	 }
			        	 else{
			        		 t=new Tags();
			        		 t.addBefore(token);
			        		 tags.put(tagCounter, t);
			        	 }
		        		 
			         }
			         else{
			        	 tagCounter++;
			         }
				}
			}
		}
		
		int num=tagCounter-1;
	   	t=null;
	   	if (tags.containsKey(num)){
	   		t=tags.get(num);
	   	}
	   	else{
	   		t=new Tags();
	   		tags.put(num, t);
	   	}
		t.addAfter("</doc>"); 
		rc.add("</doc>");
		return rc;
	}

	public void loadParagraphs(){
		if (!title.equals("Unknown")){
			StringBuilder headBuilder=new StringBuilder();
			boolean headExists=false;
			 List<String> head=tokenizer.getTokens(title);
			 headBuilder.append("<head>\n");
			 int offset=0;
			 for (String headWord:head){
				 if (offset!=0){
					int found=title.indexOf(headWord,offset);
					if (found==offset){
						headBuilder.append("<g/>\n");
					}
					offset=found+headWord.length();
				 }
				 else{
					 offset=headWord.length();
				 }
				 headWord = CharMatcher.JAVA_ISO_CONTROL.removeFrom(headWord);
				 headWord=headWord.replaceAll("[ \r\n\t\u3000\u00A0\u2007\u202F]","");
				 if (!headWord.isEmpty()){
					 headBuilder.append(headWord+"\n");
					 headExists=true;
				 }
			 }
			 headBuilder.append("</head>\n");
			 
			 if (headExists)
				 this.head=headBuilder.toString();
		 }
		
		 String[] paragraphs=content.split("\n");
		 this.paragraphs=new ArrayList<String>();
		 for (String paragraph:paragraphs){
			int offset=0;
			StringReader sr= new StringReader(paragraph);
    		BufferedReader br= new BufferedReader(sr);
    		
    		boolean paragraphExists=false;
    		StringBuilder paragraphBuilder=new StringBuilder();
    		
    		paragraphBuilder.append("<p>\n");
    		for (List<String> tokens : segmenter.getSentences(br))
    		{
    			
    			boolean sentenceExists=false;
    			StringBuilder sentenceBuilder=new StringBuilder();
    			
    			sentenceBuilder.append("<s>\n");
    			for (String token:tokens){
    				if (offset!=0){
    					int found=paragraph.indexOf(token,offset);
    					if (paragraph.indexOf(token,offset)==offset){
    						sentenceBuilder.append("<g/>\n");
    					}
    					offset=found+token.length();
    				}
    				else{
    					offset=token.length();
    				}
    				token=CharMatcher.JAVA_ISO_CONTROL.removeFrom(token);
    				token=token.replaceAll("[ \r\n\t\u3000\u00A0\u2007\u202F]","");
    				if (!token.isEmpty()){
    					sentenceBuilder.append(token+"\n");
    					sentenceExists=true;
    				}
    			}
    			sentenceBuilder.append("</s>\n");
    			
    			if (sentenceExists){
    				paragraphExists=true;
    				paragraphBuilder.append(sentenceBuilder.toString());
    			}
    		}
    		paragraphBuilder.append("</p>\n");
    		
    		if (paragraphExists){
    			this.paragraphs.add(paragraphBuilder.toString());
    		}
    		
		 }
		 content=null;
	}
}
