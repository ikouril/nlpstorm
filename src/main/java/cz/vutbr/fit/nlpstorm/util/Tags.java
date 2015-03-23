package cz.vutbr.fit.nlpstorm.util;


/**
 * Class for holding tags, needed for fully vertical input format needed by MDParser
 */
public class Tags {
	public String before="";
	public String after="";

	public void addAfter(String str){
		after+=str;
	}
	
	public void addBefore(String str){
		before+=str;
	}
}
