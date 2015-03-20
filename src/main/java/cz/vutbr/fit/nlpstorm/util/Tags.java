package cz.vutbr.fit.nlpstorm.util;

import java.util.ArrayList;
import java.util.List;

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
