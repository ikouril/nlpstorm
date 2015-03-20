package cz.vutbr.fit.nlpstorm.util;

import java.util.ArrayList;
import java.util.List;

public class DocumentHolder {
	
	private Document document=null;
	private List<ParagraphDuplicate> duplicates= new ArrayList<ParagraphDuplicate>();
	
	public DocumentHolder(Document doc){
		document=doc;
	}
	
	public DocumentHolder(ParagraphDuplicate dupl){
		duplicates.add(dupl);
	}
	
	public void addDocument(Document doc){
		document=doc;
		for (int i=0;i<duplicates.size();i++){
			ParagraphDuplicate d=duplicates.get(i);
			document.setMarker(d.getParagraphPosition(), d.isDuplicate());
		}
	}
	
	public void addParagraph(ParagraphDuplicate dupl){
		duplicates.add(dupl);
		if (document!=null){
			document.setMarker(dupl.getParagraphPosition(), dupl.isDuplicate());
		}
	}
	
	public boolean isFinalized(){
		return document!=null && document.isCompleted();
	}
	
	public Document getDocument(){
		return document;
	}
	

}
