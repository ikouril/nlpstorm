package cz.vutbr.fit.nlpstorm.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for holding a document and correspondent Paragraphs
 */
public class DocumentHolder {
	
	private Document document=null;
	private List<ParagraphDuplicate> duplicates= new ArrayList<ParagraphDuplicate>();
	
	/**
	 * Creates new DocumentHolder, given the document
	 * @param doc - target document
	 */
	public DocumentHolder(Document doc){
		document=doc;
	}
	
	/**
	 * Creates new DocumentHolder, given the Paragraph duplicate (when document arrives later than first paragraph hash)
	 * @param doc - target document
	 */
	public DocumentHolder(ParagraphDuplicate dupl){
		duplicates.add(dupl);
	}
	
	/**
	 * Adds a document to the holder, updates already received paragraphs
	 * @param doc
	 */
	public void addDocument(Document doc){
		document=doc;
		for (int i=0;i<duplicates.size();i++){
			ParagraphDuplicate d=duplicates.get(i);
			document.setMarker(d.getParagraphPosition(), d.isDuplicate());
		}
	}
	
	/**
	 * Adds a ParagraphDuplicate, possibly updates target document
	 * @param dupl
	 */
	public void addParagraph(ParagraphDuplicate dupl){
		duplicates.add(dupl);
		if (document!=null){
			document.setMarker(dupl.getParagraphPosition(), dupl.isDuplicate());
		}
	}
	
	/**
	 * Checks, whether document has all paragraphs
	 * @return true when document is finalized
	 */
	public boolean isFinalized(){
		return document!=null && document.isCompleted();
	}
	
	public Document getDocument(){
		return document;
	}
	

}
