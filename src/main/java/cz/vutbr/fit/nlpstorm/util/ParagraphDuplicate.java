package cz.vutbr.fit.nlpstorm.util;

/**
 * Class for holding a paragraph and an information regarding paragraph duplicity
 */
public class ParagraphDuplicate {
	private int paragraphPosition;
	private boolean duplicate;
	private String docId;
	
	/**
     * Creates a new ParagraphDuplicate
     * @param position the position of a paragraph within a document
     * @param dupl the duplicity information
     * @param docId the id of the document paragaph occurs in
     */
	public ParagraphDuplicate(int position,boolean dupl,String docId){
		setParagraphPosition(position);
		setDuplicate(dupl);
		this.setDocId(docId);
	}
	public int getParagraphPosition() {
		return paragraphPosition;
	}
	public void setParagraphPosition(int paragraphPosition) {
		this.paragraphPosition = paragraphPosition;
	}
	public boolean isDuplicate() {
		return duplicate;
	}
	public void setDuplicate(boolean duplicate) {
		this.duplicate = duplicate;
	}
	public String getDocId() {
		return docId;
	}
	public void setDocId(String docId) {
		this.docId = docId;
	}

}
