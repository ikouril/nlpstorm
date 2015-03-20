package cz.vutbr.fit.nlpstorm.util;

public class ParagraphDuplicate {
	private int paragraphPosition;
	private boolean duplicate;
	private String docId;
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
