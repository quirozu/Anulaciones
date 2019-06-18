/**
 * 
 */
package co.bvc.com.dao.domain;

import java.util.List;

import quickfix.Message;

public class RespuestaConstrucccionMsgFIX {
	
	private Message message;
	private List<String> listSessiones;
	
	public RespuestaConstrucccionMsgFIX() {
		
	}
	
	public RespuestaConstrucccionMsgFIX(Message message, List<String> listSessiones) {
		this.message = message;
		this.listSessiones = listSessiones;
	}

	public Message getMessage() {
		return message;
	}
	public void setMessage(Message message) {
		this.message = message;
	}
	public List<String> getListSessiones() {
		return listSessiones;
	}
	
	public void setListSessiones(List<String> listSessiones) {
		this.listSessiones = listSessiones;
	}

}
