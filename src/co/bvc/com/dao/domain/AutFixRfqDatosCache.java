package co.bvc.com.dao.domain;

public class AutFixRfqDatosCache {
	
	private String receiverSession;

	private int idCaseseq;

	private int idCase;

	private int idSecuencia;

	private String estado;

	private String fixQuoteReqId;

	private String idAfiliado;
	
	private long idEjecucion;
	
	public AutFixRfqDatosCache() {
	}

	public String getReceiverSession() {
		return this.receiverSession;
	}

	public void setReceiverSession(String receiverSession) {
		this.receiverSession = receiverSession;
	}

	public String getIdAfiliado() {
		return this.idAfiliado;
	}

	public void setIdAfiliado(String idAfiliado) {
		this.idAfiliado = idAfiliado;
	}

	public int getIdCase() {
		return this.idCase;
	}

	public void setIdCase(int idCase) {
		this.idCase = idCase;
	}

	public int getIdCaseseq() {
		return this.idCaseseq;
	}

	public void setIdCaseseq(int idCaseseq) {
		this.idCaseseq = idCaseseq;
	}

	public int getIdSecuencia() {
		return this.idSecuencia;
	}

	public void setIdSecuencia(int idSecuencia) {
		this.idSecuencia = idSecuencia;
	}

	public String getEstado() {
		return estado;
	}

	public void setEstado(String estado) {
		this.estado = estado;
	}

	public String getFixQuoteReqId() {
		return fixQuoteReqId;
	}

	public void setFixQuoteReqId(String fixQuoteReqId) {
		this.fixQuoteReqId = fixQuoteReqId;
	}

	public long getIdEjecucion() {
		return idEjecucion;
	}

	public void setIdEjecucion(long idEjecucion) {
		this.idEjecucion = idEjecucion;
	}

	@Override
	public String toString() {
		return "AutFixRfqDatosCache [receiverSession=" + receiverSession + ", idCaseseq=" + idCaseseq + ", idCase="
				+ idCase + ", idSecuencia=" + idSecuencia + ", estado=" + estado + ", fixQuoteReqId=" + fixQuoteReqId
				+ ", idAfiliado=" + idAfiliado + ", idEjecucion=" + idEjecucion + "]";
	}	
	
}
