package co.bvc.com.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import co.bvc.com.basicfix.BasicFunctions;
import co.bvc.com.basicfix.DataAccess;
import co.bvc.com.dao.domain.AutFixRfqDatosCache;
import quickfix.FieldNotFound;
import quickfix.Group;
import quickfix.field.NoPartyIDs;
import quickfix.field.PartyID;
import quickfix.field.PartyRole;
import quickfix.field.TargetCompID;
import quickfix.fix44.Message;

public class Validaciones {

	private String cadenaAI;
	private String CadenaSPrima;
	private String CadenaRPrima = "";

	DataAccess data = new DataAccess();
	private String cadenaOcho;

	public String getCadenaOcho() {
		return cadenaOcho;
	}

	public void setCadenaOcho(String cadenaOcho) {
		this.cadenaOcho = cadenaOcho;
	}

	public String getCadenaRPrima() {
		return CadenaRPrima;
	}

	public void setCadenaRPrima(String cadenaRPrima) {
		CadenaRPrima = cadenaRPrima;
	}

	public String getCadenaSPrima() {
		return CadenaSPrima;
	}

	public void setCadenaSPrima(String cadenaSPrima) {
		CadenaSPrima = cadenaSPrima;
	}

	public String getCadenaAI() {
		return cadenaAI;
	}

	public void setCadenaAI(String cadenaAI) {
		this.cadenaAI = cadenaAI;
	}

	public ArrayList<String> FragmentarCadena(String cadena) {
		ArrayList<String> claveValor = new ArrayList<String>();
		for (int i = 0; i < cadena.split("").length; i++) {
			claveValor.add(cadena.split("")[i]);
//                    System.out.println(claveValor.get(i));
		}
		return claveValor;
	}

	public ArrayList<String> FragmentarCadena1(String cadena) {
		ArrayList<String> claveValor1 = new ArrayList<String>();
		for (int i = 0; i < cadena.split("").length; i++) {
			claveValor1.add(cadena.split("")[i]);
//                    System.out.println(claveValor.get(i));
		}
		return claveValor1;
	}

	public void validarAR(AutFixRfqDatosCache datosCache, Message message) throws SQLException, FieldNotFound {
//		int contadorBuenos = 0;
//		int contadorMalos = 0;
//		String cadenaPrima = message.toString();
//		
		ResultSet resultset;
		System.out.println("***************************\n " + datosCache.getIdCaseseq() + "\n***************************\n ");
		String queryMessageAE = "SELECT * FROM aut_fix_tcr_datos WHERE ID_CASESEQ = " + datosCache.getIdCaseseq();
		resultset = DataAccess.getQuery(queryMessageAE);
		
		Map <Integer, String> mapaDB = new TreeMap<Integer, String>();
		
		while(resultset.next()) {
//			mapaDB.put(35, resultset.getString("AR_MSGTYPE"));
			mapaDB.put(571, resultset.getString("AR_TRADEREPID"));
			mapaDB.put(487, resultset.getString("AR_TRADTRANTYPE"));
			mapaDB.put(856, resultset.getString("AR_TRADEREPTYPE"));
			mapaDB.put(939, resultset.getString("AR_TRDRPTSTATUS"));
			mapaDB.put(17, resultset.getString("AR_EXECID"));
			mapaDB.put(60, resultset.getString("AR_TRANSTIME"));
		}
		
		for(Map.Entry<Integer,String> entry : mapaDB.entrySet()) {
			Integer key = entry.getKey();
			String value = entry.getValue();

			System.out.println(key + " => DB: " + value + " MG: " + message.getString(key));
		}
		
//		for (int i = 0; i < cad.size(); i++) {
//			etiquetaFix = cad.get(i).split("=")[0];
//			valorFix = cad.get(i).split("=")[1];
//			switch (etiquetaFix) {
		
	}

	public void validar3(AutFixRfqDatosCache datosCache, Message message) throws SQLException, FieldNotFound {

		ResultSet resultset;
		String queryMessageR = "SELECT * FROM bvc_automation_db.aut_fix_rfq_datos " + "WHERE ID_CASESEQ = " 
				+ datosCache.getIdCaseseq();

		resultset = DataAccess.getQuery(queryMessageR);
		String idEscenario = null, idCase = null;

		while (resultset.next()) {
			idEscenario = resultset.getString("ID_ESCENARIO");
			idCase = resultset.getString("ID_CASE");
		}

		DataAccess.cargarLogs3(message, datosCache.getIdEjecucion(), idEscenario, idCase, datosCache.getIdSecuencia());
		System.out.println("************\n SE CARGO AL LOG VALIDAR 3 \n************ ");

	}

	public void cadenaDeMensaje(String columna, String valor, String comparar) {
		System.out.println(columna + "(" + valor + "): " + comparar);
	}
}
