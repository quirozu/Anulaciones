package co.bvc.com.test;

import java.util.ArrayList;
import co.bvc.com.basicfix.DataAccess;

public class Validaciones {

	private String cadenaAI;
	private String CadenaSPrima;
	private String CadenaRPrima = "";

	DataAccess data = new DataAccess();
	private String cadenaOcho;

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
	

	public void validarZ(AutFixRfqDatosCache datosCache, Message qr) throws SQLException {

		int contadorBuenos = 0;
		int contadorMalos = 0;
		String cadena = "" + qr;
		ResultSet resultset;

		String queryMessageR = "SELECT * FROM bvc_automation_db.aut_fix_rfq_datos " + "WHERE ID_CASESEQ = "
				+ datosCache.getIdCaseseq();

		resultset = DataAccess.getQuery(queryMessageR);

		ArrayList<String> cad = FragmentarCadena(cadena);
		String valor;
		String quoteId = null, quoteCancelType = null, idCase = null, idEscenario = null, targetComId = null,
				targetSubId = null, SenderCompID = "EXC", beginString = "FIX.4.4";
		int idSecuencia = 0;
		while (resultset.next()) {

			targetComId = resultset.getString("ID_AFILIADO");
			targetSubId = resultset.getString("RQ_TRADER");
			quoteId = resultset.getString("RQ_QUOTEID");
			quoteCancelType = resultset.getString("RQ_QUOCANCTYPE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");
			idCase = resultset.getString("ID_CASE");
			idEscenario = "FIX_Z";

		}
		System.out.println("----------------------------------------");
		System.out.println("VALIDACION DEL Z\n ");

		for (int i = 0; i < cad.size(); i++) {
			valor = cad.get(i).split("=")[0];
			switch (valor) {

//			case "57":
//				if (cad.get(i).split("=")[1].equals(targetSubId)) {
//
//					contadorBuenos++;
//
//					cadenaDeMensaje("RQ_TRADER", valor, targetSubId);
//
//					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], targetSubId,
//							 idEscenario, idCase, idSecuencia, valor);
//				} else {
//					System.out.println(
//							" RQ_TRADER (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD " + targetSubId);
//					contadorMalos++;
//					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], targetSubId,
//							 idEscenario, idCase, idSecuencia, valor);
//				}
//				break;
//			case "56":
//				if (cad.get(i).split("=")[1].equals(targetComId)) {
//
//					contadorBuenos++;
//
//					cadenaDeMensaje("ID_AFILIADO", valor, targetComId);
//
//					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], targetComId,
//							 idEscenario, idCase, idSecuencia, valor);
//				} else {
//					System.out.println(
//							" ID_AFILIADO (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD " + targetComId);
//					contadorMalos++;
//					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], targetComId,
//							 idEscenario, idCase, idSecuencia, valor);
//				}
//				break;
			case "49":
				if (cad.get(i).split("=")[1].equals(SenderCompID)) {
					contadorBuenos++;

					cadenaDeMensaje(" SenderCompID ", valor, SenderCompID);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], SenderCompID,
							 idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(" SenderCompID (" + valor + "): MSG" + cad.get(i).split("=")[1] + " BD: " + SenderCompID);
					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], SenderCompID,
							 idEscenario, idCase, idSecuencia, valor);

					contadorMalos++;
				}
				break;
			case "8":
				if (cad.get(i).split("=")[1].equals(beginString)) {
					contadorBuenos++;

					cadenaDeMensaje(" beginString ", valor, beginString);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], beginString,
							 idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(
							" beginString (" + valor + "): MSG" + cad.get(i).split("=")[1] + " BD: " + beginString);
					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], beginString,
							 idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;
			case "117":
				if (cad.get(i).split("=")[1].equals(quoteId)) {
					contadorBuenos++;

					cadenaDeMensaje(" RQ_QUOTEID", valor, quoteId);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], quoteId,
							idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out
							.println(" RQ_QUOTEID (" + valor + "): MSG" + cad.get(i).split("=")[1] + " BD: " + quoteId);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], quoteId,
							idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;
			case "298":
				if (cad.get(i).split("=")[1].equals(quoteCancelType)) {
					contadorBuenos++;

					cadenaDeMensaje(" RQ_QUOCANCTYPE", valor, quoteCancelType);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], quoteCancelType,
							 idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(" RQ_QUOCANCTYPE (" + valor + "): MSG" + cad.get(i).split("=")[1] + " BD: "
							+ quoteCancelType);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], quoteCancelType,
							 idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;

			default:
				break;
			}
		}

		System.out.println("----------------------------------------");
		System.out.println("---------------------");
		System.out.println("-- VALIDACIONES Z --");
		System.out.println("LAS VALIDACIONES CORRECTAS FUERON : " + contadorBuenos);
		System.out.println("LAS VALIDACIONES ERRADAS FUERON : " + contadorMalos);
		System.out.println("TOTAL VALIDACIONES REALIZADAS : " + (contadorBuenos + contadorMalos));

	}

	public void validarAG(AutFixRfqDatosCache datosCache, Message qr) throws SQLException, FieldNotFound {

		int contadorBuenos = 0;
		int contadorMalos = 0;
		String cadena = "" + qr;
		ResultSet resultset;

		String queryMessageR = "SELECT * FROM bvc_automation_db.aut_fix_rfq_datos " + "WHERE ID_CASESEQ = "
				+ datosCache.getIdCaseseq();

		resultset = DataAccess.getQuery(queryMessageR);

		ArrayList<String> cad = FragmentarCadena(cadena);
		String valor;
		String beginString = "FIX.4.4", senderCompId = "EXC", targetCompId = null, targetSubId = null,
				securitySubType = null, idCase = null, idEscenario = null;
		int idSecuencia = 0;
		while (resultset.next()) {

//			targetCompId = resultset.getString("ID_AFILIADO");
//			targetSubId = resultset.getString("RS_TRADER");
			securitySubType = resultset.getString("RS_SECSUBTYPE");
			idEscenario = resultset.getString("ID_ESCENARIO");
			idCase = resultset.getString("ID_CASE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");

		}
		System.out.println("----------------------------------------");
		System.out.println("VALIDACION DEL AG\n ");

		for (int i = 0; i < cad.size(); i++) {
			valor = cad.get(i).split("=")[0];
			switch (valor) {

			case "8":
				if (cad.get(i).split("=")[1].equals(beginString)) {
					contadorBuenos++;

					cadenaDeMensaje(" beginString", valor, beginString);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], beginString,
							 idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(
							" beginString (" + valor + "): MSG" + cad.get(i).split("=")[1] + " BD: " + beginString);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], beginString,
							 idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;

			case "49":
				if (cad.get(i).split("=")[1].equals(senderCompId)) {
					contadorBuenos++;

					cadenaDeMensaje(" senderCompId", valor, senderCompId);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], senderCompId,
							 idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(" senderCompId (" + valor + "): MSG" + cad.get(i).split("=")[1] + " BD: " + senderCompId);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], senderCompId,
							 idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;

//			case "56":
//				if (cad.get(i).split("=")[1].equals(targetCompId)) {
//					contadorBuenos++;
//
//					cadenaDeMensaje(" ID_AFILIADO", valor, targetCompId);
//
//					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], targetCompId,
//							 idEscenario, idCase, idSecuencia, valor);
//				} else {
//					System.out.println(
//							" ID_AFILIADO (" + valor + "): MSG" + cad.get(i).split("=")[1] + " BD: " + targetCompId);
//					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], targetCompId,
//							 idEscenario, idCase, idSecuencia, valor);
//					contadorMalos++;
//				}
//				break;
//
//			case "57":
//				if (cad.get(i).split("=")[1].equals(targetSubId)) {
//					contadorBuenos++;
//
//					cadenaDeMensaje(" RS_TRADER", valor, targetSubId);
//
//					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], targetSubId,
//							 idEscenario, idCase, idSecuencia, valor);
//				} else {
//					System.out.println(
//							" RS_TRADER (" + valor + "): MSG" + cad.get(i).split("=")[1] + " BD: " + targetSubId);
//					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], targetSubId,
//							 idEscenario, idCase, idSecuencia, valor);
//					contadorMalos++;
//				}
//				break;

			case "762":
				if (cad.get(i).split("=")[1].equals(securitySubType)) {
					contadorBuenos++;

					cadenaDeMensaje(" RS_SECSUBTYPE", valor, securitySubType);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], securitySubType,
							 idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(" RS_SECSUBTYPE (" + valor + "): MSG" + cad.get(i).split("=")[1] + " BD: "
							+ securitySubType);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], securitySubType,
							 idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;
				
			case "58":
				
				DataAccess.cargarLogs3(qr, datosCache.getIdEjecucion(), idEscenario, idCase, datosCache.getIdSecuencia());
				
				break;

			default:
				break;
			}
		}

		System.out.println("----------------------------------------");
		System.out.println("-- VALIDACIONES DE AG --");
		System.out.println("LAS VALIDACIONES CORRECTAS FUERON : " + contadorBuenos);
		System.out.println("LAS VALIDACIONES ERRADAS FUERON : " + contadorMalos);
		System.out.println("TOTAL VALIDACIONES REALIZADAS : " + (contadorBuenos + contadorMalos));
		System.out.println("----------------------------------------");

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
