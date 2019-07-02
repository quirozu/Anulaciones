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

	public void ValidarRPrima(AutFixRfqDatosCache datosCache, Message message)
			throws InterruptedException, SQLException, FieldNotFound {

		int contadorBuenos = 0;
		int contadorMalos = 0;
		String cadenaPrima = message.toString();
		String clavePrima;
		ArrayList<String> cad = FragmentarCadena1(cadenaPrima);
		ResultSet resultset;
		String queryMessageR = "SELECT * FROM bvc_automation_db.aut_fix_rfq_datos " + "WHERE ID_CASESEQ = "
				+ datosCache.getIdCaseseq();

		resultset = DataAccess.getQuery(queryMessageR);
		String symbol = null, msgType = null, secSubTypec = null, side = null, orderQty = null, validuntiltime = null,
				norelatedSymg = null, idCase = null, beginString = "FIX.4.4", SenderCompID = "EXC", noPartyId = null,
				id_Escenario = null, targetComId = null, targetSubId = null, securityIdSource = "M";
		int idSecuencia = 0;

		while (resultset.next()) {
			targetComId = resultset.getString("ID_AFILIADO");
			targetSubId = resultset.getString("RQ_TRADER");
			symbol = resultset.getString("RQ_SYMBOL");
			msgType = resultset.getString("RQ_MSGTYPE");
			secSubTypec = resultset.getString("RQ_SECSUBTYPE");
			side = resultset.getString("RQ_SIDE");
			orderQty = resultset.getString("RQ_ORDERQTY");
			validuntiltime = resultset.getString("RQ_VALIDUNTILTIME");
			norelatedSymg = resultset.getString("RQ_NORELATEDSYM");
			idCase = resultset.getString("ID_CASE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");
			noPartyId = resultset.getString("RQ_NOPARTYIDS");
			id_Escenario = resultset.getString("ID_ESCENARIO");

		}
		System.out.println("----------------------------------------");
		System.out.println("VALIDACION DE R CON R PRIMA\n");

		for (int z = 0; z < cad.size(); z++) {
			clavePrima = cad.get(z).split("=")[0];
			switch (clavePrima) {
			case "22":
				if (cad.get(z).split("=")[1].equals(securityIdSource)) {

					contadorBuenos++;

					cadenaDeMensaje("securityIdSource", clavePrima, securityIdSource);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], securityIdSource,
							 id_Escenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(
							" securityIdSource (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD " + securityIdSource);
					contadorMalos++;
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1],
							securityIdSource, id_Escenario, idCase, idSecuencia, clavePrima);
				}
				break;

//			case "57":
//				if (cad.get(z).split("=")[1].equals(targetSubId)) {
//
//					contadorBuenos++;
//
//					cadenaDeMensaje("RQ_TRADER", clavePrima, targetSubId);
//
//					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], targetSubId,
//							 id_Escenario, idCase, idSecuencia, clavePrima);
//				} else {
//					System.out.println(
//							" RQ_TRADER (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD " + targetSubId);
//					contadorMalos++;
//					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], targetSubId,
//							 id_Escenario, idCase, idSecuencia, clavePrima);
//				}
//				break;
//			case "56":
//				if (cad.get(z).split("=")[1].equals(targetComId)) {
//
//					contadorBuenos++;
//
//					cadenaDeMensaje("ID_AFILIADO", clavePrima, targetComId);
//
//					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], targetComId,
//							 id_Escenario, idCase, idSecuencia, clavePrima);
//				} else {
//					System.out.println(" ID_AFILIADO (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD "
//							+ targetComId);
//					contadorMalos++;
//					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], targetComId,
//							 id_Escenario, idCase, idSecuencia, clavePrima);
//				}
//				break;
			case "55":
				if (cad.get(z).split("=")[1].equals(symbol)) {

					contadorBuenos++;

					cadenaDeMensaje(" RQ_SYMBOL", clavePrima, symbol);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], symbol,
							 id_Escenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(
							" RQ_SYMBOL (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD " + symbol);
					contadorMalos++;
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1],symbol,
							 id_Escenario, idCase, idSecuencia, clavePrima);
				}
				break;
//			case "35":
//				if (cad.get(z).split("=")[1].equals(msgType)) {
//					contadorBuenos++;
//
//					cadenaDeMensaje(" RQ_MSGTYPE", clavePrima, msgType);
//
//					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], msgType,
//							 id_Escenario, idCase, idSecuencia, cadenaPrima);
//				} else {
//					System.out.println(
//							" RQ_MSGTYPE (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD " + msgType);
//					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], msgType,
//							 id_Escenario, idCase, idSecuencia, clavePrima);
//					contadorMalos++;
//				}
//				break;
			case "762":
				if (cad.get(z).split("=")[1].equals(secSubTypec)) {
					contadorBuenos++;

					cadenaDeMensaje(" RQ_SECSUBTYPE", clavePrima, secSubTypec);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], secSubTypec,
							 id_Escenario, idCase, idSecuencia, clavePrima);

				} else {
					System.out.println(" RQ_SECSUBTYPE (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: "
							+ secSubTypec);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], secSubTypec,
							 id_Escenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;
			case "54":
				if (cad.get(z).split("=")[1].equals(side)) {
					contadorBuenos++;
					cadenaDeMensaje(" RQ_SIDE", clavePrima, side);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], side, 
							id_Escenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out
							.println("RQ_SIDE (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: " + side);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], side, 
							id_Escenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;
			case "38":
				if (cad.get(z).split("=")[1].equals(orderQty)) {
					contadorBuenos++;

					cadenaDeMensaje(" RS_ORDERQTY", clavePrima, orderQty);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), orderQty, cad.get(z).split("=")[1],
							 id_Escenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(
							"RS_ORDERQTY (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: " + orderQty);

					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), orderQty, cad.get(z).split("=")[1],
							 id_Escenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;
			case "146":
				if (cad.get(z).split("=")[1].equals(norelatedSymg)) {
					contadorBuenos++;

					cadenaDeMensaje(" RQ_NORELATEDSYM", clavePrima, norelatedSymg);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), norelatedSymg, cad.get(z).split("=")[1],
							 id_Escenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(" RQ_NORELATEDSYM(" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: "
							+ norelatedSymg);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), norelatedSymg, cad.get(z).split("=")[1],
							 id_Escenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;

			case "8":
				if (cad.get(z).split("=")[1].equals(beginString)) {
					contadorBuenos++;

					cadenaDeMensaje(" beginString", clavePrima, beginString);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], beginString,
							 id_Escenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(" beginString(" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: "
							+ beginString);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], beginString,
							 id_Escenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;
			case "453":
				if (cad.get(z).split("=")[1].equals(noPartyId)) {
					contadorBuenos++;

					cadenaDeMensaje(" RQ_NOPARTYIDS", clavePrima, noPartyId);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], noPartyId,
							id_Escenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(" RQ_NOPARTYIDS(" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: "
							+ noPartyId);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], noPartyId,
							 id_Escenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;
			case "49":
				if (cad.get(z).split("=")[1].equals(SenderCompID)) {
					contadorBuenos++;

					cadenaDeMensaje(" SenderCompID ", clavePrima, SenderCompID);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), 
							cad.get(z).split("=")[1], SenderCompID, id_Escenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(" SenderCompID(" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: "
							+ SenderCompID);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), 
							cad.get(z).split("=")[1], SenderCompID, id_Escenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;

			default:

				break;
			}
		}
		System.out.println("----------------------------------------");
		System.out.println("LAS VALIDACIONES CORRECTAS FUERON : " + contadorBuenos);
		System.out.println("LAS VALIDACIONES ERRADAS FUERON : " + contadorMalos);
		System.out.println("TOTAL VALIDACIONES REALIZADAS : " + (contadorBuenos + contadorMalos));
	}

	public void ValidarSPrima(AutFixRfqDatosCache datosCache, quickfix.Message messageIn)
			throws InterruptedException, SQLException {
		int contadorBuenos = 0;
		int contadorMalos = 0;
		String cadenaPrima = "" + messageIn;
		String clavePrima;
		ArrayList<String> cad = FragmentarCadena1(cadenaPrima);
		ResultSet resultset;
		String queryMessageR = "SELECT * FROM bvc_automation_db.aut_fix_rfq_datos " + "WHERE ID_CASESEQ = "
				+ datosCache.getIdCaseseq();

		resultset = DataAccess.getQuery(queryMessageR);
		String msgType = null, OfferSize = null, secSubType = null, symbol = null, noPartyId = "5",
				SecurityIDSource = "M", SenderCompID = "EXC", beginString = "FIX.4.4", idCase = null,
				targetSubId = null, targetComId = null, idEscenario = null, offerYield = null, bidSize = null,
				bidyield = null;
		int idSecuencia = 0;

		while (resultset.next()) {
			OfferSize = resultset.getString("RQ_OFFERSIZE");
			bidyield = resultset.getString("RQ_BIDYIELD");
			bidSize = resultset.getString("RQ_BIDSIZE");
			offerYield = resultset.getString("RQ_OFFERYIELD");
			targetComId = resultset.getString("ID_AFILIADO");
			targetSubId = resultset.getString("RQ_TRADER");
			msgType = resultset.getString("RQ_MSGTYPE");
			secSubType = resultset.getString("RQ_SECSUBTYPE");
			symbol = resultset.getString("RQ_SYMBOL");
			idCase = resultset.getString("ID_CASE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");
			idEscenario = resultset.getString("ID_ESCENARIO");

		}
		System.out.println("----------------------------------------");
		System.out.println("VALIDACION DE S CON S PRIMA\n");
		for (int z = 0; z < cad.size(); z++) {
			clavePrima = cad.get(z).split("=")[0];
			switch (clavePrima) {

			case "632":
				if (cad.get(z).split("=")[1].equals(bidyield)) {

					contadorBuenos++;

					cadenaDeMensaje("RQ_BIDYIELD", clavePrima, bidyield);

					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], bidyield,
							 idEscenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(
							" RQ_BIDYIELD (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD " + bidyield);
					contadorMalos++;
					DataAccess.cargarLogsFallidos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], bidyield,
							 idEscenario, idCase, idSecuencia, clavePrima);
				}
				break;

			case "134":
				if (cad.get(z).split("=")[1].equals(bidSize)) {

					contadorBuenos++;

					cadenaDeMensaje("RQ_BIDSIZE", clavePrima, bidSize);

					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], bidSize,
							 idEscenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(
							" RQ_BIDSIZE (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD " + bidSize);
					contadorMalos++;
					DataAccess.cargarLogsFallidos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], bidSize,
							 idEscenario, idCase, idSecuencia, clavePrima);
				}
				break;

			case "634":
				if (cad.get(z).split("=")[1].equals(offerYield)) {

					contadorBuenos++;

					cadenaDeMensaje("RQ_OFFERYIELD", clavePrima, offerYield);

					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], offerYield,
							 idEscenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(" RQ_OFFERYIELD (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD "
							+ offerYield);
					contadorMalos++;
					DataAccess.cargarLogsFallidos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], offerYield,
							 idEscenario, idCase, idSecuencia, clavePrima);
				}
				break;

//			case "57":
//				if (cad.get(z).split("=")[1].equals(targetSubId)) {
//
//					contadorBuenos++;
//
//					cadenaDeMensaje("RQ_TRADER", clavePrima, targetSubId);
//
//					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], targetSubId,
//							 idEscenario, idCase, idSecuencia, clavePrima);
//				} else {
//					System.out.println(
//							" RQ_TRADER (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD " + targetSubId);
//					contadorMalos++;
//					DataAccess.cargarLogsFallidos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], targetSubId,
//							 idEscenario, idCase, idSecuencia, clavePrima);
//				}
//				break;
//			case "56":
//				if (cad.get(z).split("=")[1].equals(targetComId)) {
//
//					contadorBuenos++;
//
//					cadenaDeMensaje("ID_AFILIADO", clavePrima, targetComId);
//
//					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], targetComId,
//							 idEscenario, idCase, idSecuencia, clavePrima);
//				} else {
//					System.out.println(" ID_AFILIADO (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD "
//							+ targetComId);
//					contadorMalos++;
//					DataAccess.cargarLogsFallidos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], targetComId,
//							 idEscenario, idCase, idSecuencia, clavePrima);
//				}
//				break;

			case "55":
				if (cad.get(z).split("=")[1].equals(symbol)) {
					contadorBuenos++;

					cadenaDeMensaje(" RQ_SYMBOL", clavePrima, symbol);

					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], symbol,
							 idEscenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(
							" RQ_SYMBOL (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: " + symbol);
					DataAccess.cargarLogsFallidos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], symbol,
							 idEscenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;
			case "762":
				if (cad.get(z).split("=")[1].equals(secSubType)) {
					contadorBuenos++;

					cadenaDeMensaje(" RQ_SECSUBTYPE", clavePrima, secSubType);

					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], secSubType,
							 idEscenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(" RQ_SECSUBTYPE (" + clavePrima + "): MSG" + cad.get(z).split("=")[1] + " BD: "
							+ secSubType);
					DataAccess.cargarLogsFallidos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], secSubType,
							 idEscenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;
			case "35":
				if (cad.get(z).split("=")[1].equals(msgType)) {
					contadorBuenos++;

					cadenaDeMensaje(" RQ_MSGTYPE", clavePrima, msgType);

					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], msgType,
							 idEscenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(
							" RQ_MSGTYPE (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: " + msgType);
					DataAccess.cargarLogsFallidos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], msgType,
							 idEscenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;
			case "135":
				if (cad.get(z).split("=")[1].equals(OfferSize)) {
					contadorBuenos++;

					cadenaDeMensaje(" OFFER SIZE", clavePrima, OfferSize);

					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], OfferSize,
							 idEscenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(
							" OFFER SIZE (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: " + OfferSize);
					DataAccess.cargarLogsFallidos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], OfferSize,
							 idEscenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;
			case "453":
				if (cad.get(z).split("=")[1].equals(noPartyId)) {
					contadorBuenos++;

					cadenaDeMensaje(" NO PARTY ID", clavePrima, noPartyId);

					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], noPartyId,
							 idEscenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(
							" NO PARTY ID (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: " + noPartyId);
					DataAccess.cargarLogsFallidos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], noPartyId,
							 idEscenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;
			case "22":
				if (cad.get(z).split("=")[1].equals(SecurityIDSource)) {
					contadorBuenos++;

					cadenaDeMensaje(" SecurityIDSource ", clavePrima, SecurityIDSource);

					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], SecurityIDSource,
							 idEscenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(
							" SecurityIDSource (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: " + SecurityIDSource);
					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], SecurityIDSource,
							 idEscenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;
			case "49":
				if (cad.get(z).split("=")[1].equals(SenderCompID)) {
					contadorBuenos++;

					cadenaDeMensaje(" SenderCompID ", clavePrima, SenderCompID);

					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], SenderCompID,
							 idEscenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(
							" SenderCompID (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: " + SenderCompID);
					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], SenderCompID,
							 idEscenario, idCase, idSecuencia, clavePrima);

					contadorMalos++;
				}
				break;
			case "8":
				if (cad.get(z).split("=")[1].equals(beginString)) {
					contadorBuenos++;

					cadenaDeMensaje(" beginString ", clavePrima, beginString);

					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], beginString,
							 idEscenario, idCase, idSecuencia, clavePrima);
				} else {
					System.out.println(
							" beginString (" + clavePrima + ") MSG: " + cad.get(z).split("=")[1] + " BD: " + beginString);
					DataAccess.cargarLogsExitosos(messageIn, datosCache.getIdEjecucion(), cad.get(z).split("=")[1], beginString,
							 idEscenario, idCase, idSecuencia, clavePrima);
					contadorMalos++;
				}
				break;

			default:
				break;
			}
		}
//		Thread.sleep(5000);
//		imput();
		System.out.println("----------------------------------------");
		System.out.println("LAS VALIDACIONES CORRECTAS FUERON : " + contadorBuenos);
		System.out.println("LAS VALIDACIONES ERRADAS FUERON : " + contadorMalos);
		System.out.println("TOTAL VALIDACIONES REALIZADAS : " + (contadorBuenos + contadorMalos));

	}

	public void validaAI(AutFixRfqDatosCache datosCache, Message qr) throws SQLException {
		int contadorBuenos = 0;
		int contadorMalos = 0;
		String cadena = "" + qr;
		ResultSet resultset;

		String queryMessageR = "SELECT * FROM bvc_automation_db.aut_fix_rfq_datos " + "WHERE ID_CASESEQ = "
				+ datosCache.getIdCaseseq();

		resultset = DataAccess.getQuery(queryMessageR);
		ArrayList<String> cad = FragmentarCadena(cadena);
		String valor;
		String type = null, symbol = null, subtype = null, side = null, order = null, latedsym = null, idCase = null,
				offerYield = null, quoteStatus = null, beginString = "FIX.4.4", idEscenario = null,
				SenderCompID = "EXC", targetSubId = null, targetComId = null, noPartyId = "5", SecurityIDSource = "M",
				OfferSize = null, bidyield = null, bidSize = null, quoteCancelType = null;
		int idSecuencia = 0;
		while (resultset.next()) {
			OfferSize = resultset.getString("RS_OFFERSIZE");
			bidyield = resultset.getString("RS_BIDYIELD");
			bidSize = resultset.getString("RS_BIDSIZE");
			offerYield = resultset.getString("RS_OFFERYIELD");
			quoteStatus = resultset.getString("RS_QUOTESTATUS");
//			targetComId = resultset.getString("ID_AFILIADO");
//			targetSubId = resultset.getString("RS_TRADER");
			type = resultset.getString("RS_MSGTYPE");
			symbol = resultset.getString("RS_SYMBOL");
			subtype = resultset.getString("RS_SECSUBTYPE");
			side = resultset.getString("RS_SIDE");
			order = resultset.getString("RS_ORDERQTY");
			latedsym = resultset.getString("RS_NORELATEDSYM");
			idCase = resultset.getString("ID_CASE");
			quoteCancelType = resultset.getString("RS_QUOTECANCELTYPE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");
			idEscenario = "FIX_AI";

		}
		System.out.println("----------------------------------------");
		System.out.println("VALIDACION DEL AI\n ");
		for (int i = 0; i < cad.size(); i++) {
			valor = cad.get(i).split("=")[0];
			switch (valor) {

			case "298":
				if (cad.get(i).split("=")[1].equals(quoteCancelType)) {
					contadorBuenos++;

					cadenaDeMensaje(" OFFER SIZE", valor, quoteCancelType);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), quoteCancelType,
							cad.get(i).split("=")[1], idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(
							" OFFER SIZE (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + quoteCancelType);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), quoteCancelType,
							cad.get(i).split("=")[1], idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;
			case "135":
				if (cad.get(i).split("=")[1].equals(OfferSize)) {
					contadorBuenos++;

					cadenaDeMensaje(" OFFER SIZE", valor, OfferSize);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), OfferSize, cad.get(i).split("=")[1],
							idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(
							" OFFER SIZE (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + OfferSize);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), OfferSize, cad.get(i).split("=")[1],
							idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;
			case "632":
				if (cad.get(i).split("=")[1].equals(bidyield)) {

					contadorBuenos++;

					cadenaDeMensaje("RS_BIDYIELD", valor, bidyield);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], bidyield, 
							idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(
							" RS_BIDYIELD (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD " + bidyield);
					contadorMalos++;
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], bidyield, 
							idEscenario, idCase, idSecuencia, valor);
				}
				break;

			case "134":
				if (cad.get(i).split("=")[1].equals(bidSize)) {

					contadorBuenos++;

					cadenaDeMensaje("RS_BIDSIZE", valor, bidSize);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], bidSize, 
							idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out
							.println(" RS_BIDSIZE (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD " + bidSize);
					contadorMalos++;
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], bidSize, 
							idEscenario, idCase, idSecuencia, valor);
				}
				break;

			case "634":
				if (cad.get(i).split("=")[1].equals(offerYield)) {

					contadorBuenos++;

					cadenaDeMensaje("RS_OFFERYIELD", valor, offerYield);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], offerYield, 
							idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(
							" RS_OFFERYIELD (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD " + offerYield);
					contadorMalos++;
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], offerYield, 
							idEscenario, idCase, idSecuencia, valor);
				}
				break;

			case "297":
				if (cad.get(i).split("=")[1].equals(quoteStatus)) {
					contadorBuenos++;

					cadenaDeMensaje(" RS_QUOTESTATUS ", valor, quoteStatus);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), quoteStatus,
							cad.get(i).split("=")[1], idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(
							" RS_QUOTESTATUS (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + quoteStatus);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], quoteStatus,
							 idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;

			case "22":
				if (cad.get(i).split("=")[1].equals(SecurityIDSource)) {
					contadorBuenos++;

					cadenaDeMensaje(" SecurityIDSource ", valor, SecurityIDSource);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), SecurityIDSource,
							cad.get(i).split("=")[1], idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out
							.println(" SecurityIDSource (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + SecurityIDSource);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], SecurityIDSource,
							 idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;
			case "453":
				if (cad.get(i).split("=")[1].equals(noPartyId)) {
					contadorBuenos++;

					cadenaDeMensaje(" NO PARTY ID", valor, noPartyId);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], noPartyId, 
							idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(
							" NO PARTY ID (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + noPartyId);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], noPartyId, 
							idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;
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
					System.out.println(" SenderCompID (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + SenderCompID);
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
							" beginString (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + beginString);
					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], beginString,
							 idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;
			case "55":
				if (cad.get(i).split("=")[1].equals(symbol)) {
					contadorBuenos++;

					cadenaDeMensaje("RS_SYMBOL", valor, symbol);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], symbol,
							idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println("RS_SYMBOL (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + symbol);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], symbol,
							idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;
			case "35":
				if (cad.get(i).split("=")[1].equals(type)) {
					contadorBuenos++;
					cadenaDeMensaje("RS_MSGTYPE", valor, type);
					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], type,
							idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println("RS_MSGTYPE (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + type);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1],  type,
							idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;
			case "762":
				if (cad.get(i).split("=")[1].equals(subtype)) {
					contadorBuenos++;
					cadenaDeMensaje("RS_SECSUBTYPE", valor, subtype);

					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1],  subtype,
							idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(
							"RS_SECSUBTYPE(" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + subtype);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], subtype,
							idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;
			case "54":
				if (cad.get(i).split("=")[1].equals(side)) {
					contadorBuenos++;

					cadenaDeMensaje("RS_SIDE", valor, side);
					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1],  side,
							idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println("RS_SIDE (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + side);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1],  side,
							idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;
			case "38":
				if (cad.get(i).split("=")[1].equals(order)) {
					contadorBuenos++;
					cadenaDeMensaje("RS_ORDERQTY", valor, order);
					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], order, 
							idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println("RS_ORDERQTY (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + order);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], order,
							idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;

			case "694":
				if (cad.get(i).split("=")[1].equals(latedsym)) {
					contadorBuenos++;
					cadenaDeMensaje("RS_NORELATEDSYM", valor, latedsym);
					DataAccess.cargarLogsExitosos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], latedsym, 
							idEscenario, idCase, idSecuencia, valor);
				} else {
					System.out.println(
							"RS_NORELATEDSYM (" + valor + ") MSG: " + cad.get(i).split("=")[1] + " BD: " + latedsym);
					DataAccess.cargarLogsFallidos(qr, datosCache.getIdEjecucion(), cad.get(i).split("=")[1], latedsym, 
							idEscenario, idCase, idSecuencia, valor);
					contadorMalos++;
				}
				break;

			default:
				break;
			}
		}
		System.out.println("----------------------------------------");
		System.out.println("LAS VALIDACIONES CORRECTAS FUERON : " + contadorBuenos);
		System.out.println("LAS VALIDACIONES ERRADAS FUERON : " + contadorMalos);
		System.out.println("TOTAL VALIDACIONES REALIZADAS : " + (contadorBuenos + contadorMalos));

	}

	public void validarOcho(AutFixRfqDatosCache datosCache, Message message) throws SQLException, FieldNotFound {
		int contadorBuenos = 0;
		int contadorMalos = 0;
		String cadena = "" + message;
		ResultSet resultset;
		String queryMessageR = "SELECT * FROM bvc_automation_db.aut_fix_rfq_datos " + "WHERE ID_CASESEQ = "
				+ datosCache.getIdCaseseq();

		resultset = DataAccess.getQuery(queryMessageR);

		ArrayList<String> cad = FragmentarCadena(cadena);
		String etiquetaFix, valorFix;
		String execType = null, orderStatus = null, side = null, leaveQty = null, orderQty = null, price = null,
				grosstradeamt = null, nopartyIds = null, secSubtype = null, reforderIdsCr = null, symbol = null,
				SenderCompID = "EXC", beginString = "FIX.4.4", SecurityIDSource = "M", senderSubId = null,
				dirtyPrice = null, partyIdsSource = null, idCase = null, idEscenario = null, targetComId = null,
				targetSubId = null, ciordId = null, execId = null, orderId = null, reforderId = null, cumQTy = null,
				fillYield = null, trdmatchId = null;
		int idSecuencia = 0;
		
		//Valores para grupos repetitivos de parties
		String setLocVal = null, execFirmVal = null, execTraderVal = null, contraTraderVal = null, contraFirmVal = null, enterTraderVal = null, enterTraderVal1 = null;
		int setLoc = 0, execFirm = 0, execTrader = 0, contraTrader = 0, contraFirm = 0, enterTrader = 0;
				
		// String ER_SETTLOCVAL 10, ER_EXECFIRMVAL 1, ER_EXECTRADERVAL 12, ER_CONTRATRADERVAL 37, ER_CONTRAFIRMVAL 17, ER_ENTERTRADERVAL 36
				
		while (resultset.next()) {
			cumQTy = resultset.getString("ER_CUMQTY");
			ciordId = resultset.getString("ER_CLORDID");
			execId = resultset.getString("ER_EXECID");
			orderId = resultset.getString("ER_ORDERID");
			reforderId = resultset.getString("ER_REFORDERID");
			targetComId = resultset.getString("ID_AFILIADO");
			targetSubId = resultset.getString("RQ_TRADER");
			execType = resultset.getString("ER_EXECTYPE");
			orderStatus = resultset.getString("ER_ORDSTATUS");
			side = resultset.getString("ER_SIDE");
			leaveQty = resultset.getString("ER_LEAVEQTY");
			orderQty = resultset.getString("ER_ORDERQTY");
			price = resultset.getString("ER_PRICE");
			grosstradeamt = resultset.getString("ER_GROSSTRADEAMT");
			nopartyIds = resultset.getString("ER_NOPARTYIDS");
			secSubtype = resultset.getString("ER_SECSUBTYPE");
			reforderIdsCr = resultset.getString("ER_REFORDERIDSCR");
			symbol = resultset.getString("ER_SYMBOL");
			senderSubId = resultset.getString("ER_SENDERSUBID");
			dirtyPrice = resultset.getString("ER_DIRTYPRICE");
			partyIdsSource = resultset.getString("ER_PARTYIDSOURCE");
			idCase = resultset.getString("ID_CASE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");
			fillYield = resultset.getString("ER_FILLYIELD");
			trdmatchId = resultset.getString("ER_TRMATCHID");
			idEscenario = "FIX_8";
			
			//Si la session es la del iniciador
			if(message.getHeader().getString(TargetCompID.FIELD).equals(BasicFunctions.getIniciator())) {
				
				execFirm	 	= resultset.getInt("ER_EXECFIRM"); // 1
				execFirmVal 	= resultset.getString("ER_EXECFIRMVAL"); 
				setLoc	 		= resultset.getInt("ER_SETTLOC"); //10
				setLocVal 		= resultset.getString("ER_SETTLOCVAL"); 
				execTrader	 	= resultset.getInt("ER_EXECTRADER"); // 12
				execTraderVal 	= resultset.getString("ER_EXECTRADERVAL"); 
				contraFirm	 	= resultset.getInt("ER_CONTRAFIRM"); // 17
				contraFirmVal 	= resultset.getString("ER_CONTRAFIRMVAL"); 
				enterTrader	 	= resultset.getInt("ER_ENTERTRADER"); // 36
				enterTraderVal 	= resultset.getString("ER_ENTERTRADERVAL"); 
				contraTrader	= resultset.getInt("ER_CONTRATRADER"); // 37
				contraTraderVal = resultset.getString("ER_CONTRATRADERVAL");
			
			} else { 
				// Si la session no es la del iniciador se invierten firma y trader por contrafirma y contratrader respectivamente.
				execFirm	 	= resultset.getInt("ER_EXECFIRM"); // 1
				execFirmVal 	= resultset.getString("ER_CONTRAFIRMVAL"); 
				setLoc	 		= resultset.getInt("ER_SETTLOC"); //10
				setLocVal 		= resultset.getString("ER_SETTLOCVAL"); 
				execTrader	 	= resultset.getInt("ER_EXECTRADER"); // 12
				execTraderVal 	= resultset.getString("ER_CONTRATRADERVAL");
				contraFirm	 	= resultset.getInt("ER_CONTRAFIRM"); // 17
				contraFirmVal 	= resultset.getString("ER_EXECFIRMVAL"); 
				enterTrader	 	= resultset.getInt("ER_ENTERTRADER"); // 36
				enterTraderVal 	= resultset.getString("ER_ENTERTRADERVAL1"); 
				contraTrader	= resultset.getInt("ER_CONTRATRADER"); // 37	
				contraTraderVal = resultset.getString("ER_EXECTRADERVAL"); 
			}
				

		}
		System.out.println("----------------------------------------");
		System.out.println(" VALIDACION DEL EXECUTION REPORT\n ");

		for (int i = 0; i < cad.size(); i++) {
			etiquetaFix = cad.get(i).split("=")[0];
			valorFix = cad.get(i).split("=")[1];
			switch (etiquetaFix) {
			case "880":
				if (valorFix.equals(trdmatchId)) {
					contadorBuenos++;

					cadenaDeMensaje(" ER_TRMATCHID ", etiquetaFix, trdmatchId);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), valorFix, trdmatchId, 
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out.println(
							" ER_TRMATCHID (" + etiquetaFix + ") MSG: " + valorFix + " BD: " + trdmatchId);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), valorFix, trdmatchId, 
							idEscenario, idCase, idSecuencia, etiquetaFix);
					contadorMalos++;
				}
				break;
			case "1623":
				if (valorFix.equals(fillYield)) {
					contadorBuenos++;

					cadenaDeMensaje(" ER_FILLYIELD ", etiquetaFix, fillYield);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), valorFix, fillYield, 
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out.println(
							" ER_FILLYIELD (" + etiquetaFix + ") MSG: " + valorFix + " BD: " + fillYield);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), valorFix, fillYield,
							idEscenario, idCase, idSecuencia, etiquetaFix);
					contadorMalos++;
				}
				break;
			case "14":
				if (valorFix.equals(cumQTy)) {
					contadorBuenos++;

					cadenaDeMensaje(" ER_CUMQTY ", etiquetaFix, cumQTy);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), cumQTy, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out.println(" ER_CUMQTY (" + etiquetaFix + ") MSG: " + valorFix + " BD: " + cumQTy);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), cumQTy, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
					contadorMalos++;
				}
				break;
			case "22":
				if (valorFix.equals(SecurityIDSource)) {
					contadorBuenos++;

					cadenaDeMensaje(" SecurityIDSource ", etiquetaFix, SecurityIDSource);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), SecurityIDSource,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out
							.println(" SecurityIDSource (" + etiquetaFix + ") MSG: " + valorFix + " BD: " + SecurityIDSource);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), SecurityIDSource,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
					contadorMalos++;
				}
				break;
			case "1080":
				if (valorFix.equals(reforderId)) {

					contadorBuenos++;

					cadenaDeMensaje("ER_REFORDERID", etiquetaFix, reforderId);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), reforderId, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out.println(
							" ER_REFORDERID (" + etiquetaFix + ") MSG: " + valorFix + " BD " + reforderId);
					contadorMalos++;
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), reforderId, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				}
				break;
			case "37":
				if (valorFix.equals(orderId)) {

					contadorBuenos++;

					cadenaDeMensaje("ER_ORDERID", etiquetaFix, orderId);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), orderId, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out
							.println(" ER_ORDERID (" + etiquetaFix + ") MSG: " + valorFix + " BD " + orderId);
					contadorMalos++;
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), orderId, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				}
				break;
			case "17":
				if (valorFix.equals(execId)) {

					contadorBuenos++;

					cadenaDeMensaje("ER_EXECID", etiquetaFix, execId);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), execId, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out.println(" ER_EXECID (" + etiquetaFix + ") MSG: " + valorFix + " BD " + execId);
					contadorMalos++;
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), execId, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				}
				break;
			case "11":

				String mensajeS = BasicFunctions.getIdEjecution() + "" + datosCache.getIdCase() + "_S";
				String mensajeAJ = BasicFunctions.getIdEjecution() + "" + datosCache.getIdCase() + "_AJ";
				
				if (message.getHeader().getString(TargetCompID.FIELD).equals(BasicFunctions.getIniciator())) {			
					if (message.getString(11).equals(mensajeAJ)) {

						DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), mensajeAJ,
								valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
					} else {
						DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), mensajeAJ,
								valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
					}

				} else {
					if (message.getString(11).equals(mensajeS)) {
						DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), mensajeS,
								valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
						
					} else {
						DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), mensajeS,
								valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
						
					}

				}
				
				break;
//			case "57":
//				if (valorFix.equals(targetSubId)) {
//
//					contadorBuenos++;
//
//					cadenaDeMensaje("RQ_TRADER", etiquetaFix, targetSubId);
//
//					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), targetSubId,
//							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
//				} else {
//					System.out.println(
//							" RQ_TRADER (" + etiquetaFix + ") MSG: " + valorFix + " BD " + targetSubId);
//					contadorMalos++;
//					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), targetSubId,
//							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
//				}
//				break;
//			case "56":
//				if (valorFix.equals(targetComId)) {
//					contadorBuenos++;
//
//					cadenaDeMensaje("ID_AFILIADO", etiquetaFix, targetComId);
//
//					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), targetComId,
//							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
//				} else {
//					System.out.println(
//							" ID_AFILIADO (" + etiquetaFix + ") MSG: " + valorFix + " BD " + targetComId);
//					contadorMalos++;
//					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), targetComId,
//							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
//				}
//				break;
			case "49":
				if (valorFix.equals(SenderCompID)) {
					contadorBuenos++;

					cadenaDeMensaje(" SenderCompID ", etiquetaFix, SenderCompID);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), SenderCompID,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out.println(" SenderCompID (" + etiquetaFix + "): MSG" + valorFix + " BD: " + SenderCompID);
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), SenderCompID,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);

					contadorMalos++;
				}
				break;
			case "8":
				if (valorFix.equals(beginString)) {
					contadorBuenos++;

					cadenaDeMensaje(" beginString ", etiquetaFix, beginString);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), beginString,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out.println(
							" beginString (" + etiquetaFix + "): MSG" + valorFix + " BD: " + beginString);
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), beginString,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
					contadorMalos++;
				}
				break;
			case "150":
				if (valorFix.equals(execType)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_EXECTYPE", etiquetaFix, execType);
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), execType, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out.println(
							" ER_EXECTYPE (" + etiquetaFix + "): MSG" + valorFix + " bd: " + execType);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), execType, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
					contadorMalos++;
				}
				break;
			case "39":
				if (valorFix.equals(orderStatus)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_ORDSTATUS", etiquetaFix, orderStatus);
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), orderStatus,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out.println(
							" ER_ORDSTATUS (" + etiquetaFix + "): MSG" + valorFix + " BD: " + orderStatus);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), orderStatus,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
					contadorMalos++;
				}
				break;
			case "54":
				if (valorFix.equals(side)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_SIDE", etiquetaFix, side);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), side, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out.println(" ER_SIDE (" + etiquetaFix + "): MSG" + valorFix + " BD: " + side);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), side, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
					contadorMalos++;
				}
				break;
			case "151":
				if (valorFix.equals(leaveQty)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_LEAVEQTY", etiquetaFix, leaveQty);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), leaveQty, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out.println(
							" ER_LEAVEQTY (" + etiquetaFix + "): MSG" + valorFix + " BD: " + leaveQty);
					contadorMalos++;
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), leaveQty, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				}
				break;
			case "38":
				if (valorFix.equals(orderQty)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_ORDERQTY", etiquetaFix, orderQty);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), orderQty, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					System.out.println(
							" ER_ORDERQTY (" + etiquetaFix + "): MSG" + valorFix + " bd: " + orderQty);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), orderQty, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
					contadorMalos++;
				}
				break;
			case "44":
				if (valorFix.equals(price)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_PRICE", etiquetaFix, price);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), price, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), price, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
					System.out.println(" ER_PRICE (" + etiquetaFix + "): MSG" + valorFix + " bd: " + price);
					contadorMalos++;
				}
				break;
			case "381":
				if (valorFix.equals(grosstradeamt)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_GROSSTRADEAMT", etiquetaFix, grosstradeamt);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), grosstradeamt,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), grosstradeamt,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
					System.out.println(" ER_GROSSTRADEAMT(" + etiquetaFix + "): MSG" + valorFix + " BD: "
							+ grosstradeamt);
					contadorMalos++;
				}
				break;
			case "453":
				if (valorFix.equals(nopartyIds)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_NOPARTYIDS", etiquetaFix, nopartyIds);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), nopartyIds, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), nopartyIds, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
					System.out.println(
							" ER_NOPARTYIDS (" + etiquetaFix + "): MSG" + valorFix + " BD: " + nopartyIds);
					contadorMalos++;
				}
				break;
			case "762":
				if (valorFix.equals(secSubtype)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_SECSUBTYPE", etiquetaFix, secSubtype);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), secSubtype, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), secSubtype, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
					System.out.println(
							" ER_SECSUBTYPE (" + etiquetaFix + "): MSG" + valorFix + " BD: " + secSubtype);
					contadorMalos++;
				}
				break;
			case "1081":
				if (valorFix.equals(reforderIdsCr)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_REFORDERIDSCR", etiquetaFix, reforderIdsCr);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), reforderIdsCr,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), reforderIdsCr,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
					System.out.println(" ER_REFORDERIDSCR (" + etiquetaFix + "): MSG" + valorFix + " BD: "
							+ reforderIdsCr);
					contadorMalos++;
				}
				break;
			case "55":
				if (valorFix.equals(symbol)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_SYMBOL", etiquetaFix, symbol);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), symbol, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), symbol, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
					System.out.println(" ER_SYMBOL (" + etiquetaFix + "): MSG" + valorFix + " BD: " + symbol);
					contadorMalos++;
				}
				break;
			case "50":
				if (valorFix.equals(senderSubId)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_SENDERSUBID", etiquetaFix, senderSubId);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), senderSubId,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), senderSubId,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
					System.out.println(
							" ER_SENDERSUBID (" + etiquetaFix + "): MSG" + valorFix + " BD: " + senderSubId);
					contadorMalos++;
				}
				break;
			case "20102":
				if (valorFix.equals(dirtyPrice)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_DIRTYPRICE", etiquetaFix, dirtyPrice);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), dirtyPrice, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), dirtyPrice, valorFix,
							idEscenario, idCase, idSecuencia, etiquetaFix);
					System.out.println(
							" ER_DIRTYPRICE (" + etiquetaFix + "): MSG" + valorFix + " BD: " + dirtyPrice);
					contadorMalos++;
				}
				break;
			case "447":
				if (valorFix.equals(partyIdsSource)) {
					contadorBuenos++;

					cadenaDeMensaje("ER_PARTYIDSOURCE", etiquetaFix, partyIdsSource);

					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), partyIdsSource,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
				} else {
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), partyIdsSource,
							valorFix, idEscenario, idCase, idSecuencia, etiquetaFix);
					System.out.println(" ER_PARTYIDSOURCE (" + etiquetaFix + "): MSG" + valorFix + " BD: "
							+ partyIdsSource);
					contadorMalos++;
				}
				break;

			default:
				break;
			}
		}
		
		//SE COMPARAN LOS VALORES EN LOS GRUPOS REPETITIVOS DE FIRMAS (453)
		List<Group> groupParties = message.getGroups(NoPartyIDs.FIELD);
		Map<Integer, String> grupos = new TreeMap<Integer,String>();
							
		for(Group firma:groupParties) {			
			grupos.put(firma.getInt(PartyRole.FIELD), firma.getString(PartyID.FIELD));	//(452, 448)	
		}
		System.out.println("************************");
		System.out.println("*** Grupos extraidos ***");
		System.out.println("************************");
		
		Iterator<Integer> it = grupos.keySet().iterator();
		while (it.hasNext()) {
			Integer key = it.next();
			String firm = grupos.get(key);
			System.out.println("Clave: " + key + " -> Valor: " + grupos.get(key));
			
			switch(key) {
			case 1: 
				//Se compara el campo PartyRole(452) que es el NUMERO que identifica cada party
				if (key.equals(execFirm)) {
					contadorBuenos++;
					cadenaDeMensaje("PartyRole", key.toString(), ""+execFirm);
					
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), key.toString(), execFirm+"",
							idEscenario, idCase, idSecuencia, "452");
				} else {
					System.out.println(" PartyRole (452). MSG: " + key + " - DB: " + execFirm);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), key.toString(), execFirm+"",
							idEscenario, idCase, idSecuencia, "452");
					contadorMalos++;
				}	
				
				//Se compara el campo PartyID(448) que es el NOMBRE que identifica cada party
				if (firm.equals(execFirmVal)) {
					contadorBuenos++;
					cadenaDeMensaje("PartyID", firm, execFirmVal);
					
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), firm, execFirmVal,
							idEscenario, idCase, idSecuencia, "448");
				} else {
					System.out.println(" PartyID (448). MSG: " + firm + " - DB: " + execFirmVal);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), firm, execFirmVal,
							idEscenario, idCase, idSecuencia, "448");
					contadorMalos++;
				}			
				
				break;
			
			case 10: 
				//Se compara el campo PartyRole(452) que es el NUMERO que identifica cada party
				if (key.equals(setLoc)) {
					contadorBuenos++;
					cadenaDeMensaje("PartyRole", key.toString(), ""+setLoc);
					
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), key.toString(), setLoc+"",
							idEscenario, idCase, idSecuencia, "452");
				} else {
					System.out.println(" PartyRole (452). MSG: " + key + " - DB: " + setLoc);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), key.toString(), setLoc+"",
							idEscenario, idCase, idSecuencia, "452");
					contadorMalos++;
				}
				
				//Se compara el campo PartyID(448) que es el NOMBRE que identifica cada party
				if (firm.equals(setLocVal)) {
					contadorBuenos++;
					cadenaDeMensaje("PartyID", firm, setLocVal);
					
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), firm, setLocVal,
							idEscenario, idCase, idSecuencia, "448");
				} else {
					System.out.println(" PartyID (448). MSG: " + firm + " - DB: " + setLocVal);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), firm, setLocVal,
							idEscenario, idCase, idSecuencia, "448");
					contadorMalos++;
				}			
				
				break;
			
				 
			case 12: 
				//Se compara el campo PartyRole(452) que es el NUMERO que identifica cada party
				if (key.equals(execTrader)) {
					contadorBuenos++;
					cadenaDeMensaje("PartyRole", key.toString(), ""+execTrader);
					
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), key.toString(), execTrader+"",
							idEscenario, idCase, idSecuencia, "452");
				} else {
					System.out.println(" PartyRole (452). MSG: " + key + " - DB: " + execTrader);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), key.toString(), execTrader+"",
							idEscenario, idCase, idSecuencia, "452");
					contadorMalos++;
				}
				
				//Se compara el campo PartyID(448) que es el NOMBRE que identifica cada party
				if (firm.equals(execTraderVal)) {
					contadorBuenos++;
					cadenaDeMensaje("PartyID", firm, execTraderVal);
					
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), firm, execTraderVal,
							idEscenario, idCase, idSecuencia, "448");
				} else {
					System.out.println(" PartyID (448). MSG: " + firm + " - DB: " + execTraderVal);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), firm, execTraderVal,
							idEscenario, idCase, idSecuencia, "448");
					contadorMalos++;
				}			
				
				break;
			
				 
			case 17: 
				//Se compara el campo PartyRole(452) que es el NUMERO que identifica cada party
				if (key.equals(contraFirm)) {
					contadorBuenos++;
					cadenaDeMensaje("PartyRole", key.toString(), ""+contraFirm);
					
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), key.toString(), contraFirm+"",
							idEscenario, idCase, idSecuencia, "452");
				} else {
					System.out.println(" PartyRole (452). MSG: " + key + " - DB: " + contraFirm);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), key.toString(), contraFirm+"",
							idEscenario, idCase, idSecuencia, "452");
					contadorMalos++;
				}
				
				//Se compara el campo PartyID(448) que es el NOMBRE que identifica cada party
				if (firm.equals(contraFirmVal)) {
					contadorBuenos++;
					cadenaDeMensaje("PartyID", firm, contraFirmVal);
					
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), firm, contraFirmVal,
							idEscenario, idCase, idSecuencia, "448");
				} else {
					System.out.println(" PartyID (448). MSG: " + firm + " - DB: " + contraFirmVal);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), firm, contraFirmVal,
							idEscenario, idCase, idSecuencia, "448");
					contadorMalos++;
				}			
				
				break;
			
				 	
			case 36: 
					//Se compara el campo PartyRole(452) que es el NUMERO que identifica cada party
				if (key.equals(enterTrader)) {
					contadorBuenos++;
					cadenaDeMensaje("PartyRole", key.toString(), ""+enterTrader);
					
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), key.toString(), enterTrader+"",
							idEscenario, idCase, idSecuencia, "452");
				} else {
					System.out.println(" PartyRole (452). MSG: " + key + " - DB: " + enterTrader);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), key.toString(), enterTrader+"",
							idEscenario, idCase, idSecuencia, "452");
					contadorMalos++;
				}
				
				//Se compara el campo PartyID(448) que es el NOMBRE que identifica cada party
				if (firm.equals(enterTraderVal)) {
					contadorBuenos++;
					cadenaDeMensaje("PartyID", firm, enterTraderVal);
					
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), firm, enterTraderVal,
							idEscenario, idCase, idSecuencia, "448");
				} else {
					System.out.println(" PartyID (448). MSG: " + firm + " - DB: " + enterTraderVal);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), firm, enterTraderVal,
							idEscenario, idCase, idSecuencia, "448");
					contadorMalos++;
				}			
				
				break;
			
			
			case 37: 
				//Se compara el campo PartyRole(452) que es el NUMERO que identifica cada party
				if (key.equals(contraTrader)) {
					contadorBuenos++;
					cadenaDeMensaje("PartyRole", key.toString(), " "+contraTrader);
					
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), key.toString(), contraTrader+"",
							idEscenario, idCase, idSecuencia, "452");
				} else {
					System.out.println(" PartyRole (452). MSG: " + key + " - DB: " + contraTrader);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), key.toString(), contraTrader+"",
							idEscenario, idCase, idSecuencia, "452");
					contadorMalos++;
				}
				
				//Se compara el campo PartyID(448) que es el NOMBRE que identifica cada party
				if (firm.equals(contraTraderVal)) {
					contadorBuenos++;
					cadenaDeMensaje("PartyID", firm, contraTraderVal);
					
					DataAccess.cargarLogsExitosos(message, datosCache.getIdEjecucion(), firm, contraTraderVal,
							idEscenario, idCase, idSecuencia, "448");
				} else {
					System.out.println(" PartyID (448). MSG: " + firm + " - DB: " + contraTraderVal);
					DataAccess.cargarLogsFallidos(message, datosCache.getIdEjecucion(), firm, contraTraderVal,
							idEscenario, idCase, idSecuencia, "448");
					contadorMalos++;
				}			
				
				break;
				
				default: 
					System.out.println("*************************************");
					System.out.println("*** PAR NO ESPERADO: CLAVE: " + key + " -> "+ firm + "***");
					System.out.println("*************************************");
				
			}
			
		}

		System.out.println("----------------------------------------");
		System.out.println("---------------------");
		System.out.println("-- VALIDACIONES ER --");
		System.out.println("LAS VALIDACIONES CORRECTAS FUERON : " + contadorBuenos);
		System.out.println("LAS VALIDACIONES ERRADAS FUERON : " + contadorMalos);
		System.out.println("TOTAL VALIDACIONES REALIZADAS : " + (contadorBuenos + contadorMalos));

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
