package co.bvc.com.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import co.bvc.com.basicfix.DataAccess;
import co.bvc.com.orquestador.AutoEngine;
import quickfix.Application;
import quickfix.DoNotSend;
import quickfix.FieldException;
import quickfix.FieldNotFound;
import quickfix.IncorrectDataFormat;
import quickfix.IncorrectTagValue;
import quickfix.Message;
import quickfix.RejectLogon;
import quickfix.SessionID;
import quickfix.UnsupportedMessageType;
import quickfix.field.Password;
import quickfix.field.Username;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.Logon;
import quickfix.fix44.MessageCracker;

public class AdapterIO extends MessageCracker implements Application {

	AutoEngine autoEngine = new AutoEngine();

	@Override
	public void onCreate(SessionID sessionId) {
		System.out.println("*****************\nonCreate - sessionId: " + sessionId);

	}

	@Override
	public void onLogon(SessionID sessionId) {
		System.out.println("*****************\nonLogon - sessionId: " + sessionId);

	}

	@Override
	public void onLogout(SessionID sessionId) {
		System.out.println("*****************\nonLogout - sessionId: " + sessionId);

	}

	@Override
	public void toAdmin(Message message, SessionID sessionId) throws FieldException {

		
//		System.out.println("*****************\ntoAdmin - ENTRADA");

		if (message instanceof Logon) {
			try {
				String negociador = message.getHeader().getString(50);
				System.out.println("+++++++++++++++++++++\nNEGOCIADOR: " + negociador);
				ArrayList<String> listUsers = new ArrayList<String>();
				ArrayList<String> listPass = new ArrayList<String>();
				ArrayList<String> listID = new ArrayList<String>();

				String queryDatosTrader = "SELECT A.USUARIO , A.CLAVE, A.ID_USUARIO, B.NOM_USUARIO "
						+ " FROM bvc_automation_db.AUT_USUARIO A INNER JOIN bvc_automation_db.aut_fix_rfq_aux_con B "
						+ " ON A.ID_USUARIO = B.ID_USUARIO WHERE A.ESTADO = 'A' AND A.PERFIL_USUARIO = 'FIXCONNECTOR';";


				ResultSet resultSet = DataAccess.getQuery(queryDatosTrader);

				while (resultSet.next()) {
					if (resultSet.getString("NOM_USUARIO").equals(negociador)) {
						message.setField(new Username(resultSet.getString("USUARIO")));
						message.setField(new Password(resultSet.getString("CLAVE")));
					}
				}

			} catch (FieldNotFound e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}

			try {
				crack(message, sessionId);
			} catch (UnsupportedMessageType e) {
				e.printStackTrace();
			} catch (FieldNotFound e) {
				e.printStackTrace();
			} catch (IncorrectTagValue e) {
				e.printStackTrace();
			}

//			System.out.println(
//					"*****************\n toAdmin - SALIDA : \n" + message + "\nPara la sessionId: " + sessionId);
		}

	}

	@Override
	public void toApp(Message message, SessionID sessionId) throws DoNotSend, FieldException {

	}

	@Override
	public void fromAdmin(Message message, SessionID sessionId)
			throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon, FieldException {

			try {
				crack(message, sessionId);
			} catch (UnsupportedMessageType e) {
				e.printStackTrace();
			} catch (FieldNotFound e) {
				e.printStackTrace();
			} catch (IncorrectTagValue e) {
				e.printStackTrace();
			}

//			System.out.println(
//					"*****************\n toAdmin - SALIDA : \n" + message + "\nPara la sessionId: " + sessionId);
	}

	@Override
	public void fromApp(Message message, SessionID sessionId)
			throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType, FieldException {
		try {
			printMessage("fromApp-Input", sessionId, message);

		} catch (Exception e) {
		}

		crack(message, sessionId);
	}

	public void onMessage(ExecutionReport message, SessionID sessionID) throws FieldNotFound {

		printMessage("MENSAJE ER", sessionID, message);

		

	}

	public void onMessage(quickfix.fix44.QuoteStatusRequest message, SessionID sessionID) throws FieldNotFound {
		printMessage("QuoteStatusRequest", sessionID, message);

	}

	public void onMessage(quickfix.fix44.QuoteStatusReport message, SessionID sessionID) throws FieldNotFound {

	}

	public void onMessage(quickfix.fix44.QuoteRequestReject message, SessionID sessionID)
			throws FieldNotFound, FieldException {
		printMessage("QuoteStatusRequest", sessionID, message);
	}

	public void onMessage(quickfix.fix44.QuoteRequest message, SessionID sessionID) throws FieldNotFound {

	}

	public void onMessage(quickfix.fix44.Quote message, SessionID sessionID) throws FieldNotFound {

	}

	public void onMessage(quickfix.fix44.QuoteCancel message, SessionID sessionID) throws FieldNotFound {

		printMessage("QuoteCancel", sessionID, message);

	}

	public static void printMessage(String typeMsg, SessionID sID, Message msg) throws FieldNotFound {
		System.out.println("********************\nTIPO DE MENSAJE: " + typeMsg + "- SESSION:" + sID + "\nMENSAJE :"
				+ msg + "\n----------------------------");

	}
}