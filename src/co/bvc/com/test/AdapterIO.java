package co.bvc.com.test;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import co.bvc.com.basicfix.BasicFunctions;
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
import quickfix.SessionNotFound;
import quickfix.UnsupportedMessageType;
import quickfix.field.Password;
import quickfix.field.PossDupFlag;
import quickfix.field.Username;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.Logon;
import quickfix.fix44.MessageCracker;
import quickfix.fix44.Quote;
import quickfix.fix44.QuoteCancel;
import quickfix.fix44.QuoteRequest;
import quickfix.fix44.QuoteRequestReject;
import quickfix.fix44.QuoteResponse;
import quickfix.fix44.QuoteStatusReport;
import quickfix.fix44.Reject;

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

		try {
			printMessage("toAdmin - ENTRADA", sessionId, message);
		} catch (FieldNotFound e1) {
			e1.printStackTrace();
		}
		System.out.println("*****************\ntoAdmin - ENTRADA");

		if (message instanceof Logon) {
			try {
				String negociador = message.getHeader().getString(50);
				System.out.println("+++++++++++++++++++++\nNEGOCIADOR: " + negociador);
				ArrayList<String> listUsers = new ArrayList<String>();
				ArrayList<String> listPass = new ArrayList<String>();
				ArrayList<String> listID = new ArrayList<String>();
				
				String queryDatosTrader = "SELECT A.USUARIO , A.CLAVE, A.ID_USUARIO, B.NOM_USUARIO " + 
						" FROM bvc_automation_db.AUT_USUARIO A INNER JOIN bvc_automation_db.aut_fix_rfq_aux_con B " + 
						" ON A.ID_USUARIO = B.ID_USUARIO WHERE A.ESTADO = 'A' AND A.PERFIL_USUARIO = 'FIXCONNECTOR';";
				
				
				ResultSet resultSet = DataAccess.getQuery(queryDatosTrader);
				
				while(resultSet.next()) {
					if(resultSet.getString("NOM_USUARIO").equals(negociador)) {
						message.setField(new Username(resultSet.getString("USUARIO")));
						message.setField(new Password(resultSet.getString("CLAVE")));
					}
				}
				
			} catch (FieldNotFound e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}

			message.setField(new PossDupFlag(true));

		}
		if (message instanceof Reject) {

			try {
				Thread.sleep(5000);
				autoEngine.validar3(sessionId, message);
			} catch (SQLException | InterruptedException | SessionNotFound | IOException e) {
				e.printStackTrace();
			} catch (FieldNotFound e) {
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

			System.out.println(
					"*****************\n toAdmin - SALIDA : \n" + message + "\nPara la sessionId: " + sessionId);
		}

	}

	@Override
	public void toApp(Message message, SessionID sessionId) throws DoNotSend, FieldException {

		try {
			printMessage("toApp", sessionId, message);
		} catch (FieldNotFound e1) {
			e1.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void fromAdmin(Message message, SessionID sessionId)
			throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon, FieldException {

		printMessage("fromAdmin-Input", sessionId, message);

		try {
			crack(message, sessionId);
		} catch (UnsupportedMessageType e) {
			e.printStackTrace();
		}
		if (message instanceof Reject) {

			try {
				Thread.sleep(5000);
				autoEngine.validar3(sessionId, message);
			} catch (SQLException | InterruptedException | SessionNotFound | IOException e) {
				e.printStackTrace();
			} catch (FieldNotFound e) {
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

			System.out.println(
					"*****************\n toAdmin - SALIDA : \n" + message + "\nPara la sessionId: " + sessionId);
		}

	}

	@Override
	public void fromApp(Message message, SessionID sessionId)
			throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType, FieldException {
		try {
			printMessage("fromApp-Input", sessionId, message);

		} catch (Exception e) {
			// TODO: handle exception
		}

		if (message instanceof QuoteRequestReject) {

			try {
				printMessage("MESAJE DE RECHAZO AG ", sessionId, message);
				Thread.sleep(5000);
				autoEngine.validarAG(sessionId, message);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (SessionNotFound e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (FieldException e) {
				e.printStackTrace();
			}
		}

		if (message instanceof Quote) {

			printMessage("MENSAJE S_PRIMA  ", sessionId, message);

			try {
				Thread.sleep(5000);
				autoEngine.validarS(sessionId, message);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (SessionNotFound e) {
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (quickfix.FieldException e) {
				e.printStackTrace();
			}
		}

		if (message instanceof QuoteRequest) {

			String idAfiliado = sessionId.toString().substring(8, 11);
			BasicFunctions.addQuoteReqId(idAfiliado, message.getString(131));

			System.out.println("\nID ESTABLECIDO EN " + BasicFunctions.getQuoteReqIdOfAfiliado(idAfiliado));

			try {
				Thread.sleep(5000);
				autoEngine.validarR(sessionId, message);
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (SessionNotFound e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (quickfix.FieldException e) {
				e.printStackTrace();
			}

		}

		if (message instanceof QuoteStatusReport) {

			printMessage("MENSAJE AI ", sessionId, message);
			try {
				Thread.sleep(5000);
				autoEngine.validarAI(sessionId, message);
			} catch (SQLException | InterruptedException e) {
				e.printStackTrace();
			} catch (SessionNotFound e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (quickfix.FieldException e) {
				e.printStackTrace();
			}

			// Lo hizo Yuli
			if (message instanceof QuoteCancel) {

				printMessage("CANCEL MENSAJE Z ", sessionId, message);

			}
		}

		crack(message, sessionId);
	}

	public void onMessage(ExecutionReport message, SessionID sessionID) throws FieldNotFound {

		printMessage("MENSAJE ER", sessionID, message);

		try {
			Thread.sleep(5000);
			autoEngine.validarAJ(sessionID, message);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SessionNotFound e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

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

		if (message instanceof QuoteCancel) {

			printMessage("QuoteCancel", sessionID, message);

			try {
				Thread.sleep(5000);
				autoEngine.validarZ(sessionID, message);
			} catch (SQLException | InterruptedException | SessionNotFound | IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void printMessage(String typeMsg, SessionID sID, Message msg) throws FieldNotFound {
		System.out.println("********************\nTIPO DE MENSAJE: " + typeMsg + "- SESSION:" + sID + "\nMENSAJE :"
				+ msg + "\n----------------------------");

	}
}