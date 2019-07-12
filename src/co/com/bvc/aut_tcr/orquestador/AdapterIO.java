package co.com.bvc.aut_tcr.orquestador;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import co.com.bvc.aut_tcr.basicfix.BasicFunctions;
import co.com.bvc.aut_tcr.basicfix.DataAccess;
import co.com.bvc.aut_tcr.orquestador.AutoEngine;
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
import quickfix.field.MsgType;
import quickfix.field.Password;
import quickfix.field.PossDupFlag;
import quickfix.field.QuoteReqID;
import quickfix.field.Username;
import quickfix.fix44.BusinessMessageReject;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.Heartbeat;
import quickfix.fix44.Logon;
import quickfix.fix44.Logout;
import quickfix.fix44.MessageCracker;
import quickfix.fix44.Quote;
import quickfix.fix44.QuoteCancel;
import quickfix.fix44.QuoteRequest;
import quickfix.fix44.QuoteRequestReject;
import quickfix.fix44.QuoteResponse;
import quickfix.fix44.QuoteStatusReport;
import quickfix.fix44.QuoteStatusRequest;
import quickfix.fix44.Reject;
import quickfix.fix44.TradeCaptureReport;
import quickfix.fix44.TradeCaptureReportAck;

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
			if(!(message instanceof Heartbeat)) 
			printMessage("toAdmin - ENTRADA", sessionId, message);
		} catch (FieldNotFound e1) {
			e1.printStackTrace();
		}
		System.out.println("*****************\ntoAdmin - ENTRADA");

		if (message instanceof Logon) {
			try {
				String negociador = message.getHeader().getString(50);
				System.out.println("+++++++++++++++++++++\nNEGOCIADOR: " + negociador);
				
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

		} else if(message instanceof Logout) {
			try {
				printMessage("ENVIANDO LOGOUT PARA: ", sessionId, message);
			} catch (FieldNotFound e) {
				e.printStackTrace();
			}
		} else {
			try {
				crack(message, sessionId);
			} catch (UnsupportedMessageType | FieldNotFound | IncorrectTagValue e) {
				e.printStackTrace();
			}
		}
		
//		if (message instanceof Reject) {
//
//			try {
//				Thread.sleep(5000);
//				autoEngine.validar3(sessionId, message);
//			} catch (SQLException | InterruptedException | SessionNotFound | IOException e) {
//				e.printStackTrace();
//			} catch (FieldNotFound e) {
//				e.printStackTrace();
//			}
//
//			try {
//				crack(message, sessionId);
//			} catch (UnsupportedMessageType e) {
//				e.printStackTrace();
//			} catch (FieldNotFound e) {
//				e.printStackTrace();
//			} catch (IncorrectTagValue e) {
//				e.printStackTrace();
//			}
//			
//		}
		System.out.println("*****************\n toAdmin - SALIDA : \n" + message + "\nPara la sessionId: " + sessionId);

	}

	@Override
	public void fromAdmin(Message message, SessionID sessionId)
			throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon, FieldException {

		if(message instanceof Heartbeat) {
			printMessage("CERO... ", sessionId, null);
		} else {
	
			try {
				printMessage("fromAdmin-Input", sessionId, message);
				crack(message, sessionId);
				printMessage("fromAdmin - output", sessionId, null);
			} catch (UnsupportedMessageType e) {
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public void toApp(Message message, SessionID sessionId) throws DoNotSend, FieldException {

		try {
			printMessage(message.getHeader().getString(MsgType.FIELD)+" ENVIADO POR toApp: ", sessionId, message);
		} catch (FieldNotFound e1) {
			e1.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void fromApp(Message message, SessionID sessionId)
			throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType, FieldException {
		
		try {
			printMessage("fromApp-Input", sessionId, message);
			crack(message, sessionId);
			printMessage("fromApp-Output",sessionId, null);
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

	public void onMessage(QuoteRequest message, SessionID sessionId) throws FieldNotFound {
		
//		String idAfiliado = sessionId.toString().substring(8, 11);		
//		BasicFunctions.addQuoteReqId(idAfiliado, message.getString(131));
		
//		try {
//			printMessage("MESAJE R-PRIMA ", sessionId, message);
//			String idAfiliado = sessionId.getSenderCompID();
//			BasicFunctions.addQuoteReqId(idAfiliado, message.getString(QuoteReqID.FIELD));
//			System.out.println("\nID ESTABLECIDO EN " + BasicFunctions.getQuoteReqIdOfAfiliado(idAfiliado));
//			
//			Thread.sleep(5000);
////			autoEngine.validarR(sessionId, message);
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} catch (SessionNotFound e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (quickfix.FieldException e) {
//			e.printStackTrace();
//		}
	}

	public void onMessage(QuoteRequestReject message, SessionID sessionId) throws FieldNotFound, FieldException {
//		try {
//			printMessage("MESAJE AG ", sessionId, message);
//			Thread.sleep(5000);
//			autoEngine.validarAG(sessionId, message);
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} catch (SessionNotFound e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (FieldException e) {
//			e.printStackTrace();
//		}
	}

	public void onMessage(QuoteStatusReport message, SessionID sessionId) throws FieldNotFound {
		
//		try {
//			printMessage("MENSAJE AI ", sessionId, message);
//			Thread.sleep(5000);
//			autoEngine.validarAI(sessionId, message);
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} catch (SessionNotFound e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (FieldException e) {
//			e.printStackTrace();
//		}
	}

	public void onMessage(Quote message, SessionID sessionId) throws FieldNotFound {
		
//		try {
//			printMessage("MENSAJE S-PRIMA  ", sessionId, message);
//			Thread.sleep(5000);
//			autoEngine.validarS(sessionId, message);
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} catch (SessionNotFound e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		} catch (FieldException e) {
//			e.printStackTrace();
//		}
	
	}

	public void onMessage(QuoteCancel message, SessionID sessionId) throws FieldNotFound {
//		try {
//			printMessage("MENSAJE Z ", sessionId, message);
//			Thread.sleep(5000);
//			autoEngine.validarZ(sessionId, message);
//		} catch (SQLException | InterruptedException | SessionNotFound | IOException e) {
//			e.printStackTrace();
//		} catch (FieldNotFound e) {
//			e.printStackTrace();
//		}
	}

	public void onMessage(Reject message, SessionID sessionId) {
		try {
			printMessage("MENSAJE 3 ", sessionId, message);
			Thread.sleep(5000);
			autoEngine.validar3(sessionId, message);
		} catch (SQLException | InterruptedException | SessionNotFound | IOException e) {
			e.printStackTrace();
		} catch (FieldNotFound e) {
			e.printStackTrace();
		}
	}
	
	public void onMessage(ExecutionReport message, SessionID sessionID) throws FieldNotFound {

//		try {
//			printMessage("MENSAJE ER", sessionID, message);
//			Thread.sleep(5000);
//			autoEngine.validarAJ(sessionID, message);
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} catch (SessionNotFound e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}

	}

	public void onMessage(QuoteStatusRequest message, SessionID sessionID) throws FieldNotFound {
		printMessage("QuoteStatusRequest", sessionID, message);
	}
	
	public void onMessage(BusinessMessageReject message, SessionID sessionID) throws FieldNotFound {
		printMessage("BusinessMessageReject", sessionID, message);
	}
	
	public void onMessage(TradeCaptureReportAck message, SessionID sessionID) throws FieldNotFound {
		try {
			printMessage("MENSAJE AR", sessionID, message);
			Thread.sleep(5000);
			autoEngine.validarAR(sessionID, message);
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

	public void onMessage(TradeCaptureReport message, SessionID sessionID) throws FieldNotFound {
		printMessage("TradeCaptureReport", sessionID, message);
	}

	public static void printMessage(String typeMsg, SessionID sID, Message msg) throws FieldNotFound {
		System.out.println("***************************\nTIPO DE MENSAJE: " + typeMsg + "- SESSION:" + sID);
		if(msg != null) System.out.println(msg);
		System.out.println("**************************");
	}
}