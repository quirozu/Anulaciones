package co.bvc.com.test;

import java.util.HashMap;
import java.util.Map;

import co.bvc.com.basicfix.BasicFunctions;
import quickfix.Application;
import quickfix.ConfigError;
import quickfix.DefaultMessageFactory;
import quickfix.FileLogFactory;
import quickfix.FileStoreFactory;
import quickfix.MemoryStoreFactory;
import quickfix.MessageFactory;
import quickfix.ScreenLogFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;

public class Login {

	private SessionID sessionID1;
	private SessionID sessionID2;
	private SessionID sessionID3;
	private SessionID sessionID4;
	private SessionID sessionID5;
	private Application application;
	private String SessionID;
	private static Map<String, SessionID> mapSessiones = new HashMap<String, SessionID>();

	public static void addSession(String k, SessionID v) {
		mapSessiones.put(k, v);
	}

	public static SessionID getSessionOfAfiliado(String afiliado) {
		return mapSessiones.get(afiliado);
	}

	public void initiation() {

		application = BasicFunctions.getAdapterIO();

		try {
			sessionID1 = startSession("sessionSettings1_27.cfg");
			if (sessionID1 == null) {
				System.out.println("Conexi�n 1 errada");
			} else {
				Session.lookupSession(sessionID1).logon();
				mapSessiones.put(sessionID1.getSenderCompID(), sessionID1);
				// BasicFunctions.logon(sessionID1);
				System.out.println(
						"SESSION ADICIONADA. " + sessionID1.getSenderCompID() + " : " + sessionID1);
			}

			sessionID2 = startSession("sessionSettings2_35.cfg");
			if (sessionID2 == null) {
				System.out.println("Conexi�n 2 errada");
			} else {
				Session.lookupSession(sessionID2).logon();
//				BasicFunctions.logon(sessionID2);
				mapSessiones.put(sessionID2.getSenderCompID(), sessionID2);

				System.out.println(
						"SESSION ADICIONADA. " + sessionID2.getSenderCompID() + " : " + sessionID2);
			}

			sessionID3 = startSession("sessionSettings2_37.cfg");
			if (sessionID3 == null) {
				System.out.println("Conexi�n 3 errada");
			} else {
				Session.lookupSession(sessionID3).logon();
				mapSessiones.put(sessionID3.getSenderCompID(), sessionID3);
				System.out.println(
						"SESSION ADICIONADA. " + sessionID3.getSenderCompID() + " : " + sessionID3);
			}

			sessionID4 = startSession("sessionSettings1_19.cfg");
			if (sessionID4 == null) {
				System.out.println("Conexi�n 4 errada");
			} else {
				Session.lookupSession(sessionID4).logon();

				mapSessiones.put(sessionID4.getSenderCompID(), sessionID4);

				System.out.println(
						"SESSION ADICIONADA. " + sessionID4.getSenderCompID() + " : " + sessionID4);
			}

			sessionID5 = startSession("sessionSettings1_20.cfg");
			if (sessionID5 == null) {
				System.out.println("Conexi�n 5 errada");
			} else {
				Session.lookupSession(sessionID5).logon();

				mapSessiones.put(sessionID5.getSenderCompID(), sessionID5);

				System.out.println(
						"SESSION ADICIONADA. " + sessionID5.getSenderCompID() + " : " + sessionID5);
			}

			System.out.println("************************************");

			Thread.sleep(3000);

			System.out.println("*************************************");
			System.out.println("***  SESIONES CREADAS E INICIADAS ***");
			System.out.println("*************************************");

		} catch (Exception exp) {
			exp.printStackTrace();
		} finally {
			System.out.println(" ");

		}

	}

	public static Map<String, SessionID> getMapSessiones() {
		return mapSessiones;
	}

	public static void setMapSessiones(Map<String, SessionID> mapSessiones) {
		Login.mapSessiones = mapSessiones;
	}

	public SessionID getSessionID1() {
		return sessionID1;
	}

	public SessionID getSessionID2() {
		return sessionID2;
	}

	public SessionID getSessionID3() {
		return sessionID3;
	}

	public SessionID getSessionID4() {
		return sessionID4;
	}

	public SessionID getSessionID5() {
		return sessionID5;
	}

	public String getSessionID(String Afiliado) {
		return SessionID;
	}

	private SessionID startSession(String fileConf) {
		SocketInitiator socketInitiator = null;
		try {
			SessionSettings sessionSettings = new SessionSettings("resources\\" + fileConf);

			FileStoreFactory fileStoreFactory = new FileStoreFactory(sessionSettings);
			FileLogFactory fileLogFactory = new FileLogFactory(sessionSettings);
			MessageFactory messageFactory = new DefaultMessageFactory();
			socketInitiator = new SocketInitiator(application, fileStoreFactory, sessionSettings, fileLogFactory,
					messageFactory);

			// Se ejecuta onCreate de AdapterIO
			socketInitiator.start();
			SessionID sessionID = socketInitiator.getSessions().get(0);

			return sessionID;
		} catch (Exception e) {
			System.out.println("Error de conexion INET. Mensaje: " + e.getMessage());
			return null;
		}
	}

}
