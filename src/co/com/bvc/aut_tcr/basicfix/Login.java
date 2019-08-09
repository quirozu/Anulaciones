package co.com.bvc.aut_tcr.basicfix;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import co.com.bvc.aut_tcr.orquestador.AdapterIO;
import quickfix.Application;
import quickfix.DefaultMessageFactory;
import quickfix.FileLogFactory;
import quickfix.FileStoreFactory;
import quickfix.MessageFactory;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.SocketInitiator;

public class Login {


	private static Application application;
	private static Map<String, SessionID> mapSessiones = new HashMap<String, SessionID>();
	
	public Login() {
		
	}

	public Login(AdapterIO adapterIO) throws InterruptedException {
		if(application == null) {
			application = adapterIO;
		}
//		application = adapterIO;
		Properties prop = new Properties();
		InputStream is = null;
		
		try {
			is = new FileInputStream("resources//session_setting.properties");
			prop.load(is);
		} catch(IOException e) {
			System.out.println("Excepcion: "+e.toString());
			
				
		}
		
		//Por cada archivo configurado en session_setting.properties se crea una sesion
		for (Enumeration<Object> e = prop.keys(); e.hasMoreElements(); ) {
			// Obtenemos el objeto
			Object obj = e.nextElement();
			String k = obj.toString();
			String v = prop.getProperty(k);
			
			System.out.println("****************************");
			System.out.println(obj + ": " + v);
			
			try {
				SessionID sessionId = startSession(v);
				if (sessionId == null) {
					System.out.println("Conexion "+k+" errada");
				} else {
					Session.lookupSession(sessionId).logon();
					mapSessiones.put(sessionId.getSenderCompID(), sessionId);
					System.out.println("SESSION ADICIONADA. " + sessionId.getSenderCompID() + " : " + sessionId);
					System.out.println("****************************\n");
				}
			} catch (Exception e2) {
				System.out.println(e2.toString());
			}
		}
		
		Thread.sleep(2000);

		System.out.println("*************************************");
		System.out.println("***  SESIONES CREADAS E INICIADAS ***");
		System.out.println("*************************************");
		
	}
	
	private SessionID startSession(String fileConf) {
		SocketInitiator socketInitiator = null;
		try {
			SessionSettings sessionSettings = new SessionSettings(fileConf);

			FileStoreFactory fileStoreFactory = new FileStoreFactory(sessionSettings);
			FileLogFactory fileLogFactory = new FileLogFactory(sessionSettings);
			MessageFactory messageFactory = new DefaultMessageFactory();
			socketInitiator = new SocketInitiator(application, fileStoreFactory, sessionSettings, fileLogFactory,
					messageFactory);

			// Se ejecuta onCreate de AdapterIO
			socketInitiator.start();
			SessionID sessionId = socketInitiator.getSessions().get(0);
			
			return sessionId;
		} catch (Exception e) {
			System.out.println("Error de conexion INET. Mensaje: " + e.getMessage());
			return null;
		}
	}
	
	public void endSessions() {

		try {
			for (Map.Entry<String, SessionID> sesion : mapSessiones.entrySet()) {
				Session.lookupSession(sesion.getValue()).logout();
			}

			System.out.println("**************************");
			System.out.println("***  SESIONES CERRADAS ***");
			System.out.println("**************************");

		} catch (Exception exp) {
			exp.printStackTrace();
		} 
//		finally {
//			System.out.println(" ");
//
//		}

	}

	public static SessionID getSessionOfAfiliado(String afiliado) {
		return mapSessiones.get(afiliado);
	}
	
	public static Map<String, SessionID> getMapSessiones() {
		return mapSessiones;
	}



}
