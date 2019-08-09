package co.com.bvc.aut_tcr.orquestador;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import co.com.bvc.aut_tcr.amp.TradeCancelAmp;
import co.com.bvc.aut_tcr.basicfix.BasicFunctions;
import co.com.bvc.aut_tcr.basicfix.CreateMessage;
import co.com.bvc.aut_tcr.basicfix.DataAccess;
import co.com.bvc.aut_tcr.basicfix.Login;
import co.com.bvc.aut_tcr.dao.domain.AutFixRfqDatosCache;
import co.com.bvc.aut_tcr.dao.domain.RespuestaConstrucccionMsgFIX;
import co.com.bvc.aut_tcr.reporte.CreateReport;
import quickfix.ConfigError;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;

public class AutoEngine {

	CreateMessage createMesage = new CreateMessage();
	public static Login login;
	public static AdapterIO adapterIO;
	
	// metodo que inicia la ejecucion
	public void iniciarEjecucion(int escenarioInicial, int escenarioFinal)
			throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {

//		BasicFunctions.createConn();
//		int firsIdCaseSec = BasicFunctions.getFirtsIdCaseSeq(escenarioEjecucion);
//		BasicFunctions.setEscenarioPrueba(escenarioEjecucion);
//		BasicFunctions.setEscenarioFinal(escenarioFinal);
		DataAccess.getConnection();
		int firsIdCaseSec = BasicFunctions.getFirtsIdCaseSeq(escenarioInicial);
		BasicFunctions.setEscenarioPrueba(escenarioInicial);
		BasicFunctions.setEscenarioFinal(escenarioFinal);
		if (firsIdCaseSec > 0) {
			BasicFunctions.startVariables();
			adapterIO = new AdapterIO();
			login = new Login(adapterIO);
			DataAccess.limpiarCache();
			ejecutarSiguientePaso();
			
//			Login.createLogin();
//			DataAccess.limpiarCache();
//			
//			ejecutarSiguientePaso();
		} else {
			System.out.println("NO HAY DATOS EN LA BASE DE DATOS...");
		}
	}

	public void ejecutarSiguientePaso()
			throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {
		
		System.out.println("SIGUIENTE PASO ID_CASESEQ: " + BasicFunctions.getIdCaseSeq());
		ResultSet rsDatos = DataAccess.datosMensaje(BasicFunctions.getIdCaseSeq());
		
		int total = 0;
		while(rsDatos.next()) {
			total++;
		}
		
		if (total > 0) {
			rsDatos.beforeFirst();
			while (rsDatos.next()) {
				
				int caso = rsDatos.getString("ID_CASE") == null ? 0 : rsDatos.getInt("ID_CASE");
	
				BasicFunctions.setIdCase(caso);
				System.out.println("Continua con el siguiente paso.");
				System.out.println("************************** " + caso);
				
				if(BasicFunctions.getIdCase()>0 && BasicFunctions.getIdCase()<=BasicFunctions.getEscenarioFinal()) {
					
					enviarMensaje(rsDatos);
					Thread.sleep(2000);
					BasicFunctions.setIdCaseSeq(BasicFunctions.getIdCaseSeq() + 1);
					System.out.println("++++++++++++++++ SECUENCIA INCREMENTADA A " + BasicFunctions.getIdCaseSeq() + "++++++++++++++++");
				} else {
					generarReporte();
	//				System.out.println("\nGENERANDO REPORTE...");
	//				CreateReport.maina();
	//				login.endSessions();
	//				System.out.println("++++++++++++++++++++++++++++++++++++++++++++");
	//				System.out.println("++++++++++++++ FIN DE EJECUCION ++++++++++++");
	//				System.out.println("++++++++++++++++++++++++++++++++++++++++++++");
				}
	
			}
		
//		int caso = BasicFunctions.getEscenarioFinal();
////		Thread.sleep(5000);
//
//		System.out.println("ID_CASESEQ: " + BasicFunctions.getIdCaseSeq());
//		
//		ResultSet rsDatos = DataAccess.datosMensaje(BasicFunctions.getIdCaseSeq());
//		Thread.sleep(5000);
//		
//		if(rsDatos != null) {
//			System.out.println("INGRESA AL IF");
////			while (rsDatos.next()) {
////				System.out.println("INGRESA AL WHILE");
////				BasicFunctions.setIdCase(rsDatos.getInt("ID_CASE"));				
////				if (BasicFunctions.getIdCase() > BasicFunctions.getEscenarioFinal()) {
////					generarReporte();
////				} else {
////					
////					System.out.println("INGRESA AL IF ESCENARIOFINAL");
////					System.out.print("\n**************************************\n*** INICIA REGISTRO " + BasicFunctions.getIdCaseSeq());
////					System.out.print(", ESCENARIO " + BasicFunctions.getIdCase() + " ***\n**************************************\n");
////					
////					enviarMensaje(rsDatos);
////					Thread.sleep(5000);
////				}
////			}
////			System.out.println("RESULTSET NULO");
//			while (rsDatos.next()) {
//
//				BasicFunctions.setIdCase(rsDatos.getInt("ID_CASE"));
//				System.out.println("Continua con el siguiente paso.");
//				System.out.println("************************** " + caso);
//
//				if (caso < BasicFunctions.getIdCase() ) {
//					Thread.sleep(5000);
//					System.out.println("++++++++++++++++++++++++++++++++++++++++++++");
//					System.out.println("++++++++++++++ FIN DE EJECUCION ++++++++++++");
//					System.out.println("++++++++++++++++++++++++++++++++++++++++++++");
//					caso++;
//					System.out.println("GENERAR REPORTE....");
//					CreateReport.maina();
//					BasicFunctions.FinalLogin();
//				} else {
//					enviarMensaje(rsDatos);
//					Thread.sleep(5000);
//					BasicFunctions.setIdCaseSeq(BasicFunctions.getIdCaseSeq() + 1);
//					System.out.println("++++++++++++++++ SECUENCIA ++++++++ " + BasicFunctions.getIdCaseSeq());
//				}
//				}
//
		} else {
			generarReporte();
		}
	}
	
	public void enviarMensaje(ResultSet resultSet)
				throws SessionNotFound, SQLException, InterruptedException, FieldNotFound {
	
			String idEscenario = resultSet.getString("ID_ESCENARIO");
			String idAfiliado = resultSet.getString("ID_AFILIADO");
			String contrafirm = resultSet.getString("CONTRAFIRM");
			String idCase = resultSet.getString("ID_CASE");
			
			
//			String idCase = resultSet.getString("ID_CASE");
//			System.out.println(resultSet);
			AutFixRfqDatosCache datosCache = new AutFixRfqDatosCache();
			RespuestaConstrucccionMsgFIX respConstruccion = new RespuestaConstrucccionMsgFIX();
	
			switch (idEscenario) {
				
			case "FIX_AE":
				
				System.out.println("************************************************************");
				System.out.println("************************************************************");
				System.out.println("********                                            ********");
				System.out.println("********  COMIENZA ESCENARIO "+ idCase + "                      ********");
				System.out.println("********                                            ********");
				System.out.println("************************************************************");
				System.out.println("************************************************************");
				
				
				
				System.out.println("**************************************");
				System.out.println("** CREANDO MENSAJE FIX_AE INICIADOR **");
				System.out.println("**************************************");
				
				BasicFunctions.setIniciator(idAfiliado);
				BasicFunctions.setReceptor(contrafirm);
				BasicFunctions.setLastAE(false);
				
				respConstruccion = createMesage.createAE(resultSet);
				
				for(String session : respConstruccion.getListSessiones()) {
					System.out.println("session en autoengine: "+session);
					
					// Construir mensaje a cache.
					datosCache.setReceiverSession(session);
					datosCache.setIdCaseseq(resultSet.getInt("ID_CASESEQ"));
					datosCache.setIdCase(resultSet.getInt("ID_CASE"));
					datosCache.setIdSecuencia(resultSet.getInt("ID_SECUENCIA"));
					datosCache.setEstado(resultSet.getString("ESTADO"));
					datosCache.setIdAfiliado(resultSet.getString("ID_AFILIADO"));
					datosCache.setIdEjecucion(BasicFunctions.getIdEjecution());
	
					cargarCache(datosCache);
					
				}
				System.out.println("MENSAJE\n"+respConstruccion.getMessage()+ "SESION: "+Login.getSessionOfAfiliado(idAfiliado));
				
				Session.sendToTarget(respConstruccion.getMessage(), Login.getSessionOfAfiliado(idAfiliado));
				
				System.out.println("MENSAJE AE ENVIADO");
				System.out.println("SESSIONES CREADAS: ");
				DataAccess.mostrarCache();
				
				break;
				

	        case "FIX_AE_R":
				
	        	 System.out.println("*************************************");
	        	 System.out.println("** CREANDO MENSAJE FIX_AE RECEPTOR **");
	        	 System.out.println("*************************************");
				
	        	 respConstruccion = createMesage.createAE(resultSet);
				
	        	 for(String session : respConstruccion.getListSessiones()) {
					
					// Construir mensaje a cache.
					datosCache.setReceiverSession(session);
					datosCache.setIdCaseseq(resultSet.getInt("ID_CASESEQ"));
					datosCache.setIdCase(resultSet.getInt("ID_CASE"));
					datosCache.setIdSecuencia(resultSet.getInt("ID_SECUENCIA"));
					datosCache.setEstado(resultSet.getString("ESTADO"));
					datosCache.setIdAfiliado(resultSet.getString("ID_AFILIADO"));
					datosCache.setIdEjecucion(BasicFunctions.getIdEjecution());
	 
					cargarCache(datosCache);
					
				}
	            
	            Session.sendToTarget(respConstruccion.getMessage(), Login.getSessionOfAfiliado(idAfiliado));
	            
	            System.out.println("MENSAJE AE_R ENVIADO");  
	            System.out.println("SESSIONES CREADAS: ");
				DataAccess.mostrarCache();
				
				
				break;
				
	
			default:
				System.out.println("ID_ESCENARIO ENCONTRADO: "+idEscenario);
				break;
			}
	
		}

	public void generarReporte() throws SQLException, IOException, InterruptedException {
		System.out.println("\nGENERAR REPORTE....");
		CreateReport.maina();
		login.endSessions();
		Thread.sleep(6000);
		
		System.out.println("*******************************************");
		System.out.println("************* FIN DE EJECUCION ************");
		System.out.println("*******************************************");
				
	}

	// Metodo que guarda el registro en base de datos
	public void cargarCache(AutFixRfqDatosCache datosCache) throws SQLException, InterruptedException {
		DataAccess.cargarCache(datosCache);
	}

	// Metodo que elimina el registro en cache (base de datos)
	public void eliminarDatoCache(String session) throws SQLException, InterruptedException {

		String queryDelete = "DELETE FROM aut_fix_rfq_cache WHERE RECEIVER_SESSION = " + "'" + session
				+ "'" + ";";
		System.out.println("QUERY BORRADO: " + queryDelete);

		DataAccess.setQuery(queryDelete);
//		Thread.sleep(2000);
//		DataAccess.mostrarCache();
	}

	// Metodo que extraer el registro en base de datos
	public AutFixRfqDatosCache obtenerCache(String session) throws SQLException, InterruptedException {

		System.out.println("SESS: " + session);
		return DataAccess.obtenerCache(session);

	}
    
	public void validarAR(SessionID sessionId, Message message) throws SQLException, InterruptedException, FieldNotFound, SessionNotFound, IOException, ConfigError {
		
		System.out.println("**************************");
		System.out.println("** INGRESA A VALIDAR AR **");
		System.out.println("**************************");
		
//		String sIdAfiliado = sessionId.toString().substring(8, 11);
		String sIdAfiliado = sessionId.getSenderCompID();
		System.out.println("AFILIADO: " + sIdAfiliado);
		AutFixRfqDatosCache datosCache = obtenerCache(sIdAfiliado);
		
		eliminarDatoCache(sIdAfiliado);
		
		Validaciones validaciones = new Validaciones();
		validaciones.validarAR(datosCache, (quickfix.fix44.Message) message);
		
		if(message.isSetField(58)) {
			DataAccess.limpiarCache();
			ejecutarSiguienteEscenario();
		} else {
			
			if(DataAccess.validarContinuidadEjecucion()) {
				System.out.println("*** CACHE VACIO. LISTO SIGUIENTE PASO ***");
				ejecutarSiguientePaso();				
			}else {
				System.out.println("*** AR - DATOS EN CACHE. VALIDACIONES PENDIENTES ***");
				DataAccess.mostrarCache();
			}		
		}
	}

	public void validarAE(SessionID sessionId, Message message) throws SQLException, InterruptedException, SessionNotFound, IOException, FieldNotFound, ConfigError {
		
		System.out.println("**************************");
		System.out.println("** INGRESA A VALIDAR AE **");
		System.out.println("**************************");
		
//		String sIdAfiliado = sessionId.toString().substring(8, 11);
		String idAfiliado = sessionId.getSenderCompID();
		System.out.println("AFILIADO: " + idAfiliado);
		
		//Si es el �ltimo AE se emplean la sesiones en cach� preparadas para validar ER.
		String idCacheAfiliado = BasicFunctions.isLastAE() ? idAfiliado+"_1" : idAfiliado;
		
		AutFixRfqDatosCache datosCache = obtenerCache(idCacheAfiliado);
		
		Validaciones validaciones = new Validaciones();
		validaciones.validarAE(datosCache, (quickfix.fix44.Message) message);
		
		int valueTRType = message.isSetField(856) ? message.getInt(856) : 0;
		
		eliminarDatoCache(idCacheAfiliado);
		
		//Cuando llegue el mensaje de aceptaci�n al iniciador se dispara la aprobaci�n por bolsa
		if(valueTRType == 99 && idAfiliado.equals(BasicFunctions.getIniciator())) {
			String trMatchId = message.getString(880);
			int decisionDVC = BasicFunctions.getAmpDcvDecition();
			String userInet = "su1";
			String passInet = "";
			
			switch(decisionDVC) {
			case 0:
				BasicFunctions.setLastAE(true);
				boolean rejectBVC = TradeCancelAmp.tradeCancelReject(userInet, passInet, trMatchId);
				
				if(rejectBVC) {
					System.out.println("RECHAZO EXITOSO...");
				} else {
					System.out.println("RECHAZO NO EXITOSO...");
				}	
				break;
				
			case 1:
//				BasicFunctions.setLastAE(false);
				boolean approveBVC = TradeCancelAmp.tradeCancelApprove(userInet, passInet, trMatchId);
			
				if(approveBVC) {
					System.out.println("APROBACION EXITOSA...");
				} else {
					System.out.println("APROBACION NO EXITOSA...");
				}	
			}
			
		}
		
		if(valueTRType == 98) {
			System.out.println("SOLICITUD RECHAZADA... SALTA SIGUIENTE ESCENARIO");
		}
		
		if(DataAccess.validarContinuidadEjecucion()) {
			System.out.println("*** CACHE VACIO. LISTO SIGUIENTE PASO ***");
			ejecutarSiguientePaso();				
		}else {
			System.out.println("*** AE - DATOS EN CACHE. VALIDACIONES PENDIENTES ***");
			DataAccess.mostrarCache();
		}
		
	}
	
	public static void printMessage(String typeMsg, SessionID sessionId, Message message) throws FieldNotFound {
		System.out.println("********************\nTIPO DE MENSAJE: " + typeMsg + "- SESSION:" + sessionId
				+ "\nMENSAJE :" + message + "\n----------------------------");

	}

//	public void validarAG(SessionID sessionId, Message message)
//			throws SQLException, InterruptedException, SessionNotFound, IOException, FieldNotFound {
//
//		System.out.println("**************************");
//		System.out.println("** INGRESA A validar AG **");
//		System.out.println("**************************");
//
////		String sIdAfiliado = sessionId.toString().substring(8, 11);
//		String sIdAfiliado = sessionId.getSenderCompID();
//		AutFixRfqDatosCache datosCache = obtenerCache(sIdAfiliado);
//		Validaciones validaciones = new Validaciones();
////		validaciones.validarAG(datosCache, (quickfix.fix44.Message) message);
//
//		// Eliminar Registro en Cache.
//		DataAccess.limpiarCache();
//
//		ejecutarSiguienteEscenario();
//		System.out.println("** CONTINUAR ***");
//
//		System.out.println("*********** SALIENDO DE validarAG ************");
//	}

	public void validarER(SessionID sessionId, Message message)
			throws SQLException, InterruptedException, SessionNotFound, IOException, FieldNotFound, ConfigError {

		System.out.println("**************************");
		System.out.println("** INGRESA A VALIDAR ER **");
		System.out.println("**************************");

//		String sIdAfiliado = sessionId.toString().substring(8, 11);
		String sIdAfiliado = sessionId.getSenderCompID();
		System.out.println("AFILIADO: " + sIdAfiliado);
		AutFixRfqDatosCache datosCache = obtenerCache(sIdAfiliado+"_1");
		Validaciones validaciones = new Validaciones();
		validaciones.validarER(datosCache, (quickfix.fix44.Message) message);

		// Eliminar Registro en Cache.
//		DataAccess.limpiarCache();
		eliminarDatoCache(sIdAfiliado+"_1");
//		Thread.sleep(5000);
		
		if(DataAccess.validarContinuidadEjecucion()) {
			System.out.println("TERMINAN VALIDACIONES DE ER. SALTA SIGUIENTE ESCENARIO....");
			ejecutarSiguientePaso();
		}else {
			System.out.println("*** FALTAN VALIDACIONES DE ER ***");
		}		
		
		System.out.println("*********** SALIENDO DE validarER ************");
	}

	public void ejecutarSiguienteEscenario()
			throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {

		BasicFunctions.setIdCase(BasicFunctions.getIdCase()+1);

		String query = "SELECT * FROM aut_fix_tcr_datos" + " WHERE ID_CASE= " + BasicFunctions.getIdCase()
				+ " ORDER BY ID_CASESEQ ASC LIMIT 1;";
	
		System.out.println("CONSULTA SIGUIENTE CASO: "+query);
		
		ResultSet resultset = DataAccess.getQuery(query);
		
		
			if (resultset.next()) {
				int idCaseSeq = resultset.getInt("ID_CASESEQ");
				
				//El idCaseSeq se reduce en 1 porque al entrar al ejecutarSiguientePaso vuelve a incrementarlo
				BasicFunctions.setIdCaseSeq(idCaseSeq);
				ejecutarSiguientePaso();
			
			} else {
				generarReporte();
			}
		
//		int total = 0;
//		while (resultset.next()){
//		   //Obtienes la data que necesitas...
//		   total++;
//		}
//		System.out.println("El total de registros es : "+total);
		
//		if(total > 0) {
//			resultset.beforeFirst();
//			while (resultset.next()) {
//				int idCaseSeq = resultset.getInt("ID_CASESEQ");
//				
//				//El idCaseSeq se reduce en 1 porque al entrar al ejecutarSiguientePaso vuelve a incrementarlo
//				BasicFunctions.setIdCaseSeq(idCaseSeq);
//				ejecutarSiguientePaso();
//			}
//		} else {
//			generarReporte();
//		}
	}

	public void validar3(SessionID sessionId, Message messageIn)

			throws SQLException, InterruptedException, SessionNotFound, IOException, FieldNotFound {

		System.out.println("*************************");
		System.out.println("** INGRESA A VALIDAR 3 **");
		System.out.println("*************************");

//		String sIdAfiliado = sessionId.toString().substring(8, 11);
		String sIdAfiliado = sessionId.getSenderCompID();
		AutFixRfqDatosCache datosCache = obtenerCache(sIdAfiliado);

		Validaciones validaciones = new Validaciones();
		validaciones.validar3(datosCache, (quickfix.fix44.Message) messageIn);

		// Eliminar Registro en Cache.
//		eliminarDatoCache(sIdAfiliado);
		DataAccess.limpiarCache();
		ejecutarSiguienteEscenario();
		System.out.println("** CONTINUAR ***");
		System.out.println("*********** SALIENDO DE VALIDAR 3 ************");
		Thread.sleep(5000);

	}

}