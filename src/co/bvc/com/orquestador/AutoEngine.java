package co.bvc.com.orquestador;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import co.bvc.com.basicfix.BasicFunctions;
import co.bvc.com.basicfix.DataAccess;
import co.bvc.com.dao.domain.AutFixRfqDatosCache;
import co.bvc.com.dao.domain.RespuestaConstrucccionMsgFIX;
import co.bvc.com.test.CreateMessage;
import co.bvc.com.test.CreateReport;
import co.bvc.com.test.Login;
import co.bvc.com.test.Validaciones;
import quickfix.FieldNotFound;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.field.QuoteStatus;
import quickfix.field.SenderSubID;
import quickfix.field.TargetCompID;
import quickfix.field.Text;
import quickfix.Message;

public class AutoEngine {

	CreateMessage createMesage = new CreateMessage();

	// metodo que inicia la ejecucion
	public void iniciarEjecucion(int escenarioEjecucion, int escenarioFinal)
			throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {

		BasicFunctions.createConn();
		int firsIdCaseSec = BasicFunctions.getFirtsIdCaseSeq(escenarioEjecucion);
		BasicFunctions.setEscenarioPrueba(escenarioEjecucion);
		BasicFunctions.setEscenarioFinal(escenarioFinal);
		if (firsIdCaseSec > 0) {
			BasicFunctions.startVariables();
			BasicFunctions.createLogin();
			DataAccess.limpiarCache();
			ejecutarSiguientePaso();
		} else {
			System.out.println("NO HAY DATOS EN LA BASE DE DATOS...");
		}
	}

	public void ejecutarSiguientePaso()
			throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {

		int caso = BasicFunctions.getEscenarioFinal();
		Thread.sleep(5000);
		System.out.println("ID_CASESEQ: " + BasicFunctions.getIdCaseSeq());
		ResultSet rsDatos = DataAccess.datosMensajeTrc(BasicFunctions.getIdCaseSeq());
		while (rsDatos.next()) {

			BasicFunctions.setIdCase(rsDatos.getInt("ID_CASE"));
			System.out.println("Continua con el siguiente paso.");
			System.out.println("************************** " + caso);

			if (caso < BasicFunctions.getIdCase()) {
				Thread.sleep(5000);
				System.out.println("********************************************");
				System.out.println("************* FIN DE EJECUCION ************");
				System.out.println("********************************************");
				caso++;
				System.out.println("GENERAR REPORTE....");
				CreateReport.maina();
				BasicFunctions.FinalLogin();
			} else {
				enviarMensaje(rsDatos);
				Thread.sleep(5000);
				BasicFunctions.setIdCaseSeq(BasicFunctions.getIdCaseSeq() + 1);
				System.out.println("************* SECUENCIA *************" + BasicFunctions.getIdCaseSeq());
			}

		}

	}
	
	public void ejecutarSiguientePasoTcr()
			throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {

		int caso = BasicFunctions.getEscenarioFinal();
		Thread.sleep(5000);
		System.out.println("ID_CASESEQ: " + BasicFunctions.getIdCaseSeq());
		ResultSet rsDatos = DataAccess.datosMensajeTrc(BasicFunctions.getIdCaseSeq());
		while (rsDatos.next()) {

			BasicFunctions.setIdCase(rsDatos.getInt("ID_CASE"));
			System.out.println("Continua con el siguiente paso.");
			System.out.println("************************** " + caso);

			if (caso < BasicFunctions.getIdCase()) {
				Thread.sleep(5000);
				System.out.println("********************************************");
				System.out.println("************* FIN DE EJECUCION ************");
				System.out.println("********************************************");
				caso++;
				System.out.println("GENERAR REPORTE....");
				CreateReport.maina();
				BasicFunctions.FinalLogin();
			} else {
				enviarMensaje(rsDatos);
				Thread.sleep(5000);
				BasicFunctions.setIdCaseSeq(BasicFunctions.getIdCaseSeq() + 1);
				System.out.println("************* SECUENCIA ************* " + BasicFunctions.getIdCaseSeq());
			}

		}

	}

	public void enviarMensaje(ResultSet resultSet)
			throws SessionNotFound, SQLException, InterruptedException, FieldNotFound {

		String msgType = resultSet.getString("ID_ESCENARIO");
		String idAfiliado = resultSet.getString("ID_AFILIADO");
		String idCase = resultSet.getString("ID_CASE");
		System.out.println(resultSet);
		AutFixRfqDatosCache datosCache = new AutFixRfqDatosCache();
		RespuestaConstrucccionMsgFIX respConstruccion = new RespuestaConstrucccionMsgFIX();

		System.out.println("****************** AFILIADO " + idAfiliado);
		System.out.println("*********************\n" + msgType + "\n*********************");
		switch (msgType) {
			
		case "FIX_AE":
			
			System.out.println("**********************");
			System.out.println("** INGRESA A FIX_AE **");
			System.out.println("**********************");
			
			respConstruccion = createMesage.createAE(resultSet);
			Session.sendToTarget(respConstruccion.getMessage(), Login.getSessionOfAfiliado("001"));
			System.out.println("MENSAJE DE ENVIADO");
			
			break;
			
         case "FIX_AE_R":
			
			System.out.println("**********************");
			System.out.println("** INGRESA A FIX_AE_R **");
			System.out.println("**********************");
			
//			respConstruccion = createMesage.createAE_R(resultSet);
			System.out.println("INGRESA A  ---- EMPEZAR A CREAR MENSAJE DE ER");
			
			break;
			

		default:
			break;
		}

	}

	// Metodo que guarda el registro en base de datos
	public void cargarCache(AutFixRfqDatosCache datosCache) throws SQLException, InterruptedException {
		DataAccess.cargarCache(datosCache);
	}

	// Metodo que elimina el registro en cache (base de datos)
	public void eliminarDatoCache(String session) throws SQLException, InterruptedException {

		String queryDelete = "DELETE FROM bvc_automation_db.aut_fix_rfq_cache WHERE RECEIVER_SESSION = " + "'" + session
				+ "'" + ";";

		DataAccess.setQuery(queryDelete);
	}

	// Metodo que extraer el registro en base de datos
	public AutFixRfqDatosCache obtenerCache(String session) throws SQLException, InterruptedException {

		System.out.println("SESS: " + session);
		return DataAccess.obtenerCache(session);

	}
    
	public void validarAR(SessionID sessionId, Message messageIn) {
		
		System.out.println("*************************");
		System.out.println("** INGRESA A validarAR **");
		System.out.println("*************************");
		
		
	}

	public static void printMessage(String typeMsg, SessionID sessionId, Message message) throws FieldNotFound {
		System.out.println("********************\nTIPO DE MENSAJE: " + typeMsg + "- SESSION:" + sessionId
				+ "\nMENSAJE :" + message + "\n----------------------------");

	}

	public void validarAG(SessionID sessionId, Message message)
			throws SQLException, InterruptedException, SessionNotFound, IOException, FieldNotFound {

		System.out.println("*************************");
		System.out.println("** INGRESA A validar AG **");
		System.out.println("*************************");

		String sIdAfiliado = sessionId.toString().substring(8, 11);
		AutFixRfqDatosCache datosCache = obtenerCache(sIdAfiliado);
		Validaciones validaciones = new Validaciones();
		validaciones.validarAG(datosCache, (quickfix.fix44.Message) message);

		// Eliminar Registro en Cache.
		DataAccess.limpiarCache();

		ejecutarSiguienteEscenario();
		System.out.println("** CONTINUAR ***");

		System.out.println("*********** SALIENDO DE validarAG ************");
	}

	public void ejecutarSiguienteEscenario()
			throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {

		int sec = BasicFunctions.getIdCase();
		sec = sec + 1;
		System.out.println("********* " + sec);
		String query = "SELECT * FROM bvc_automation_db.aut_fix_rfq_datos" + " WHERE ID_CASE= " + sec
				+ " ORDER BY ID_CASESEQ ASC LIMIT 1;";
		System.out.println(query);
		ResultSet resultset = DataAccess.getQuery(query);
		while (resultset.next()) {
			int cas = resultset.getInt("ID_CASESEQ");
			System.out.println("******************* " + cas);
			BasicFunctions.setIdCaseSeq(cas);
			ejecutarSiguientePaso();
		}
	}

	public void validar3(SessionID sessionId, Message messageIn)

			throws SQLException, InterruptedException, SessionNotFound, IOException, FieldNotFound {

		System.out.println("*************************");
		System.out.println("** INGRESA A VALIDAR 3 **");
		System.out.println("*************************");

		String sIdAfiliado = sessionId.toString().substring(8, 11);
		AutFixRfqDatosCache datosCache = obtenerCache(sIdAfiliado);

		Validaciones validaciones = new Validaciones();
		validaciones.validar3(datosCache, (quickfix.fix44.Message) messageIn);

		// Eliminar Registro en Cache.
		eliminarDatoCache(sIdAfiliado);
		DataAccess.limpiarCache();
		ejecutarSiguienteEscenario();
		System.out.println("** CONTINUAR ***");
		System.out.println("*********** SALIENDO DE VALIDAR 3 ************");
		Thread.sleep(5000);

	}

}
