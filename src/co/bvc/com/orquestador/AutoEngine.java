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
import quickfix.field.SenderSubID;
import quickfix.field.TargetCompID;
import quickfix.Message;

public class AutoEngine {

	CreateMessage createMesage = new CreateMessage();

	// metodo que inicia la ejecucion
	public void iniciarEjecucion()
			throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {

		BasicFunctions.createConn();
		BasicFunctions.startVariables();
		BasicFunctions.createLogin();

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

	public static void printMessage(String typeMsg, SessionID sessionId, Message message) throws FieldNotFound {
		System.out.println("********************\nTIPO DE MENSAJE: " + typeMsg + "- SESSION:" + sessionId
				+ "\nMENSAJE :" + message + "\n----------------------------");

	}

	public void ejecutarSiguienteEscenario()
			throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {

//		int sec = BasicFunctions.getIdCase();
//		sec = sec + 1;
//		System.out.println("+++++++++++++++++ " + sec);
//		String query = "SELECT * FROM bvc_automation_db.aut_fix_rfq_datos" + " WHERE ID_CASE= " + sec
//				+ " ORDER BY ID_CASESEQ ASC LIMIT 1;";
//		System.out.println(query);
//		ResultSet resultset = DataAccess.getQuery(query);
//		while (resultset.next()) {
//			int cas = resultset.getInt("ID_CASESEQ");
//			System.out.println("+++++++++++++++++++++++++++++++ " + cas);
//			BasicFunctions.setIdCaseSeq(cas);
//			ejecutarSiguientePaso();
//		}
	}

}
