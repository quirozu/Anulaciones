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

	public void iniciarEjecucion(int escenarioEjecucion)
			throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {

		BasicFunctions.createConn();
		int firsIdCaseSec = BasicFunctions.getFirtsIdCaseSeq(escenarioEjecucion);
		BasicFunctions.setEscenarioPrueba(escenarioEjecucion);
		if (firsIdCaseSec > 0) {
			BasicFunctions.startVariables();
			BasicFunctions.createLogin();
			DataAccess.limpiarCache();
			ejecutarSiguientePaso();
		} else {
			System.out.println("NO HAY DATOS EN LA BASE DE DATOS...");
		}
	}
	
	// Metodo que guarda el registro en base de datos
	public void cargarCache(AutFixRfqDatosCache datosCache) throws SQLException, InterruptedException {
		DataAccess.cargarCache(datosCache);
	}

	// Metodo que elimina el registro en cache (base de datos)
	
	public void enviarMensaje(ResultSet resultSet) throws SQLException, FieldNotFound {
		
		String msgType = resultSet.getString("ID_ESCENARIO");
		String idAfiliado = resultSet.getString("ID_AFILIADO");
		System.out.println(resultSet.toString());
		
		AutFixRfqDatosCache datosCache = new AutFixRfqDatosCache();
		RespuestaConstrucccionMsgFIX respConstruccion = new RespuestaConstrucccionMsgFIX();

		System.out.println("*********************\n" + msgType + "\n*********************");
		switch (msgType) {

		case "FIX_AE":
			
			System.out.println("*********************");
			System.out.println("** INGRESA A FIX_EA **");
			System.out.println("*********************");

			respConstruccion = createMesage.createAE(resultSet);
			System.out.println("INGRESA A AE ---- EMPEZAR A CREAR MENSAJE DE EA");
			
			
			break;
		
		case "FIX_AR":
			
			System.out.println("INGRESA A AE ---- EMPEZAR A CREAR MENSAJE DE EA");
			
			break;
			
		default:
			break;
		}
		
		
	}
	
	public void eliminarDatoCache(String session) throws SQLException, InterruptedException {

		String queryDelete = "DELETE FROM bvc_automation_db.aut_fix_tcr_datos WHERE RECEIVER_SESSION = " + "'" + session
				+ "'" + ";";

		DataAccess.setQuery(queryDelete);
	}

	// Metodo que extraer el registro en base de datos
	public AutFixRfqDatosCache obtenerCache(String session) throws SQLException, InterruptedException {

		System.out.println("SESS: " + session);
		return DataAccess.obtenerCache(session);

	}

//	Metodo ejecutar siguiente paso
	
	int caso = BasicFunctions.getEscenarioPrueba();
	
	public void ejecutarSiguientePaso()
			throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {
		
		Thread.sleep(5000);
		System.out.println("ID_CASESEQ: " + BasicFunctions.getIdCaseSeq());
		ResultSet rsDatos = DataAccess.datosMensaje(BasicFunctions.getIdCaseSeq());
		while (rsDatos.next()) {

			BasicFunctions.setIdCase(rsDatos.getInt("ID_CASE"));
			System.out.println("<---------------- SIGUIENTE ---------------->");
			enviarMensaje(rsDatos);
			Thread.sleep(5000);
			BasicFunctions.setIdCaseSeq(BasicFunctions.getIdCaseSeq() + 1);
			System.out.println("******************\n SECUENCIA " + BasicFunctions.getIdCaseSeq() + "\n******************");
			if (caso < BasicFunctions.getIdCase()) {
				Thread.sleep(5000);
				System.out.println("********************************************");
				System.out.println("************* FIN DE EJECUCION *************");
				System.out.println("********************************************");
				caso++;
				System.out.println("Generar reporte....");
				CreateReport.maina();
			}

		}

		System.out.println("FIN EJECUCION....");
	}
	public void ejecutarSiguienteEscenario()
			throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {

		int sec = BasicFunctions.getIdCase();
		sec = sec + 1;
		System.out.println("+++++++++++++++++ " + sec);
		
		String query = "SELECT * FROM bvc_automation_db.aut_fix_tcr_datos" + " WHERE ID_CASE= " + sec
				+ " ORDER BY ID_CASESEQ ASC LIMIT 1;";
		ResultSet resultset = DataAccess.getQuery(query);
		
		while (resultset.next()) {
			int cas = resultset.getInt("ID_CASESEQ");
			System.out.println("*************\n " + cas + "\n*************");
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
