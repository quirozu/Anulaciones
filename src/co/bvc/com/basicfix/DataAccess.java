package co.bvc.com.basicfix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import co.bvc.com.dao.domain.AutFixRfqDatosCache;
import quickfix.FieldNotFound;
import quickfix.Message;

public class DataAccess {

	private static String _usuario;
	private static String _pwd;
	private static String _db;
	private static String HOST;
	private static String PORT;
	private static Connection conn = null;

	public static Connection getConnection() {
		try {
			if (conn == null) {
				ParametersRead p = new ParametersRead();
				String[] lineas = p.leerConexion();
				_usuario = lineas[0].split("=")[1].trim();
				_pwd = lineas[1].split("=")[1].trim();
				_db = lineas[2].split("=")[1].trim();
				HOST = lineas[3].split("=")[1].trim();
				PORT = lineas[4].split("=")[1].trim();
				String driver = "com.mysql.jdbc.Driver";
				//127.0.0.1
				String _url = "jdbc:mysql://" + HOST + ":" + PORT + "/" + _db;
				Class.forName(driver);
				conn = DriverManager.getConnection(_url, _usuario, _pwd);
				System.out.println("Conectionesfull");
			}
		} catch (ClassNotFoundException | SQLException ex) {
			ex.printStackTrace();
		}
		return conn;
	}

	public static ResultSet getQuery(String _query) throws SQLException {
		Statement state = null;
		ResultSet resultSet = null;
		try {
			state = (Statement) conn.createStatement();
			resultSet = state.executeQuery(_query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return resultSet;
	}

	public static int getFirstIdCaseSeq(int escenarioEjecucion) throws SQLException {

		String queryInicio = "SELECT ID_CASESEQ FROM aut_fix_tcr_datos" + " WHERE ID_CASE= "
				+ escenarioEjecucion + " ORDER BY ID_CASESEQ ASC LIMIT 1";

		ResultSet rs = DataAccess.getQuery(queryInicio);
		int idCaseSeq = -1;

		while (rs.next()) {

			idCaseSeq = rs.getInt("ID_CASESEQ");
			BasicFunctions.setIdCaseSeq(idCaseSeq);
			BasicFunctions.imprimir(idCaseSeq);

		}
		return idCaseSeq;
	}
	
	public static ResultSet datosMensaje(int idCaseSeq) throws SQLException {

		String queryDatos = "SELECT * FROM aut_fix_tcr_datos WHERE ID_CASESEQ=" + idCaseSeq;
		ResultSet rsDatos = DataAccess.getQuery(queryDatos);

		return rsDatos;
	}

	public static void setQuery(String _query) throws SQLException {
		Statement state = null;
		int resultSet = 0;
		try {
			state = (Statement) conn.createStatement();
			resultSet = state.executeUpdate(_query);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void cargarCache(AutFixRfqDatosCache datosCache) throws SQLException {

		PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO `aut_fix_rfq_cache` VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
		ps.setString(1, datosCache.getReceiverSession());
		ps.setInt(2, datosCache.getIdCaseseq());
		ps.setInt(3, datosCache.getIdCase());
		ps.setInt(4, datosCache.getIdSecuencia());
		ps.setString(5, datosCache.getEstado());
		ps.setString(6, datosCache.getFixQuoteReqId());
		ps.setString(7, datosCache.getIdAfiliado());
		ps.setLong(8, datosCache.getIdEjecucion());
		ps.executeUpdate();

	}

	public static AutFixRfqDatosCache obtenerCache(String sessionRec) throws SQLException, InterruptedException {
		Thread.sleep(5000);
		AutFixRfqDatosCache datosCache = null;
		String queryInicio = "SELECT * FROM aut_fix_rfq_cache WHERE RECEIVER_SESSION = '" + sessionRec
				+ "'";
		
		System.out.println("RS OBTENER CACHE:" + queryInicio);
		ResultSet rs = DataAccess.getQuery(queryInicio);
		
		while (rs.next()) {
			// Crea el objeto recuperado.
			datosCache = new AutFixRfqDatosCache();
			// Establecer el objeto recibido.
			datosCache.setReceiverSession(rs.getString("RECEIVER_SESSION"));
			datosCache.setIdCaseseq(rs.getInt("ID_CASESEQ"));
			datosCache.setIdCase(rs.getInt("ID_CASE"));
			datosCache.setIdSecuencia(rs.getInt("ID_SECUENCIA"));
			datosCache.setEstado(rs.getString("ESTADO"));
			datosCache.setFixQuoteReqId(rs.getString("FIX_QUOTE_REQ_ID"));
			datosCache.setIdAfiliado(rs.getString("ID_AFILIADO"));
			datosCache.setIdEjecucion(rs.getLong("ID_EJECUCION"));

		}
		return datosCache;

	}
	
	public static void mostrarCache() throws SQLException, InterruptedException {
		Thread.sleep(3000);
		
		String queryInicio = "SELECT * FROM aut_fix_rfq_cache";
		ResultSet rs = DataAccess.getQuery(queryInicio);
		
		System.out.println("Sessiones activas en cache: ");

		while (rs.next()) {
			System.out.println("SESSION: " +rs.getString("RECEIVER_SESSION") + " SECUENCIA: " + rs.getInt("ID_SECUENCIA"));
		}
		System.out.println("<><><><><><>-<><><><><><>");
	}
	
	public static void cargarLogsExitosos(String nomEtiqueta, int clave, String valor,
			String idEscenario, String idCase, int idSecuencia ) throws SQLException {

		PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO aut_log_ejecucion(`ID_EJECUCION`, `ID_ESCENARIO`, `COD_CASO`, `ID_SECUENCIA`, `FECHA_EJECUCION`, `ESTADO_EJECUCION`, `DESCRIPCION_VALIDACION`, `MENSAJE`, `CODIGO_ERROR`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
		ps.setLong(1, BasicFunctions.getIdEjecution());
		ps.setString(2, idEscenario);
		ps.setString(3, idCase);
		ps.setInt(4, idSecuencia);
		ps.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
		ps.setString(6, "EXITOSO");
		ps.setString(7, nomEtiqueta + "(" + clave + ") => " + valor);
		ps.setString(8, "");
		ps.setNull(9, Types.INTEGER);
		ps.executeUpdate();

	}


//	public static void cargarLogsExitosos(Message message, long ID_EJECUCION, String clave, String valor,
//			String idEscenario, String idCase, int idSecuencia, String clavePrima) throws SQLException {
//
//		PreparedStatement ps = conn.prepareStatement(
//				"INSERT INTO `bvc_automation_db`.`aut_log_ejecucion`(`ID_EJECUCION`, `ID_ESCENARIO`, `COD_CASO`, `ID_SECUENCIA`, `FECHA_EJECUCION`, `ESTADO_EJECUCION`, `DESCRIPCION_VALIDACION`, `MENSAJE`, `CODIGO_ERROR`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
//		ps.setLong(1, ID_EJECUCION);
//		ps.setString(2, idEscenario);
//		ps.setString(3, idCase);
//		ps.setInt(4, idSecuencia);
//		ps.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
//		ps.setString(6, "EXITOSO");
//		ps.setString(7, "(" + clavePrima + ") MSG: " + clave + " BD: " + valor);
//		ps.setString(8, "");
//		ps.setNull(9, Types.INTEGER);
//		ps.executeUpdate();
//
//	}

	public static void limpiarCache() throws SQLException {
		String strQueryLimpiar = "DELETE FROM `aut_fix_rfq_cache` WHERE  RECEIVER_SESSION <> ''";
		setQuery(strQueryLimpiar);
	}
	
	public static void cargarLogsFallidos(Message message, String nomEtiqueta, int clave, String vlMsg, String vlDb,
			String idEscenario, String idCase, int idSecuencia) throws SQLException {

		PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO `aut_log_ejecucion` (`ID_EJECUCION`, `ID_ESCENARIO`, `COD_CASO`, `ID_SECUENCIA`, `FECHA_EJECUCION`, `ESTADO_EJECUCION`, `DESCRIPCION_VALIDACION`, `MENSAJE`, `CODIGO_ERROR`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
		ps.setLong(1, BasicFunctions.getIdEjecution());
		ps.setString(2, idEscenario);
		ps.setString(3, idCase);
		ps.setInt(4, idSecuencia);
		ps.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
		ps.setString(6, "FALLIDO");
		ps.setString(7, nomEtiqueta + "(" + clave + ") => MSG: " + vlMsg +" - BD: " + vlDb);
		ps.setString(8, message.toString());
		ps.setNull(9, Types.INTEGER);
		ps.executeUpdate();

	}

//	public static void cargarLogsFallidos(Message message, long ID_EJECUCION, String clave, String valor,
//			String idEscenario, String idCase, int idSecuencia, String clavePrima) throws SQLException {
//
//		PreparedStatement ps = conn.prepareStatement(
//				"INSERT INTO `bvc_automation_db`.`aut_log_ejecucion` (`ID_EJECUCION`, `ID_ESCENARIO`, `COD_CASO`, `ID_SECUENCIA`, `FECHA_EJECUCION`, `ESTADO_EJECUCION`, `DESCRIPCION_VALIDACION`, `MENSAJE`, `CODIGO_ERROR`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
//		ps.setLong(1, ID_EJECUCION);
//		ps.setString(2, idEscenario);
//		ps.setString(3, idCase);
//		ps.setInt(4, idSecuencia);
//		ps.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
//		ps.setString(6, "FALLIDO");
//		ps.setString(7, "(" + clavePrima + ") MSG: " + clave + " BD: " + valor);
//		ps.setString(8, message.toString());
//		ps.setNull(9, Types.INTEGER);
//		ps.executeUpdate();
//
//	}

	public static boolean validarContinuidadEjecucion() throws SQLException, InterruptedException {

		System.out.println("ENTRANDO A VALIDAR CONTINUIDAD...");
		String query = "SELECT count(1) as cantidad FROM aut_fix_rfq_cache";

		ResultSet rs = DataAccess.getQuery(query);
		Thread.sleep(2000);
		int cantidadEscenarios = 0;

		while (rs.next()) {
			cantidadEscenarios = rs.getInt("cantidad");
			System.out.println(
					"*************** CANTIDAD MENSAJES POR VALIDAR: " + cantidadEscenarios + "\n*********************");
		}

		if (cantidadEscenarios > 0) {
			System.out.println("AUN QUEDAN ESCENARIOS POR VALIDAR");
			mostrarCache();
			return false;
		} else {
			System.out.println("SE FINALIZA VALIDACION DE ESCENARIOS");
			return true;
		}
	}
	
	
	public static void cargarLogs3(Message message, String idEscenario, String idCase,
			int idSecuencia) throws SQLException, FieldNotFound {

		PreparedStatement ps = conn.prepareStatement(

				"INSERT INTO `aut_log_ejecucion`(`ID_EJECUCION`, `ID_ESCENARIO`, `COD_CASO`, `ID_SECUENCIA`, `FECHA_EJECUCION`, `ESTADO_EJECUCION`, `DESCRIPCION_VALIDACION`, `MENSAJE`, `CODIGO_ERROR`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

		ps.setLong(1, BasicFunctions.getIdEjecution());
		ps.setString(2, idEscenario);
		ps.setString(3, idCase);
		ps.setInt(4, idSecuencia);
		ps.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
		ps.setString(6, "FALLIDO");
		ps.setString(7, message.getString(58));
		ps.setString(8, message.toString());
		ps.setNull(9, Types.INTEGER);
		ps.executeUpdate();

	}

//	public static void cargarLogs3(Message message, long ID_EJECUCION, String idEscenario, String idCase,
//			int idSecuencia) throws SQLException, FieldNotFound {
//
//		PreparedStatement ps = conn.prepareStatement(
//
//				"INSERT INTO `bvc_automation_db`.`aut_log_ejecucion`(`ID_EJECUCION`, `ID_ESCENARIO`, `COD_CASO`, `ID_SECUENCIA`, `FECHA_EJECUCION`, `ESTADO_EJECUCION`, `DESCRIPCION_VALIDACION`, `MENSAJE`, `CODIGO_ERROR`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
//
//		ps.setLong(1, ID_EJECUCION);
//		ps.setString(2, idEscenario);
//		ps.setString(3, idCase);
//		ps.setInt(4, idSecuencia);
//		ps.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
//		ps.setString(6, "FALLIDO");
//		ps.setString(7, message.getString(58));
//		ps.setString(8, message.toString());
//		ps.setNull(9, Types.INTEGER);
//		ps.executeUpdate();
//
//	}

}
