package co.bvc.com.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import co.bvc.com.basicfix.BasicFunctions;
import co.bvc.com.basicfix.DataAccess;
import co.bvc.com.dao.domain.AutFixRfqDatosCache;
import quickfix.FieldNotFound;
import quickfix.Message;

public class Validaciones {

	private static Connection conn = null;

	DataAccess data = new DataAccess();
	private String cadenaOcho;

	public String getCadenaOcho() {
		return cadenaOcho;
	}

	public void setCadenaOcho(String cadenaOcho) {
		this.cadenaOcho = cadenaOcho;
	}

	public ArrayList<String> FragmentarCadena(String cadena) {
		ArrayList<String> claveValor = new ArrayList<String>();
		for (int i = 0; i < cadena.split("").length; i++) {
			claveValor.add(cadena.split("")[i]);
//                    System.out.println(claveValor.get(i));
		}
		return claveValor;
	}

	public ArrayList<String> FragmentarCadena1(String cadena) {
		ArrayList<String> claveValor1 = new ArrayList<String>();
		for (int i = 0; i < cadena.split("").length; i++) {
			claveValor1.add(cadena.split("")[i]);
//                    System.out.println(claveValor.get(i));
		}
		return claveValor1;
	}

	
	public void validarAE(AutFixRfqDatosCache datosCache, Message message) throws SQLException, FieldNotFound {
		int contadorBuenos = 0;
		int contadorMalos = 0;
		
		int idCase = 0;
		int idSecuencia = 0;
		String id_Escenario = "";
		
		ResultSet resultset;
		String queryMessageAE = "SELECT * FROM aut_fix_tcr_datos WHERE ID_CASESEQ = " + datosCache.getIdCaseseq();
		
		resultset = DataAccess.getQuery(queryMessageAE);
		
		Map <Integer, String> mapaDB = new TreeMap<Integer, String>();
		
		while(resultset.next()) {
			mapaDB.put(571, BasicFunctions.getIdEjecution()+""+datosCache.getIdCaseseq()+"_AE");
			mapaDB.put(487, resultset.getString("AE_TRADTRANTYPE"));
			mapaDB.put(856, resultset.getString("AE_TRADEREPTYPE"));
			mapaDB.put(880, resultset.getString("AE_TRMATCHID"));
			
			mapaDB.put(31, resultset.getString("AE_LASTPX"));
			mapaDB.put(32, resultset.getString("AE_LASTQTY"));
			mapaDB.put(60, resultset.getString("AE_TRANSTIME"));
			mapaDB.put(552, resultset.getString("AE_NOSIDES"));
			//mapaDB.put(54, resultset.getString("AE_SIDE"));

			idCase = resultset.getInt("ID_CASE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");
			id_Escenario = resultset.getString("ID_ESCENARIO");
		}
		
		for(Map.Entry<Integer,String> entry : mapaDB.entrySet()) {
			Integer key = entry.getKey();
			String valueDB = entry.getValue();
			
			String valueMSG = message.isSetField(key) ? message.getString(key) : null; 
			
			String nomEtiqueta = "NombreEtiqueta"; //falta implementar contra LIBRERIA FIX4.4.xml
			
			System.out.println(key + " => DB: " + valueDB + " MG: " + message.getString(key));
			
			if(valueDB != null && valueDB.equals(valueMSG)) {
				contadorBuenos++;
				cadenaDeMensaje("securityIdSource", key.toString(), valueDB);

				DataAccess.cargarLogsExitosos(nomEtiqueta, key.toString(), valueDB, id_Escenario, String.valueOf(idCase), idSecuencia);
				
			} else {
				contadorMalos++;
				DataAccess.cargarLogsFallidos(message, nomEtiqueta, key.toString(), valueMSG, valueDB, id_Escenario, String.valueOf(idCase), idSecuencia);
			}
			
		}
		
		System.out.println("----------------------------------------");
		System.out.println("---------------------");
		System.out.println("-- VALIDACIONES AE --");
		System.out.println("LAS VALIDACIONES CORRECTAS FUERON : " + contadorBuenos);
		System.out.println("LAS VALIDACIONES ERRADAS FUERON : " + contadorMalos);
		System.out.println("TOTAL VALIDACIONES REALIZADAS : " + (contadorBuenos + contadorMalos));
		
	}
	
	public void validarAR(AutFixRfqDatosCache datosCache, Message message) throws SQLException, FieldNotFound {
		int contadorBuenos = 0;
		int contadorMalos = 0;
		
		int idCase = 0;
		int idSecuencia = 0;
		String id_Escenario = "";
//		String cadenaPrima = message.toString();
		
		ResultSet resultset;
		String queryMessageAR = "SELECT * FROM aut_fix_tcr_datos WHERE ID_CASESEQ = " + datosCache.getIdCaseseq();
		
		resultset = DataAccess.getQuery(queryMessageAR);
		
		Map <Integer, String> mapaDB = new TreeMap<Integer, String>();
		
		while(resultset.next()) {
//			mapaDB.put(35, resultset.getString("AR_MSGTYPE"));
			DateTimeFormatter formato = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS");
			LocalDateTime transactTime = LocalDateTime.parse(resultset.getString("AR_TRANSTIME"), formato);
			
			String transTimeFormated=transactTime.getYear()+""+transactTime.getMonthValue()+""+transactTime.getDayOfMonth()+
					"-"+transactTime.getHour()+":"+transactTime.getMinute()+":"+transactTime.getSecond()+"."+transactTime.getNano();
			
			mapaDB.put(571, BasicFunctions.getIdEjecution()+""+datosCache.getIdCaseseq()+"_AE");
			mapaDB.put(487, resultset.getString("AR_TRADTRANTYPE"));
			mapaDB.put(856, resultset.getString("AR_TRADEREPTYPE"));
			mapaDB.put(939, resultset.getString("AR_TRDRPTSTATUS"));
			mapaDB.put(17, resultset.getString("AR_EXECID")); //Aumenta con cada ejecuci贸n
//			mapaDB.put(60, resultset.getString("AR_TRANSTIME"));
			mapaDB.put(60, transTimeFormated);
			idCase = resultset.getInt("ID_CASE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");
			id_Escenario = resultset.getString("ID_ESCENARIO");
		}
		
		for(Map.Entry<Integer,String> entry : mapaDB.entrySet()) {
			Integer key = entry.getKey();
			String valueDB = entry.getValue();
			
			String valueMSG = message.isSetField(key) ? message.getString(key) : null;
			
			String nomEtiqueta = "NombreEtiqueta"; //falta implementar contra LIBRERIA FIX4.4.xml
			
			System.out.println(key + " => DB: " + valueDB + " MG: " + valueMSG);
			
			if(valueDB.equals(message.getString(key))) {
				contadorBuenos++;
				cadenaDeMensaje("securityIdSource", key.toString(), valueDB);

				DataAccess.cargarLogsExitosos(nomEtiqueta, key.toString(), valueDB, id_Escenario, String.valueOf(idCase), idSecuencia);
				
			} else {
				contadorMalos++;
				DataAccess.cargarLogsFallidos(message, nomEtiqueta, key.toString(), valueMSG, valueDB, id_Escenario, String.valueOf(idCase), idSecuencia);
			}			
		}
		
		if(message.isSetField(58)) {
			System.out.println("MENSAJE RECHAZADO...\n" + message.getString(58));
			DataAccess.cargarLogs3(message, id_Escenario, String.valueOf(idCase), idSecuencia);
		} else {
			System.out.println("MENSAJE ACEPTADO...");
		}
		
		System.out.println("----------------------------------------");
		System.out.println("---------------------");
		System.out.println("-- VALIDACIONES AR --");
		System.out.println("LAS VALIDACIONES CORRECTAS FUERON : " + contadorBuenos);
		System.out.println("LAS VALIDACIONES ERRADAS FUERON : " + contadorMalos);
		System.out.println("TOTAL VALIDACIONES REALIZADAS : " + (contadorBuenos + contadorMalos));
		
	}

	public void validarER(AutFixRfqDatosCache datosCache, Message message) throws SQLException, FieldNotFound {
		int contadorBuenos = 0;
		int contadorMalos = 0;
		
		int idCase = 0;
		int idSecuencia = 0;
		String id_Escenario = "";
		
		ResultSet resultset;
		String queryMessageAE = "SELECT * FROM aut_fix_tcr_datos WHERE ID_CASESEQ = " + BasicFunctions.getIdCaseSeq();
		
		resultset = DataAccess.getQuery(queryMessageAE);
		
		Map <Integer, String> mapaDB = new TreeMap<Integer, String>();
		
		while(resultset.next()) {
			mapaDB.put(35, resultset.getString("AE_MSGTYPE"));
			mapaDB.put(37, resultset.getString("AE_TRMATCHID"));
			mapaDB.put(150, resultset.getString("ER_EXECTYPE"));
			mapaDB.put(55, resultset.getString("AE_SYMBOL"));
			mapaDB.put(762, resultset.getString("AE_SECSUBTYPE"));
			mapaDB.put(48, resultset.getString("AE_SECID"));
			mapaDB.put(22, resultset.getString("AE_SECIDSOURCE"));
			mapaDB.put(31, resultset.getString("AE_LASTPX"));
			mapaDB.put(32, resultset.getString("AE_LASTQTY"));
			mapaDB.put(54, resultset.getString("AE_SIDE"));
			mapaDB.put(75, resultset.getString("AE_TRADEDATE"));
			mapaDB.put(64, resultset.getString("AE_SETTDATE"));
			mapaDB.put(1, resultset.getString("AE_ACCOUNT"));
			mapaDB.put(381, resultset.getString("AE_GROSSTRADEAMT"));
			mapaDB.put(880, resultset.getString("AE_TRMATCHID"));
			mapaDB.put(1057, resultset.getString("ER_AGGRESSORIND"));
			
			if(resultset.getString("MERCADO").equals("RF")) {
				mapaDB.put(824, resultset.getString("AE_TRADELEGREFID"));
				mapaDB.put(914, resultset.getString("AE_AGREEID"));
				mapaDB.put(916, resultset.getString("AE_STARTDATE"));
				mapaDB.put(917, resultset.getString("AE_ENDDATE"));
				mapaDB.put(311, resultset.getString("AE_UNDERSYMBOL"));
				mapaDB.put(763, resultset.getString("AE_UNDERSECTYPE"));
				mapaDB.put(232, resultset.getString("AE_NOSTIPULA"));
				mapaDB.put(233, resultset.getString("AE_STIPTYPE"));
				mapaDB.put(234, resultset.getString("AE_STIPVALUE"));
				mapaDB.put(453, resultset.getString("AE_NOPARTYID"));
				mapaDB.put(921, resultset.getString("AE_STARTCASH"));
				mapaDB.put(922, resultset.getString("AE_ENDCASH"));
			}

			mapaDB.put(17, resultset.getString("AE_EXECID"));
			mapaDB.put(60, resultset.getString("AE_TRANSTIME"));
			
			idCase = resultset.getInt("ID_CASE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");
			id_Escenario = resultset.getString("ID_ESCENARIO");
		}
		
		for(Map.Entry<Integer,String> entry : mapaDB.entrySet()) {
			Integer key = entry.getKey();
			String valueDB = entry.getValue();
			
			String nomEtiqueta = "NombreEtiqueta"; //falta implementar contra LIBRERIA FIX4.4.xml
			
			System.out.println(key + " => DB: " + valueDB + " MG: " + message.getString(key));
			
			if(valueDB != null && valueDB.equals(message.getString(key))) {
				contadorBuenos++;
				cadenaDeMensaje("securityIdSource", key.toString(), valueDB);

				DataAccess.cargarLogsExitosos(nomEtiqueta, key.toString(), valueDB, id_Escenario, String.valueOf(idCase), idSecuencia);
				
			} else {
				contadorMalos++;
				DataAccess.cargarLogsFallidos(message, nomEtiqueta, key.toString(), message.getString(key), valueDB, id_Escenario, String.valueOf(idCase), idSecuencia);
			}
			
		}
		
		System.out.println("----------------------------------------");
		System.out.println("---------------------");
		System.out.println("-- VALIDACIONES ER --");
		System.out.println("LAS VALIDACIONES CORRECTAS FUERON : " + contadorBuenos);
		System.out.println("LAS VALIDACIONES ERRADAS FUERON : " + contadorMalos);
		System.out.println("TOTAL VALIDACIONES REALIZADAS : " + (contadorBuenos + contadorMalos));
		
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
	
//	public void validar3(AutFixRfqDatosCache datosCache, Message message) throws SQLException, FieldNotFound {
//
//		ResultSet resultset;
//		String queryMessageR = "SELECT * FROM bvc_automation_db.aut_fix_rfq_datos " + "WHERE ID_CASESEQ = " 
//				+ datosCache.getIdCaseseq();
//
//		resultset = DataAccess.getQuery(queryMessageR);
//		String idEscenario = null, idCase = null;
//
//		while (resultset.next()) {
//			idEscenario = resultset.getString("ID_ESCENARIO");
//			idCase = resultset.getString("ID_CASE");
//		}
//
//		DataAccess.cargarLogs3(message, datosCache.getIdEjecucion(), idEscenario, idCase, datosCache.getIdSecuencia());
//		System.out.println("************\n SE CARGO AL LOG VALIDAR 3 \n************ ");
//
//	}

	public void cadenaDeMensaje(String columna, String valor, String comparar) {
		System.out.println(columna + "(" + valor + "): " + comparar);
	}
}
