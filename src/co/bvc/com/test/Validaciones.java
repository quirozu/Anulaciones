package co.bvc.com.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.bind.ParseConversionEvent;

import co.bvc.com.basicfix.BasicFunctions;
import co.bvc.com.basicfix.DataAccess;
import co.bvc.com.dao.domain.AutFixRfqDatosCache;
import quickfix.ConfigError;
import quickfix.DataDictionary;
import quickfix.FieldNotFound;
import quickfix.Group;
import quickfix.Message;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;

public class Validaciones {

	private static Connection conn = null;
	DataDictionary dictionary = null;
	DataAccess data = new DataAccess();
	
	public void validarAR(AutFixRfqDatosCache datosCache, Message message) throws SQLException, FieldNotFound, ConfigError {
		int contadorBuenos = 0;
		int contadorMalos = 0;
		
		int idCase = 0;
		int idSecuencia = 0;
		String id_Escenario = "";
	
		ResultSet resultset;
		String queryMessageAR = "SELECT * FROM aut_fix_tcr_datos WHERE ID_CASESEQ = " + datosCache.getIdCaseseq();
		
		resultset = DataAccess.getQuery(queryMessageAR);
		
		Map <Integer, String> mapaDB = new TreeMap<Integer, String>();
		
		while(resultset.next()) {
			//Valores 17, 58 y 60 son propios del mensaje por tanto no se validan
			mapaDB.put(571, BasicFunctions.getIdEjecution()+""+datosCache.getIdCaseseq()+"_AE");
			mapaDB.put(487, resultset.getString("AR_TRADTRANTYPE"));
			mapaDB.put(856, resultset.getString("AR_TRADEREPTYPE"));
			mapaDB.put(939, resultset.getString("AR_TRDRPTSTATUS"));
			mapaDB.put(751, resultset.getString("AR_TRADREPREASON"));
			mapaDB.put(572, resultset.getString("AR_TRAREPREFID"));
	
			idCase = resultset.getInt("ID_CASE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");
//			id_Escenario = resultset.getString("ID_ESCENARIO");
			id_Escenario = "FIX_AR";
		}
		
		for(Map.Entry<Integer,String> entry : mapaDB.entrySet()) {
			Integer key = entry.getKey();
			String valueDB = entry.getValue();
			
			String valueMSG = message.isSetField(key) ? message.getString(key) : null;
			
			String nomEtiqueta = getNameTag(key);
			
			System.out.println(nomEtiqueta + "(" + key + ") => DB: " + valueDB + " MG: " + valueMSG);
			
			//Se compara si ninguno tiene valores nulos
			if(valueDB != null && valueMSG != null) {
				if(valueDB.equals(valueMSG)) {
					contadorBuenos++;
					msgBuenos("AR-"+nomEtiqueta, key, valueDB);
					DataAccess.cargarLogsExitosos(nomEtiqueta, key, valueDB, id_Escenario, String.valueOf(idCase), idSecuencia);
				} else {
					contadorMalos++;
					msgMalos("AR-"+nomEtiqueta,key,valueDB,valueMSG);
					DataAccess.cargarLogsFallidos(message, nomEtiqueta, key, valueMSG, valueDB, id_Escenario, String.valueOf(idCase), idSecuencia);
				}
				
			} else {
				if (valueDB == null && valueMSG == null) {
					contadorBuenos++;
					msgBuenos("AR-"+nomEtiqueta, key, valueDB);
					DataAccess.cargarLogsExitosos(nomEtiqueta, key, valueDB, id_Escenario, String.valueOf(idCase), idSecuencia);
				} else {
					contadorMalos++;
					msgMalos("AR-"+nomEtiqueta,key,valueDB,valueMSG);
					DataAccess.cargarLogsFallidos(message, nomEtiqueta, key, valueMSG, valueDB, id_Escenario, String.valueOf(idCase), idSecuencia);
				}
			}
			
		}
		
		if(message.isSetField(58)) {
			System.out.println("MENSAJE RECHAZADO...\n" + message.getString(58));
			DataAccess.cargarLogs3(message, id_Escenario, String.valueOf(idCase), idSecuencia);
		} else {
			System.out.println("MENSAJE ACEPTADO...");
		}
		
		System.out.println("----------------------------------------");
		System.out.println("-- FINAL VALIDACIONES AR --");
		System.out.println("LAS VALIDACIONES CORRECTAS FUERON : " + contadorBuenos);
		System.out.println("LAS VALIDACIONES ERRADAS FUERON : " + contadorMalos);
		System.out.println("TOTAL VALIDACIONES REALIZADAS : " + (contadorBuenos + contadorMalos));
		
	}

	public void validarAE(AutFixRfqDatosCache datosCache, Message message) throws SQLException, FieldNotFound, ConfigError {
		int contadorBuenos = 0;
		int contadorMalos = 0;
		int idCase = 0;
		int idSecuencia = 0;
		String idEscenario = "";
		String mercado = "DEFAULT";
		
		String idAfiliado = message.getHeader().getString(TargetCompID.FIELD); //56
		
		String queryMessageAE = "SELECT * FROM aut_fix_tcr_datos WHERE ID_CASE = " + datosCache.getIdCase();
		
		if(idAfiliado.equals(BasicFunctions.getIniciator())) {
			queryMessageAE += " AND ID_SECUENCIA = 1;";
		} else {
			queryMessageAE += " AND ID_SECUENCIA = 2;";
		}
		
		System.out.println("CONSULTA AE: " +  queryMessageAE);
		
		ResultSet resultset = DataAccess.getQuery(queryMessageAE);
		
		Map <Integer, String> mapaDB = new TreeMap<Integer, String>();
		
		while(resultset.next()) {
			idCase = resultset.getInt("ID_CASE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");
			idEscenario = resultset.getString("ID_ESCENARIO");
			mercado = resultset.getString("MERCADO"); 
			
			mapaDB.put(487, resultset.getString("AE_TRADTRANTYPE"));
			
			if(idAfiliado.equals(BasicFunctions.getIniciator())) {
				mapaDB.put(571, BasicFunctions.getIdEjecution()+"1_AE");
			}
			
			if(datosCache.getIdSecuencia() == 1) {
				mapaDB.put(487, "0");
				mapaDB.put(856, resultset.getString("AE_RECTREPTYPE1"));
				mapaDB.put(828, Integer.toString(resultset.getInt("AE_TRDTYPE")));
				mapaDB.put(150, resultset.getString("AE_EXECTYPE"));
				mapaDB.put(572, resultset.getString("AE_TRAREPREFID"));
				mapaDB.put(20102, resultset.getString("AE_DIRTYPRICE"));
				mapaDB.put(62, resultset.getString("AE_VALIDUNTILTIME"));			
			} else {
				mapaDB.put(487, resultset.getString("AE_TRADTRANTYPE"));
				mapaDB.put(856, resultset.getString("AE_RECTREPTYPE2"));
				
				if(mercado.equals("RFFON") || mercado.equals("RVFON")) {
					//Falta crear campos en DB
//					mapaDB.put(824, resultset.getString("AE_TRADELEGREFID"));
//					mapaDB.put(914, resultset.getString("AE_AGREEID"));
//					mapaDB.put(916, resultset.getString("AE_STARTDATE"));
//					mapaDB.put(917, resultset.getString("AE_ENDDATE"));
//					mapaDB.put(232, resultset.getString("AE_NOSTIPULA"));
//					mapaDB.put(233, resultset.getString("AE_STIPTYPE"));
//					mapaDB.put(234, resultset.getString("AE_STIPVALUE"));
					
					//Este campo sobraría
//					mapaDB.put(453, resultset.getString("AE_NOPARTYID"));
					//AQUI VAN LOS PARTY'S ADICIONALES.
					mapaDB.put(1, resultset.getString("AE_ACCOUNT"));	
					mapaDB.put(921, resultset.getString("AE_STARTCASH"));		
					mapaDB.put(922, resultset.getString("AE_ENDCASH"));
				}				
			}

			mapaDB.put(573, resultset.getString("AE_MATCHSTATUS"));
			mapaDB.put(15, resultset.getString("AE_CURRENCY"));

			SimpleDateFormat SDF = new SimpleDateFormat("yyyMMdd");
//			long id_ejecution = Long.parseLong(SDF.format(new Date()));
			String strSettDate = SDF.format(resultset.getString("AE_SETTDATE"));
			String strTradeDate = SDF.format(resultset.getString("AE_SETTDATE"));
			
//			mapaDB.put(64, resultset.getString("AE_SETTDATE"));
//			mapaDB.put(75, resultset.getString("AE_TRADEDATE"));
			mapaDB.put(64, strSettDate);
			mapaDB.put(75, strTradeDate);

			mapaDB.put(381, resultset.getString("AE_GROSSTRADEAMT"));
			mapaDB.put(880, resultset.getString("AE_TRMATCHID"));
			mapaDB.put(55, resultset.getString("AE_SYMBOL"));
			mapaDB.put(762, resultset.getString("AE_SECSUBTYPE"));
			mapaDB.put(31, resultset.getString("AE_LASTPX"));
			mapaDB.put(32, resultset.getString("AE_LASTQTY"));
//			mapaDB.put(60, resultset.getString("AE_TRANSTIME"));	
			mapaDB.put(552, resultset.getString("AE_NOSIDES"));
			
//			mapaDB.put(54, Integer.toString(resultset.getInt("AE_SIDE")));
//			mapaDB.put(54, Integer.toString(3-resultset.getInt("AE_SIDE"))); //3-2=1  3-1=2
			mapaDB.put(54, resultset.getString("AE_SIDE"));
//			mapaDB.put(453, resultset.getString("AE_NOPARTYID"));
			//AQUI VAN LOS PARTYIES.
			mapaDB.put(1, resultset.getString("AE_ACCOUNT"));	
//			mapaDB.put(17, resultset.getString("AE_EXECID"));
			
		}
		
		for(Map.Entry<Integer,String> entry : mapaDB.entrySet()) {
			Integer key = entry.getKey();
			String valueDB = entry.getValue();
			String valueMSG = null;
			
			//Si la clave se encuentra dentro del gupo NoSides
			if(key==54 || key == 1 || key == 453 || key == 921 || key == 922) {
				
//				String valueMSG = message.isSetField(key) ? message.getString(key) : null;

				List<Group> groupNoSides = message.getGroups(552);
									
				for(Group grupo:groupNoSides) {
					valueMSG = grupo.isSetField(key) ? grupo.getString(key) : null;
				}
			} else valueMSG = message.isSetField(key) ? message.getString(key) : null;
			
			String nomEtiqueta = getNameTag(key);
			
			System.out.println(nomEtiqueta + "(" + key + ") => DB: " + valueDB + " MG: " + valueMSG);
			
			//Se compara si ninguno tiene valores nulos
			if(valueDB != null && valueMSG != null) {
				if(valueDB.equals(valueMSG)) {
					contadorBuenos++;
					msgBuenos("ER-"+nomEtiqueta, key, valueDB);
					DataAccess.cargarLogsExitosos(nomEtiqueta, key, valueDB, idEscenario, String.valueOf(idCase), idSecuencia);
				} else {
					contadorMalos++;
					msgMalos("ER-"+nomEtiqueta,key,valueDB,valueMSG);
					DataAccess.cargarLogsFallidos(message, nomEtiqueta, key, valueMSG, valueDB, idEscenario, String.valueOf(idCase), idSecuencia);
				}
				
			} else {
				if (valueDB == null && valueMSG == null) {
					contadorBuenos++;
					msgBuenos("ER-"+nomEtiqueta, key, valueDB);
					DataAccess.cargarLogsExitosos(nomEtiqueta, key, valueDB, idEscenario, String.valueOf(idCase), idSecuencia);
				} else {
					contadorMalos++;
					msgMalos("ER-"+nomEtiqueta,key,valueDB,valueMSG);
					DataAccess.cargarLogsFallidos(message, nomEtiqueta, key, valueMSG, valueDB, idEscenario, String.valueOf(idCase), idSecuencia);
				}
			}
		}
			
		
		
//		for(Map.Entry<Integer,String> entry : mapaDB.entrySet()) {
//			Integer key = entry.getKey();
//			String valueDB = entry.getValue();
//			
//			String valueMSG = message.isSetField(key) ? message.getString(key) : null;
//			
//			String nomEtiqueta = getNameTag(key);
//			
////			System.out.println(key + " => DB: " + valueDB + " MG: " + valueMSG);
//			System.out.println(nomEtiqueta + "(" + key + ") => DB: " + valueDB + " MG: " + valueMSG);
//			
//			//Se compara si ninguno tiene valores nulos
//			if(valueDB != null && valueMSG != null) {
//				if(valueDB.equals(valueMSG)) {
//					contadorBuenos++;
//					msgBuenos("AE-"+nomEtiqueta, key, valueDB);
//					DataAccess.cargarLogsExitosos(nomEtiqueta, key, valueDB, idEscenario, String.valueOf(idCase), idSecuencia);
//				} else {
//					contadorMalos++;
//					msgMalos("AE-"+nomEtiqueta,key,valueDB,valueMSG);
//					DataAccess.cargarLogsFallidos(message, nomEtiqueta, key, valueMSG, valueDB, idEscenario, String.valueOf(idCase), idSecuencia);
//				}
//				
//			} else {
//				if (valueDB == null && valueMSG == null) {
//					contadorBuenos++;
//					msgBuenos("AE-"+nomEtiqueta, key, valueDB);
//					DataAccess.cargarLogsExitosos(nomEtiqueta, key, valueDB, idEscenario, String.valueOf(idCase), idSecuencia);
//				} else {
//					contadorMalos++;
//					msgMalos("AE-"+nomEtiqueta,key,valueDB,valueMSG);
//					DataAccess.cargarLogsFallidos(message, nomEtiqueta, key, valueMSG, valueDB, idEscenario, String.valueOf(idCase), idSecuencia);
//				}
//			}
//		}
		
		System.out.println("----------------------------------------");
		System.out.println("---------------------");
		System.out.println("-- VALIDACIONES AE --");
		System.out.println("LAS VALIDACIONES CORRECTAS FUERON : " + contadorBuenos);
		System.out.println("LAS VALIDACIONES ERRADAS FUERON : " + contadorMalos);
		System.out.println("TOTAL VALIDACIONES REALIZADAS : " + (contadorBuenos + contadorMalos));
		
	}
	
	public void validarER(AutFixRfqDatosCache datosCache, Message message) throws SQLException, FieldNotFound, ConfigError {
		
		int contadorBuenos = 0;
		int contadorMalos = 0;
		int idCase = 0;
		int idSecuencia = 0;
		String idEscenario = "";
		
		String idAfiliado = message.getHeader().getString(TargetCompID.FIELD); //56
		
		String queryMessageER = "SELECT * FROM aut_fix_tcr_datos WHERE ID_CASE = " + datosCache.getIdCase();
		System.out.println("CONSULTA ER: "+ queryMessageER);
		
		if(idAfiliado.equals(BasicFunctions.getIniciator())) {
			queryMessageER += " AND ID_SECUENCIA = 1;";
		} else {
			queryMessageER += " AND ID_SECUENCIA = 2;";
		}
		
		System.out.println("CONSULTA ER: "+ queryMessageER);
		
		ResultSet resultset = DataAccess.getQuery(queryMessageER);
		
		Map <Integer, String> mapaDB = new TreeMap<Integer, String>();	
			
		while(resultset.next()) {
			
			idCase = resultset.getInt("ID_CASE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");
			idEscenario = resultset.getString("ID_ESCENARIO");
			
			mapaDB.put(37, resultset.getString("AE_TRMATCHID")); // Deberías ser AE_ORDERID que no está en DB.
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
			mapaDB.put(381, resultset.getString("AE_GROSSTRADEAMT"));
			mapaDB.put(880, resultset.getString("AE_TRMATCHID"));
			mapaDB.put(1057, resultset.getString("ER_AGGRESSORIND"));
			
			if(resultset.getString("MERCADO").equals("DV")) {
				mapaDB.put(1, resultset.getString("AE_ACCOUNT"));
			}
						
			if(resultset.getString("MERCADO").equals("RFFON") || resultset.getString("MERCADO").equals("RVFON")) {
//				mapaDB.put(824, resultset.getString("AE_TRADELEGREFID"));
//				mapaDB.put(914, resultset.getString("AE_AGREEID"));
//				mapaDB.put(916, resultset.getString("AE_STARTDATE"));
//				mapaDB.put(917, resultset.getString("AE_ENDDATE"));
//				mapaDB.put(311, resultset.getString("AE_UNDERSYMBOL"));
//				mapaDB.put(763, resultset.getString("AE_UNDERSECTYPE"));
//				mapaDB.put(232, resultset.getString("AE_NOSTIPULA"));
//				mapaDB.put(233, resultset.getString("AE_STIPTYPE"));
//				mapaDB.put(234, resultset.getString("AE_STIPVALUE"));
//				mapaDB.put(453, resultset.getString("AE_NOPARTYID"));
				mapaDB.put(921, resultset.getString("AE_STARTCASH"));
				mapaDB.put(922, resultset.getString("AE_ENDCASH"));
			}

//			mapaDB.put(17, resultset.getString("AE_EXECID"));
			mapaDB.put(60, resultset.getString("AE_TRANSTIME"));  //Falla porque el motor lo genera.
			
			idCase = resultset.getInt("ID_CASE");
			idSecuencia = resultset.getInt("ID_SECUENCIA");
			idEscenario = "FIX_8";
//			idEscenario = resultset.getString("ID_ESCENARIO");
		}
		for(Map.Entry<Integer,String> entry : mapaDB.entrySet()) {
			Integer key = entry.getKey();
			String valueDB = entry.getValue();
			
			String valueMSG = message.isSetField(key) ? message.getString(key) : null;
			
			String nomEtiqueta = getNameTag(key);
			
			System.out.println(nomEtiqueta + "(" + key + ") => DB: " + valueDB + " MG: " + valueMSG);
			
			//Se compara si ninguno tiene valores nulos
			if(valueDB != null && valueMSG != null) {
				if(valueDB.equals(valueMSG)) {
					contadorBuenos++;
					msgBuenos("ER-"+nomEtiqueta, key, valueDB);
					DataAccess.cargarLogsExitosos(nomEtiqueta, key, valueDB, idEscenario, String.valueOf(idCase), idSecuencia);
				} else {
					contadorMalos++;
					msgMalos("ER-"+nomEtiqueta,key,valueDB,valueMSG);
					DataAccess.cargarLogsFallidos(message, nomEtiqueta, key, valueMSG, valueDB, idEscenario, String.valueOf(idCase), idSecuencia);
				}
				
			} else {
				if (valueDB == null && valueMSG == null) {
					contadorBuenos++;
					msgBuenos("ER-"+nomEtiqueta, key, valueDB);
					DataAccess.cargarLogsExitosos(nomEtiqueta, key, valueDB, idEscenario, String.valueOf(idCase), idSecuencia);
				} else {
					contadorMalos++;
					msgMalos("ER-"+nomEtiqueta,key,valueDB,valueMSG);
					DataAccess.cargarLogsFallidos(message, nomEtiqueta, key, valueMSG, valueDB, idEscenario, String.valueOf(idCase), idSecuencia);
				}
			}
			
		}
		
	//		for(Map.Entry<Integer,String> entry : mapaDB.entrySet()) {
	//			Integer key = entry.getKey();
	//			String valueDB = entry.getValue();
	//			
	//			String nomEtiqueta =  getNameTag(key); //"NombreEtiqueta"; //falta implementar contra LIBRERIA FIX4.4.xml
	//			System.out.println(nomEtiqueta + "(" + key + ") => DB: " + valueDB + " MG: " + message.getString(key));
	//			
	////			System.out.println(key + " => DB: " + valueDB + " MG: " + message.getString(key));
	//			
	//			if(valueDB != null && valueDB.equals(message.getString(key))) {
	//				contadorBuenos++;
	//				msgBuenos(nomEtiqueta, key, valueDB);
	//
	//				DataAccess.cargarLogsExitosos(nomEtiqueta, key, valueDB, id_Escenario, String.valueOf(idCase), idSecuencia);
	//				
	//			} else {
	//				contadorMalos++;
	//				DataAccess.cargarLogsFallidos(message, nomEtiqueta, key, message.getString(key), valueDB, id_Escenario, String.valueOf(idCase), idSecuencia);
	//			}
	//			
	//		}
			
			System.out.println("----------------------------------------");
			System.out.println("---------------------");
			System.out.println("-- VALIDACIONES ER --");
			System.out.println("LAS VALIDACIONES CORRECTAS FUERON : " + contadorBuenos);
			System.out.println("LAS VALIDACIONES ERRADAS FUERON : " + contadorMalos);
			System.out.println("TOTAL VALIDACIONES REALIZADAS : " + (contadorBuenos + contadorMalos));
			
		}

	public void validar3(AutFixRfqDatosCache datosCache, Message message) throws SQLException, FieldNotFound {
	
		ResultSet resultset;
		String queryMessageR = "SELECT * FROM bvc_automation_db.aut_fix_rfq_datos " + "WHERE ID_CASESEQ = " 
				+ datosCache.getIdCaseseq();
	
		resultset = DataAccess.getQuery(queryMessageR);
		String idEscenario = null, idCase = null;
	
		while (resultset.next()) {
			idEscenario = resultset.getString("ID_ESCENARIO");
			idCase = resultset.getString("ID_CASE");
		}
	
		DataAccess.cargarLogs3(message, idEscenario, idCase, datosCache.getIdSecuencia());
		System.out.println("************\n SE CARGO AL LOG VALIDAR 3 \n************ ");
	
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

	public void msgBuenos(String nomTag, int keyTag, String valueDb) {
		System.out.println("IGUALES: " + nomTag + "(" + keyTag + "): " + valueDb);
	}
	
	public void msgMalos(String nomTag, int keyTag, String valueDb, String valueMsg) {
		System.out.println("DIFERENTES: " + nomTag + "(" + keyTag + ") => DB:" + valueDb + ", MSG:" + valueMsg);
	}

	public String getNameTag(int tag) throws ConfigError {
		
		dictionary = new DataDictionary("resources\\datadictionary\\FIX44.xml");
		String nameTag = "";
	
		if (dictionary.hasFieldValue(tag)) {
			nameTag = dictionary.getFieldName(tag);
		}
		return nameTag;
	}
}
