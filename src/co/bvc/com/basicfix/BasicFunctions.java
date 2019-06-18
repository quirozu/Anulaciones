package co.bvc.com.basicfix;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import co.bvc.com.test.AdapterIO;
import co.bvc.com.test.Login;

public class BasicFunctions {

	private static Connection conn;
	private static Login login;
	private static Map<String, String> quoteReqId = new HashMap<String, String>();
	private static long idEjecution;
	private static AdapterIO adapterIO;

	public static Connection getConn() {
		return conn;
	}

	public static void setConn(Connection conn) {
		BasicFunctions.conn = conn;
	}

	public static Login getLogin() {
		return login;
	}

	public static void setLogin(Login login) {
		BasicFunctions.login = login;
	}

	// Metodo de QuoteReqId
	public static void addQuoteReqId(String k, String v) {
		BasicFunctions.quoteReqId.put(k, v);
	}

	public static String getQuoteReqIdOfAfiliado(String afiliado) {
		return BasicFunctions.quoteReqId.get(afiliado);
	}

	public static void setQuoteReqId(Map<String, String> quoteReqId) {
		BasicFunctions.quoteReqId = quoteReqId;
	}

	public static Map<String, String> getQuoteReqId() {
		return quoteReqId;
	}

	public static long getIdEjecution() {
		return idEjecution;
	}

	public static void setIdEjecution(long idEjecution) {
		BasicFunctions.idEjecution = idEjecution;
	}

	public static AdapterIO getAdapterIO() {
		return adapterIO;
	}

	public static void setAdapterIO(AdapterIO adapterIO) {
		BasicFunctions.adapterIO = adapterIO;
	}

	/**
	 * Crea la conexi�n a la db y se la asigna a la variable conn de
	 * BasicFunctions
	 * 
	 * @return
	 */
	public static boolean createConn() {
		boolean retorno = false;

		//
		BasicFunctions.conn = DataAccess.getConnection();
		if (BasicFunctions.conn != null) {
			retorno = true;
		}

		return retorno;
	}

	/**
	 * Se crea el adaptador y las sessiones y el login con el motor de INET
	 * 
	 * @return
	 */
	public static void createLogin() {
		if (BasicFunctions.adapterIO == null) {
			BasicFunctions.adapterIO = new AdapterIO();

		}

		if (BasicFunctions.login == null) {
			BasicFunctions.login = new Login();
			BasicFunctions.login.initiation();
		}
	}

	public static void startVariables() {
		SimpleDateFormat SDF = new SimpleDateFormat("yyyMMddHmmss");
		long id_ejecution = Long.parseLong(SDF.format(new Date()));
		System.out.println("ID_EJECUCION GENERADO : " + id_ejecution);

		BasicFunctions.setIdEjecution(id_ejecution);

	}

	public static int getFirtsIdCaseSeq(int escenarioEjecucion) throws SQLException {
//		int firstIdDB = DataAccess.getFirstIdCaseSeq(escenarioEjecucion);
//		return firstIdDB;
		return 0;
	}

	public static void imprimir(String vari) {
		System.out.println(
				"\n#####################\nCLASE: " + vari.getClass() + "VARIABLE: " + vari + "\n#####################");
	}

	public static void imprimir(int vari) {
		System.out.println("\n#####################\nVARIABLE ENTERA: " + vari + "\n#####################");
	}

	public static void imprimir(boolean vari) {
		System.out.println("\n#####################\nVARIABLE BOOLEAN: " + vari + "\n#####################");
	}

}
