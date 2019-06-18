package co.bvc.com.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import co.bvc.com.basicfix.DataAccess;

public class CreateReport {
	static DataAccess conexiona = new DataAccess();

	@SuppressWarnings("static-access")
	public static void maina() throws SQLException, IOException {

		ResultSet rs = null;

		String query = "SELECT ID_EJECUCION, ID_ESCENARIO, COD_CASO, ID_SECUENCIA, ESTADO_EJECUCION, DESCRIPCION_VALIDACION, MENSAJE, CODIGO_ERROR, DESC_ERROR FROM bvc_automation_db.aut_log_ejecucion logEjec LEFT JOIN `bvc_automation_db`.`aut_fix_rfq_codigo_error` codError on logEjec.CODIGO_ERROR = codError.ID_CODIGO WHERE ID_ESCENARIO LIKE \"%FIX%\" AND ID_EJECUCION = (SELECT max(ID_EJECUCION) FROM bvc_automation_db.aut_log_ejecucion WHERE ID_ESCENARIO LIKE \"%FIX%\") ORDER BY COD_CASO ASC, ID_SECUENCIA ASC;";

		try {
			rs = conexiona.getQuery(query);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		rs.next();
		String num = rs.getString("ID_EJECUCION");
		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss");
		System.out.println(System.getProperty("user.dir") + "/Reporte iteracion " + num + " Fecha "
				+ formatter.format(date) + ".csv");
		FileWriter pw = new FileWriter(System.getProperty("user.dir") + "/Reporte iteracion " + num + " Fecha "
				+ formatter.format(date) + ".csv");
		String casoActual = "";
		double totalExitososCaso = 0;
		double totalFallidosCaso = 0;
		double totalExitosos = 0;
		double totalFallidos = 0;
		double porcentaje = 0;
		pw.append("CASO");
		pw.append(",");
		pw.append("PASO");
		pw.append(",");
		pw.append("TIPO");
		pw.append(",");
		pw.append("ESTADO");
		pw.append(",");
		pw.append("TOTAL EXITOSOS");
		pw.append(",");
		pw.append("TOTAL FALLIDOS");
		pw.append(",");
		pw.append("PORCENTAJE VALIDACION");
		pw.append(",");
		pw.append("DESCRIPCION VALIDACION");
		pw.append(",");
		pw.append("MENSAJE");
		pw.append(",");
		pw.append("CODIGO ERROR");
		pw.append(",");
		pw.append("ERROR");
		pw.append("\r\n");
		pw.append(rs.getString("COD_CASO"));
		pw.append(",");
		pw.append(rs.getString("ID_SECUENCIA"));
		pw.append(",");
		pw.append(rs.getString("ID_ESCENARIO"));
		pw.append(",");
		pw.append(rs.getString("ESTADO_EJECUCION"));
		pw.append(",");
		pw.append("");
		pw.append(",");
		pw.append("");
		pw.append(",");
		pw.append("");
		pw.append(",");
		pw.append(rs.getString("DESCRIPCION_VALIDACION"));
		casoActual = rs.getString("COD_CASO");
		if (rs.getString("ESTADO_EJECUCION").equals("EXITOSO")) {
			totalExitosos++;
			totalExitososCaso++;
		} else {
			pw.append(",");
			pw.append(rs.getString("MENSAJE"));
			pw.append(",");
			pw.append(rs.getString("CODIGO_ERROR"));
			pw.append(",");
			pw.append(rs.getString("DESC_ERROR"));
			totalFallidos++;
			totalFallidosCaso++;
		}
		pw.append("\r\n");

		while (rs.next()) {
			if (!rs.getString("COD_CASO").equals(casoActual)) {
				porcentaje = (totalExitosos / (totalExitosos + totalFallidos)) * 100;
				pw.append("Fin de caso " + casoActual);
				pw.append(",");
				pw.append("");
				pw.append(",");
				pw.append("");
				pw.append(",");
				pw.append("");
				pw.append(",");
				pw.append(totalExitosos + "");
				pw.append(",");
				pw.append(totalFallidos + "");
				pw.append(",");
				pw.append(String.format("%.2f", porcentaje).replace(",", ".") + "%");
				pw.append(",");
				pw.append("");
				pw.append("\r\n");
				totalExitosos = 0;
				totalFallidos = 0;
				porcentaje = 0;
			}
			pw.append(rs.getString("COD_CASO"));
			pw.append(",");
			pw.append(rs.getString("ID_SECUENCIA"));
			pw.append(",");
			pw.append(rs.getString("ID_ESCENARIO"));
			pw.append(",");
			pw.append(rs.getString("ESTADO_EJECUCION"));
			pw.append(",");
			pw.append("");
			pw.append(",");
			pw.append("");
			pw.append(",");
			pw.append("");
			pw.append(",");
			pw.append(rs.getString("DESCRIPCION_VALIDACION"));
			casoActual = rs.getString("COD_CASO");
			if (rs.getString("ESTADO_EJECUCION").equals("EXITOSO")) {
				totalExitosos++;
				totalExitososCaso++;
			} else {
				pw.append(",");
				pw.append(rs.getString("MENSAJE"));
				pw.append(",");
				pw.append(rs.getString("CODIGO_ERROR"));
				pw.append(",");
				pw.append(rs.getString("DESC_ERROR"));
				totalFallidos++;
				totalFallidosCaso++;
			}
			pw.append("\r\n");
		}
		porcentaje = (totalExitosos / (totalExitosos + totalFallidos)) * 100;
		pw.append("Fin de caso " + casoActual);
		pw.append(",");
		pw.append("");
		pw.append(",");
		pw.append("");
		pw.append(",");
		pw.append("");
		pw.append(",");
		pw.append(totalExitosos + "");
		pw.append(",");
		pw.append(totalFallidos + "");
		pw.append(",");
		pw.append(String.format("%.2f", porcentaje).replace(",", ".") + "%");
		pw.append(",");
		pw.append("");
		pw.append("\r\n");
		porcentaje = (totalExitososCaso / (totalExitososCaso + totalFallidosCaso)) * 100;
		pw.append("\r\n");
		pw.append("Total exitosos:");
		pw.append(",");
		pw.append(totalExitososCaso + "");
		pw.append("\r\n");
		pw.append("Total fallidos:");
		pw.append(",");
		pw.append(totalFallidosCaso + "");
		pw.append("\r\n");
		pw.append("Porcentaje total: ");
		pw.append(",");
		pw.append(String.format("%.2f", porcentaje).replace(",", ".") + "%");
		pw.flush();
		pw.close();
		System.out.println("finished");

	}

}
