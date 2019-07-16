package co.bvc.com.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import co.bvc.com.basicfix.BasicFunctions;
import co.bvc.com.basicfix.Constantes;
import co.bvc.com.basicfix.DataAccess;
import co.bvc.com.dao.domain.RespuestaConstrucccionMsgFIX;
import quickfix.FieldNotFound;
import quickfix.field.BeginString;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.SecuritySubType;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TradeReportID;
import quickfix.field.TradeReportTransType;
import quickfix.field.TradeReportType;
import quickfix.field.TransactTime;
import quickfix.field.TrdMatchID;
import quickfix.fix44.Message.Header;
import quickfix.fix44.TradeCaptureReport;

public class CreateMessage {

	
      public RespuestaConstrucccionMsgFIX createAE(ResultSet resultSet) throws FieldNotFound {
		
		RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();

		
		try {
						
			DateTimeFormatter formato = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS");
			LocalDateTime transactTime = LocalDateTime.parse(resultSet.getString("AE_TRANSTIME"), formato);
			
			BasicFunctions.setIniciator(resultSet.getString("ID_AFILIADO"));
			
			TradeCaptureReport.NoSides noSides = new TradeCaptureReport.NoSides();
			
			String strTradeRepId = BasicFunctions.getIdEjecution() + resultSet.getString("ID_CASE") + "_AE";
			TradeReportID tradeReportID = new TradeReportID(strTradeRepId);
			TradeCaptureReport tcr = new TradeCaptureReport();
			tcr.setField(tradeReportID);	
			Header header = (Header) tcr.getHeader();
			header.setField(new BeginString(Constantes.PROTOCOL_FIX_VERSION)); // 8
			tcr.setField(new TradeReportTransType(resultSet.getInt("AE_TRADTRANTYPE")));
			tcr.setField(new TradeReportType(resultSet.getInt("AE_TRADEREPTYPE")));
	        tcr.setField(new TrdMatchID(resultSet.getString("AE_TRMATCHID")));
	        tcr.setField(new LastPx(resultSet.getDouble("AE_LASTPX")));
	        tcr.setField(new LastQty(resultSet.getDouble("AE_LASTQTY")));
	        tcr.setField(new TransactTime(transactTime));
			tcr.setField(new Symbol(resultSet.getString("AE_SYMBOL")));
			tcr.setField(new SecuritySubType(resultSet.getString("AE_SECSUBTYPE")));
			noSides.set(new Side(resultSet.getString("AE_SIDE").charAt(0)));
			tcr.addGroup(noSides);			
			respuestaMessage.setMessage(tcr);
			
			List<String> list = new ArrayList<String>();
			String idAfiliado = resultSet.getString("ID_AFILIADO");
			String contrafirm = resultSet.getString("CONTRAFIRM");
			list.add(idAfiliado);
			list.add(contrafirm);
			
			//Si se envía aceptación se crean dos sessiones adicionales en cache.
			if(tcr.getString(856)=="97") {
				list.add(idAfiliado+"_ER");
				list.add(contrafirm+"_ER");
			}
			
			respuestaMessage.setListSessiones(list);

			return respuestaMessage;

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;

	}
    
    public RespuestaConstrucccionMsgFIX createAE_R(ResultSet resultSet) throws SQLException, FieldNotFound {
    	  
    	RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();
    	  
    	TradeCaptureReport tcrR = new TradeCaptureReport();
        TradeCaptureReport.NoSides noSides = new TradeCaptureReport.NoSides();
    	  
    	DateTimeFormatter formato = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS");
		LocalDateTime transactTime = LocalDateTime.parse(resultSet.getString("AE_TRANSTIME"), formato);
		
		String strTradeRepId = BasicFunctions.getIdEjecution() + resultSet.getString("ID_CASE") + "_AE_R";
		TradeReportID tradeReportID = new TradeReportID(strTradeRepId);
		tcrR.setField(tradeReportID);
		Header header = (Header) tcrR.getHeader();
		header.setField(new BeginString(Constantes.PROTOCOL_FIX_VERSION));
		tcrR.setField(new TradeReportTransType(resultSet.getInt("AE_TRADTRANTYPE")));
		tcrR.setField(new TradeReportType(resultSet.getInt("AE_TRADEREPTYPE")));
		tcrR.setField(new TrdMatchID(resultSet.getString("AE_TRMATCHID")));
    	tcrR.setField(new Symbol(resultSet.getString("AE_SYMBOL")));
    	tcrR.setField(new SecuritySubType(resultSet.getString("AE_SECSUBTYPE")));
    	tcrR.setField(new LastPx(resultSet.getDouble("AE_LASTPX")));
    	tcrR.setField(new LastQty(resultSet.getDouble("AE_LASTQTY")));
    	tcrR.setField(new TransactTime(transactTime));
    	noSides.set(new Side(resultSet.getString("AE_SIDE").charAt(0)));
		tcrR.addGroup(noSides);	
    	  
    	respuestaMessage.setMessage(tcrR);
    	  
    	List<String> list = new ArrayList<String>();
		String idAfiliado = resultSet.getString("ID_AFILIADO");
		String contrafirm = resultSet.getString("CONTRAFIRM");
		list.add(idAfiliado);
		list.add(contrafirm);
		
		//Si se envía aceptación se crean dos sessiones adicionales en cache.
		if(tcrR.getString(856)=="97") {
			list.add(idAfiliado+"_ER");
			list.add(contrafirm+"_ER");
		}
		
		respuestaMessage.setListSessiones(list);
    	  
		return respuestaMessage;
    	  
      }
      

}
