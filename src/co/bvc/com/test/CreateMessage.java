package co.bvc.com.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import co.bvc.com.basicfix.BasicFunctions;
import co.bvc.com.basicfix.Constantes;
import co.bvc.com.basicfix.DataAccess;
import co.bvc.com.dao.domain.RespuestaConstrucccionMsgFIX;
import quickfix.FieldNotFound;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.StringField;
import quickfix.field.BeginString;
import quickfix.field.BidSize;
import quickfix.field.BidYield;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.NoPartyIDs;
import quickfix.field.OfferSize;
import quickfix.field.OfferYield;
import quickfix.field.OrderQty;
import quickfix.field.PartyID;
import quickfix.field.PartyIDSource;
import quickfix.field.PartyRole;
import quickfix.field.QuoteCancelType;
import quickfix.field.QuoteID;
import quickfix.field.QuoteReqID;
import quickfix.field.QuoteRespID;
import quickfix.field.QuoteRespType;
import quickfix.field.SecurityID;
import quickfix.field.SecurityIDSource;
import quickfix.field.SecuritySubType;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TradeReportID;
import quickfix.field.TradeReportTransType;
import quickfix.field.TradeReportType;
import quickfix.field.TransactTime;
import quickfix.field.TrdMatchID;
import quickfix.field.ValidUntilTime;
import quickfix.fix44.Quote;
import quickfix.fix44.QuoteCancel;
import quickfix.fix44.QuoteRequest;
import quickfix.fix44.QuoteResponse;
import quickfix.fix44.TradeCaptureReport;
import quickfix.fix44.Message.Header;

public class CreateMessage {

	
      public RespuestaConstrucccionMsgFIX createAE(ResultSet resultSet) throws FieldNotFound {
		
		RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();

		
		String queryParties = "SELECT linea.ID_ESCENARIO, partes.AE_PARTYID, partes.AE_PARTYIDSOURCE, partes.AE_PARTYROLE, partes.RECEIVER_SESSION\r\n"
		        + "FROM aut_fix_tcr_datos linea INNER JOIN aut_fix_tcrparty_datos partes\r\n"  
				+ "ON linea.ID_CASESEQ = partes.TCR_IDCASE\r\n" + "WHERE linea.ID_CASESEQ = 1"; //+ BasicFunctions.getIdCaseSeqTcr();
			
		ResultSet resultSetParties;
		
		try {
			System.out.println("===================================>>>>>>>>>>>"+resultSet.getString("AE_SECSUBTYPE"));
			
			DateTimeFormatter formato = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
			LocalDateTime transactTime = LocalDateTime.parse(resultSet.getString("AE_TRANSTIME"), formato);
			
			BasicFunctions.setIniciator(resultSet.getString("ID_AFILIADO"));
			resultSetParties = DataAccess.getQuery(queryParties);
			
			TradeCaptureReport.NoSides noSides = new TradeCaptureReport.NoSides();
			
			String strTradeRepId = BasicFunctions.getIdEjecution() + resultSet.getString("ID_CASE") + "_AE";
			TradeReportID tradeReportID = new TradeReportID(strTradeRepId);
			TradeCaptureReport tcr = new TradeCaptureReport();
			tcr.setField(tradeReportID);	
			Header header = (Header) tcr.getHeader();
			header.setField(new BeginString(Constantes.PROTOCOL_FIX_VERSION)); // 8
			tcr.setField(new TradeReportTransType(0));
			tcr.setField(new TradeReportType(96));
	        tcr.setField(new TrdMatchID(resultSet.getString("AE_TRMATCHID")));
	        tcr.setField(new LastPx(resultSet.getDouble("AE_LASTPX")));
	        tcr.setField(new LastQty(resultSet.getDouble("AE_LASTQTY")));
	        tcr.setField(new TransactTime(transactTime));
//	        tcr.setField(new SecurityID(resultSet.getString("")));
//	        tcr.setField(new SecurityIDSource(BasicFunctions.getSecurityIdSource()));
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
			
			respuestaMessage.setListSessiones(list);
			
			System.out.println("***************");
			System.out.println("** AE CREADO  **");
			System.out.println(tcr);
			System.out.println("***************");

			return respuestaMessage;

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;

	}
	
      
      public RespuestaConstrucccionMsgFIX createAE_R(ResultSet resultSet) {
    	  
    	  RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();
    	  
    	  
    	  
    	  
    	  
    	  
    	  
		return respuestaMessage;
    	  
      }
      

}
