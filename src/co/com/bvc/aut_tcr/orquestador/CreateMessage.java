package co.com.bvc.aut_tcr.orquestador;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import co.com.bvc.aut_tcr.basicfix.BasicFunctions;
import co.com.bvc.aut_tcr.basicfix.Constantes;
import co.com.bvc.aut_tcr.basicfix.DataAccess;
import co.com.bvc.aut_tcr.basicfix.Login;
import co.com.bvc.aut_tcr.dao.domain.RespuestaConstrucccionMsgFIX;
import quickfix.FieldNotFound;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionNotFound;
import quickfix.StringField;
import quickfix.field.BeginString;
import quickfix.field.BidSize;
import quickfix.field.BidYield;
import quickfix.field.LastPx;
import quickfix.field.LastQty;
import quickfix.field.NoPartyIDs;
import quickfix.field.NoSides;
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
import quickfix.field.SenderCompID;
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
import quickfix.fix44.component.Parties;
import quickfix.fix44.Message;
import quickfix.fix44.Message.Header;

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
			
			respuestaMessage.setListSessiones(list);

			return respuestaMessage;

			} catch (SQLException e) {
				e.printStackTrace();
			}

			return null;

	}
		
	      
	public RespuestaConstrucccionMsgFIX createAE_R(ResultSet resultSet) throws SQLException {
	    	  
		RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();
	    	  
		TradeCaptureReport trcR = new TradeCaptureReport();
		TradeCaptureReport.NoSides noSides = new TradeCaptureReport.NoSides();
		  
		DateTimeFormatter formato = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS");
		LocalDateTime transactTime = LocalDateTime.parse(resultSet.getString("AE_TRANSTIME"), formato);
		
		String strTradeRepId = BasicFunctions.getIdEjecution() + resultSet.getString("ID_CASE") + "_AE_R";
		TradeReportID tradeReportID = new TradeReportID(strTradeRepId);
		trcR.setField(tradeReportID);
		Header header = (Header) trcR.getHeader();
		header.setField(new BeginString(Constantes.PROTOCOL_FIX_VERSION));
		trcR.setField(new TradeReportTransType(resultSet.getInt("AE_TRADTRANTYPE")));
		trcR.setField(new TradeReportType(resultSet.getInt("AE_TRADEREPTYPE")));
		trcR.setField(new TrdMatchID(resultSet.getString("AE_TRMATCHID")));
		trcR.setField(new Symbol(resultSet.getString("AE_SYMBOL")));
		trcR.setField(new SecuritySubType(resultSet.getString("AE_SECSUBTYPE")));
		trcR.setField(new LastPx(resultSet.getDouble("AE_LASTPX")));
		trcR.setField(new LastQty(resultSet.getDouble("AE_LASTQTY")));
		trcR.setField(new TransactTime(transactTime));
		noSides.set(new Side(resultSet.getString("AE_SIDE").charAt(0)));
		trcR.addGroup(noSides);	
		  
		respuestaMessage.setMessage(trcR);
		  
		List<String> list = new ArrayList<String>();
		String idAfiliado = resultSet.getString("ID_AFILIADO");
		String contrafirm = resultSet.getString("CONTRAFIRM");
		list.add(idAfiliado);
		list.add(contrafirm);
				
		respuestaMessage.setListSessiones(list);
			  
		return respuestaMessage;
			  
	}
	      

//public RespuestaConstrucccionMsgFIX createAE(ResultSet resultSet) throws FieldNotFound {
//		
//		RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();
//
//		
////		String queryParties = "SELECT linea.ID_ESCENARIO, partes.AE_PARTYID, partes.AE_PARTYIDSOURCE, partes.AE_PARTYROLE, partes.RECEIVER_SESSION\r\n"
////		        + "FROM aut_fix_tcr_datos linea INNER JOIN aut_fix_tcrparty_datos partes\r\n"  
////				+ "ON linea.ID_CASESEQ = partes.TCR_IDCASE\r\n" + "WHERE linea.ID_CASESEQ = 1"; //+ BasicFunctions.getIdCaseSeqTcr();
////			
////		ResultSet resultSetParties;
////		
//		try {
//			System.out.println("===================================>>>>>>>>>>>"+resultSet.getString("AE_SECSUBTYPE"));
//			
//			DateTimeFormatter formato = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S");
//			LocalDateTime transactTime = LocalDateTime.parse(resultSet.getString("AE_TRANSTIME"), formato);
//			
//			BasicFunctions.setIniciator(resultSet.getString("ID_AFILIADO"));
//			BasicFunctions.setReceptor(resultSet.getString("CONTRAFIRM"));
//			
//			TradeCaptureReport.NoSides noSides = new TradeCaptureReport.NoSides();
//			
//			String strTradeRepId = BasicFunctions.getIdEjecution() + resultSet.getString("ID_CASE") + "_AE";
//			TradeReportID tradeReportID = new TradeReportID(strTradeRepId);
//			TradeCaptureReport tcr = new TradeCaptureReport();
//			tcr.setField(tradeReportID);	
//			Header header = (Header) tcr.getHeader();
//			header.setField(new BeginString(Constantes.PROTOCOL_FIX_VERSION)); // 8
//			
////			tcr.setField(new TradeReportTransType(0));
////			tcr.setField(new TradeReportType(96));
//			
//			tcr.setField(new TradeReportTransType(resultSet.getInt("AE_TRADTRANTYPE"))); 	//487
//	        tcr.setField(new TradeReportType(resultSet.getInt("AE_TRADEREPTYPE")));		//856
//	        tcr.setField(new TrdMatchID(resultSet.getString("AE_TRMATCHID")));		//880
//	        tcr.setField(new LastPx(resultSet.getDouble("AE_LASTPX")));			//31
//	        tcr.setField(new LastQty(resultSet.getDouble("AE_LASTQTY")));		//32
//	        tcr.setField(new TransactTime(transactTime));		//60
////	        tcr.setField(new SecurityID(resultSet.getString("")));
////	        tcr.setField(new SecurityIDSource(BasicFunctions.getSecurityIdSource()));
//			tcr.setField(new Symbol(resultSet.getString("AE_SYMBOL")));		//55
//			tcr.setField(new SecuritySubType(resultSet.getString("AE_SECSUBTYPE")));	//762
//			noSides.set(new Side(resultSet.getString("AE_SIDE").charAt(0)));	//54
//			tcr.addGroup(noSides);		
//
//			respuestaMessage.setMessage(tcr);
//			
//			List<String> list = new ArrayList<String>();
//			String idAfiliado = resultSet.getString("ID_AFILIADO");
//			String contrafirm = resultSet.getString("CONTRAFIRM");
//			
//			list.add(idAfiliado);
//			list.add(contrafirm);
//			
//			//Si la cancelación es aceptada por la contraparte 
//			//se deben crear dos sesiones adicionales en caché para validar los ER
//			if(resultSet.getInt("AE_TRADEREPTYPE") == 97) {
//				list.add(idAfiliado+"ER");
//				list.add(contrafirm+"ER");
//			}
//			
//			respuestaMessage.setListSessiones(list);
//
//			System.out.println("***************");
//			System.out.println("** AE CREADO  **");
//			System.out.println(tcr);
//			System.out.println("***************");
//			
////			for(String ses:list) {
////				System.out.println("SEssion creada: "+ ses);
////			}
//
//			return respuestaMessage;
//
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//
//		return null;
//
//	}
	
//    public RespuestaConstrucccionMsgFIX createAE(ResultSet resultSet) throws FieldNotFound {
//		
//		RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();
//
//		String queryParties = "SELECT linea.ID_ESCENARIO, partes.AE_PARTYID, partes.AE_PARTYIDSOURCE, partes.AE_PARTYROLE, partes.RECEIVER_SESSION\r\n" + 
//				"FROM aut_fix_tcr_datos linea INNER JOIN aut_fix_tcrparty_datos partes\r\n" + 
//				"ON linea.ID_CASESEQ = partes.TCR_IDCASE WHERE linea.ID_CASESEQ = 1;";  //BasicFunctions.getIdCaseSeq();
//		
//		ResultSet consultTrc;
//
//		System.out.println("******************\n SECUENCIA DE ESCENARIO: "+ BasicFunctions.getIdCaseSeq() + "\n******************" );
//		
//		ResultSet resultSetParties;
//		
//		try {
//			System.out.println("*******************\n INGRESA A TRY DE CREAR EA \n**********************");
//			BasicFunctions.setIniciator(resultSet.getString("ID_AFILIADO"));
//			resultSetParties = DataAccess.getQuery(queryParties);
//			
//			TradeCaptureReport.NoSides noSides = new TradeCaptureReport.NoSides();
//			
//			String strTradeRepId = BasicFunctions.getIdEjecution() + resultSet.getString("ID_CASE") + "_AE";
//			TradeReportID tradeReportID = new TradeReportID(strTradeRepId);
//			TradeCaptureReport tcr = new TradeCaptureReport();
//			tcr.setField(tradeReportID);	
//			Header header = (Header) tcr.getHeader();
//			header.setField(new BeginString(Constantes.PROTOCOL_FIX_VERSION)); // 8
//			tcr.setField(new TradeReportTransType(0));
//			tcr.setField(new TradeReportType(96));
//			tcr.setField(new TrdMatchID(resultSet.getString("AE_TRMATCHID")));
//			
//	        tcr.setField(new LastPx(resultSet.getDouble("AE_LASTPX")));
//	        tcr.setField(new LastQty(resultSet.getDouble("AE_LASTQTY")));
//	        
//	        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.0");
//			LocalDateTime dateTime = LocalDateTime.parse(resultSet.getString("AE_TRANSTIME"), formatter);
//			tcr.setField(new TransactTime(dateTime)); // "20190404-23:00:00";
//	        
//	        //tcr.setField(new TransactTime(resultSet.getString("AE_TRANSTIME")));
//	        
//	        //tcr.setField(new SecurityID(resultSet.getString("AE_SECID")));
//	        tcr.setField(new SecurityIDSource(resultSet.getString("AE_SECIDSOURCE")));
//			tcr.set(new Symbol(resultSet.getString("AE_SYMBOL")));
//			tcr.setField(new SecuritySubType(resultSet.getString("AE_SECSUBTYPE")));
//			//tcr.setField(new NoSides(1));
//			noSides.set(new Side(resultSet.getString("AE_SIDE").charAt(0)));
//			
//			
//			//TradeCaptureReport.NoSides.NoPartyIDs parte = new TradeCaptureReport.NoSides.NoPartyIDs();
//			
//			//Parties parties = new Parties();
//			
//			//Parties
////			while(resultSetParties.next()) {
////				
////				parte.set(new PartyID(resultSetParties.getString("RQ_PARTYID")));
////				parte.set(new PartyIDSource('C'));
////				parte.set(new PartyRole(resultSetParties.getInt("RQ_PARTYROLE")));
////				
////				parties.addGroup(parte);
////				noSides.set(parties);
////			}
////
//			tcr.addGroup(noSides);			
//
////			QuoteRequest.NoRelatedSym noRelatedSym = new QuoteRequest.NoRelatedSym();
//					
////			List<String> list = new ArrayList<String>();
////			String idAfiliado = resultSet.getString("ID_AFILIADO");
////			list.add(idAfiliado);
////
////			if (noRelatedSym.getInt(NoPartyIDs.FIELD) == 1) {
////				System.out.println("\n\nPARA TODO EL MERCADO.....\n");
////				BasicFunctions.setAllMarket(true);
////				list.clear();
////				Iterator<String> itSessiones = Login.getMapSessiones().keySet().iterator();
////
////				while (itSessiones.hasNext()) {
////					String idAfiliadoMap = itSessiones.next();
////					list.add(idAfiliadoMap);
////					System.out.println("Nuevo Afiliado: " + idAfiliadoMap + " -> Session: "
////							+ Login.getMapSessiones().get(idAfiliadoMap));
////				}
////				// Se asigna Session+r Para validar R prima al inicializador
////				list.add(idAfiliado + "AE");
////
////			}
//
////			tcr.addGroup(noRelatedSym);
//
//			respuestaMessage.setMessage(tcr);
////			respuestaMessage.setListSessiones(list);
//
//			System.out.println("***************");
//			System.out.println("** AE CREADO  **");
//			System.out.println(tcr);
//			System.out.println("***************");
//
//			System.out.println(respuestaMessage);
//			return respuestaMessage;
//
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//
//		return null;
//
//	}
	
	public RespuestaConstrucccionMsgFIX createR(ResultSet resultSet)
			throws SessionNotFound, SQLException, FieldNotFound {

		RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();

		String queryParties = "SELECT linea.ID_ESCENARIO, partes.RQ_PARTYID, partes.RQ_PARTYIDSOURCE, partes.RQ_PARTYROLE, partes.RECEIVER_SESSION\r\n"
				+ "FROM aut_fix_rfq_datos linea INNER JOIN aut_fix_rfqparty_datos partes\r\n"
				+ "	ON linea.ID_CASESEQ = partes.RFQ_IDCASE\r\n" + "WHERE linea.ID_CASESEQ ="
				+ BasicFunctions.getIdCaseSeq();

		ResultSet resultSetParties;

		try {
			BasicFunctions.setAllMarket(false);
			BasicFunctions.setIniciator(resultSet.getString("ID_AFILIADO"));
			resultSetParties = DataAccess.getQuery(queryParties);

			String strQuoteReqId = BasicFunctions.getIdEjecution() + resultSet.getString("ID_CASE") + "_R";

			QuoteReqID quoteReqID = new QuoteReqID(strQuoteReqId); // 131
			QuoteRequest quoteRequest = new QuoteRequest(quoteReqID); // 35 --> R
			Header header = (Header) quoteRequest.getHeader();
			header.setField(new BeginString(Constantes.PROTOCOL_FIX_VERSION)); // 8
			QuoteRequest.NoRelatedSym noRelatedSym = new QuoteRequest.NoRelatedSym();

			Symbol symbol = resultSet.getString("RQ_SYMBOL") == null ? new Symbol("   ")
					: new Symbol(resultSet.getString("RQ_SYMBOL"));
			noRelatedSym.set(symbol);
//			noRelatedSym.set(new Symbol(resultSet.getString("RQ_SYMBOL")));
			noRelatedSym.setField(new SecurityIDSource("M"));
			noRelatedSym.setField(new OrderQty(resultSet.getDouble("RQ_ORDERQTY")));
			noRelatedSym.setField(new StringField(54, resultSet.getString("RQ_SIDE")));
			noRelatedSym.setField(new SecuritySubType(resultSet.getString("RQ_SECSUBTYPE")));
			noRelatedSym.setField(new NoPartyIDs());

			QuoteRequest.NoRelatedSym.NoPartyIDs parte = new QuoteRequest.NoRelatedSym.NoPartyIDs();

			List<String> list = new ArrayList<String>();
			String idAfiliado = resultSet.getString("ID_AFILIADO");
			list.add(idAfiliado);
//			 Parties
			while (resultSetParties.next()) {
				String rSession = resultSetParties.getString("RECEIVER_SESSION");
				if (rSession != null) {
					list.add(rSession);
				}

				parte.set(new PartyID(resultSetParties.getString("RQ_PARTYID")));
				parte.set(new PartyIDSource('C'));
				parte.set(new PartyRole(resultSetParties.getInt("RQ_PARTYROLE")));

				noRelatedSym.addGroup(parte);
			}
			if (noRelatedSym.getInt(NoPartyIDs.FIELD) == 1) {
				System.out.println("\n\nPARA TODO EL MERCADO.....\n");
				BasicFunctions.setAllMarket(true);
				list.clear();
				Iterator<String> itSessiones = Login.getMapSessiones().keySet().iterator();

				while (itSessiones.hasNext()) {
					String idAfiliadoMap = itSessiones.next();
					list.add(idAfiliadoMap);
					System.out.println("Nuevo Afiliado: " + idAfiliadoMap + " -> Session: "
							+ Login.getMapSessiones().get(idAfiliadoMap));
				}
				// Se asigna Session+r Para validar R prima al inicializador
				list.add(idAfiliado + "R");

			}

			quoteRequest.addGroup(noRelatedSym);

			respuestaMessage.setMessage(quoteRequest);
			respuestaMessage.setListSessiones(list);

			System.out.println("***************");
			System.out.println("** R CREADO  **");
			System.out.println(quoteRequest);
			System.out.println("***************");

			return respuestaMessage;

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public RespuestaConstrucccionMsgFIX createS(ResultSet resultSet, String strQuoteReqId)
			throws SessionNotFound, SQLException {

		RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();

		String queryParties = "SELECT linea.ID_ESCENARIO, partes.RQ_PARTYID, partes.RQ_PARTYIDSOURCE, partes.RQ_PARTYROLE, partes.RECEIVER_SESSION\r\n"
				+ "FROM aut_fix_rfq_datos linea INNER JOIN aut_fix_rfqparty_datos partes\r\n"
				+ "	ON linea.ID_CASESEQ = partes.RFQ_IDCASE\r\n" + "WHERE linea.ID_CASESEQ ="
				+ BasicFunctions.getIdCaseSeq();

		ResultSet resultSetParties;
//		String cIdRandom = Integer.toString((int) ((Math.random() * 80_000_000) + 1_000_000));
//
//		BasicFunctions.setQuoteIdGenered(cIdRandom);

		System.out.println("QUOTE ID GENERADO: " + BasicFunctions.getQuoteIdGenered());

		try {
			BasicFunctions.setReceptor(resultSet.getString("ID_AFILIADO"));
			resultSetParties = DataAccess.getQuery(queryParties);

			String strQuoteId = BasicFunctions.getIdEjecution() + resultSet.getString("ID_CASE") + "_S";

			QuoteID quoteID = new QuoteID(strQuoteId);
			Quote quote = new Quote(quoteID); // 35 --> S

			Header header = (Header) quote.getHeader();
			header.setField(new BeginString(Constantes.PROTOCOL_FIX_VERSION)); // 8

			quote.setField(new QuoteReqID(strQuoteReqId)); // 131
			quote.set(new Symbol(resultSet.getString("RQ_SYMBOL")));
			quote.setField(new SecuritySubType(resultSet.getString("RQ_SECSUBTYPE")));
			quote.setField(new OfferSize(resultSet.getDouble("RQ_OFFERSIZE")));
			quote.setField(new OfferYield(resultSet.getDouble("RQ_OFFERYIELD")));

			quote.setField(new BidYield(resultSet.getDouble("RQ_BIDYIELD")));
			quote.setField(new BidSize(resultSet.getDouble("RQ_BIDSIZE")));

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.0");
			LocalDateTime dateTime = LocalDateTime.parse(resultSet.getString("RQ_VALIDUNTILTIME"), formatter);
			quote.setField(new ValidUntilTime(dateTime)); // "20190404-23:00:00";

			// Parties
			Quote.NoPartyIDs parte = new Quote.NoPartyIDs();

			List<String> list = new ArrayList<String>();
			list.add(BasicFunctions.getReceptor());

			while (resultSetParties.next()) {
				String rSession = resultSetParties.getString("RECEIVER_SESSION");
				if (rSession != null) {
					list.add(rSession);
				}
				parte.set(new PartyID(resultSetParties.getString("RQ_PARTYID")));
				parte.set(new PartyIDSource('C'));
				parte.set(new PartyRole(resultSetParties.getInt("RQ_PARTYROLE")));

				quote.addGroup(parte);
			}

			if (BasicFunctions.isAllMarket()) {

				System.out.println("\n\nPARA TODO EL MERCADO.....\n");
				Iterator<String> itSessiones = Login.getMapSessiones().keySet().iterator();

				list.clear();
				while (itSessiones.hasNext()) {
					String idAfiliadoMap = itSessiones.next();
					list.add(idAfiliadoMap);
					System.out.println("Nuevo Afiliado: " + idAfiliadoMap + " -> Session: "
							+ Login.getMapSessiones().get(idAfiliadoMap));
				}
				// Se asigna Session+r Para validar RC prima al inicializador
				list.add(BasicFunctions.getIniciator() + "R");

			}

			respuestaMessage.setListSessiones(list);
			respuestaMessage.setMessage(quote);

			System.out.println("***************");
			System.out.println("** S CREADO  **");
			System.out.println(quote);
			System.out.println("***************");

			return respuestaMessage;

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;

	}

	public RespuestaConstrucccionMsgFIX createAJ(ResultSet resultset, String strQuoteId)
			throws SessionNotFound, SQLException {

		RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();

		String strQuoteRespId = BasicFunctions.getIdEjecution() + resultset.getString("ID_CASE") + "_AJ";

		try {
			QuoteRespID quoteRespID = new QuoteRespID(strQuoteRespId);
			QuoteRespType qouteRespType = new QuoteRespType(resultset.getInt("RQ_QUORESPTYPE"));
			QuoteResponse quoteResponse = new QuoteResponse(quoteRespID, qouteRespType); // 35 --> AJ

			Header header = (Header) quoteResponse.getHeader();
			header.setField(new BeginString(Constantes.PROTOCOL_FIX_VERSION)); // 8

			quoteResponse.setField(new QuoteID(strQuoteId));
			quoteResponse.setField(new StringField(54, resultset.getString("RQ_SIDE")));
			quoteResponse.set(new Symbol(resultset.getString("RQ_SYMBOL")));
			quoteResponse.setField(new StringField(762, resultset.getString("RQ_SECSUBTYPE")));

			System.out.println("NOS Message Sent : " + quoteResponse);

			respuestaMessage.setMessage(quoteResponse);

			List<String> list = new ArrayList<String>();
			list.add(BasicFunctions.getIniciator());
			list.add(BasicFunctions.getReceptor());

			respuestaMessage.setListSessiones(list);

			System.out.println("****************");
			System.out.println("** AJ CREADO  **");
			System.out.println(quoteResponse);
			System.out.println("****************");

			return respuestaMessage;

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
	}

	public RespuestaConstrucccionMsgFIX createZ(final SessionID sessionId, ResultSet resultSet)
			throws SessionNotFound, SQLException {

		System.out.println("------------------------------\nDATOS RECIBIDOS PARA Z....\nSession: " + sessionId);

		RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();

		QuoteCancel quoteCancel = new QuoteCancel();
		Header header = (Header) quoteCancel.getHeader();
		header.setField(new BeginString(Constantes.PROTOCOL_FIX_VERSION)); // 8

		quoteCancel.setField(new QuoteCancelType(5));// RQ_QUOTECANCTYPE
		quoteCancel.setField(new QuoteID(BasicFunctions.getIdEjecution() + resultSet.getString("ID_CASE") + "_S"));

		System.out.println("Nos Message Sent : " + quoteCancel);

		respuestaMessage.setMessage(quoteCancel);

		List<String> list = new ArrayList<String>();

		list.add(BasicFunctions.getIniciator());
		list.add(BasicFunctions.getReceptor());

		respuestaMessage.setListSessiones(list);

		System.out.println("****************");
		System.out.println("** Z CREADO  **");
		System.out.println(quoteCancel);
		System.out.println("****************");

		return respuestaMessage;
	}

}
