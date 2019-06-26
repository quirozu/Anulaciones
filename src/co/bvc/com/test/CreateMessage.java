package co.bvc.com.test;

import java.awt.Component;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import co.bvc.com.basicfix.BasicFunctions;
import co.bvc.com.basicfix.Constantes;
import co.bvc.com.basicfix.DataAccess;
import co.bvc.com.dao.domain.RespuestaConstrucccionMsgFIX;
import quickfix.FieldNotFound;
import quickfix.field.BeginString;
import quickfix.field.NoPartyIDs;
import quickfix.field.NoSides;
import quickfix.field.PartyID;
import quickfix.field.PartyIDSource;
import quickfix.field.PartyRole;
import quickfix.field.SecuritySubType;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.TradeReportID;
import quickfix.field.TradeReportTransType;
import quickfix.field.TradeReportType;
import quickfix.fix44.QuoteRequest;
import quickfix.fix44.TradeCaptureReport;
import quickfix.fix44.Message.Header;
import quickfix.fix44.QuoteCancel;

public class CreateMessage {

	public RespuestaConstrucccionMsgFIX createAE(ResultSet resultSet) throws FieldNotFound {
		
		RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();

		String queryParties = "SELECT linea.ID_ESCENARIO, partes.RQ_PARTYID, partes.RQ_PARTYIDSOURCE, partes.RQ_PARTYROLE, partes.RECEIVER_SESSION\r\n"
				+ "FROM aut_fix_tcr_datos linea INNER JOIN aut_fix_rfqparty_datos partes\r\n"
				+ "	ON linea.ID_CASESEQ = partes.RFQ_IDCASE\r\n" + "WHERE linea.ID_CASESEQ ="
				+ BasicFunctions.getIdCaseSeq();

		System.out.println("******************\n SECUENCIA DE ESCENARIO: "+ BasicFunctions.getIdCaseSeq() + "\n******************" );
		
		ResultSet resultSetParties;
		
		try {
			System.out.println("*******************\n INGRESA A TRY DE CREAR EA \n**********************");
			BasicFunctions.setIniciator(resultSet.getString("ID_AFILIADO"));
			resultSetParties = DataAccess.getQuery(queryParties);
			
			String strTradeRepId = BasicFunctions.getIdEjecution() + resultSet.getString("ID_CASE") + "_AE";
			System.out.println("1");
			TradeReportID tradeReportID = new TradeReportID(strTradeRepId);
			System.out.println("2");
			TradeCaptureReport trc = new TradeCaptureReport();
			System.out.println("3");
			trc.setField(tradeReportID);	
			System.out.println("4");
			Header header = (Header) trc.getHeader();
			System.out.println("5");
			header.setField(new BeginString(Constantes.PROTOCOL_FIX_VERSION)); // 8
			System.out.println("6");
			trc.setField(new TradeReportTransType(0));
			System.out.println("7");
			trc.setField(new TradeReportType(96));
			System.out.println("8");
			trc.setField(new Symbol(resultSet.getString("AE_SYMBOL")));
			System.out.println("9");
			trc.setField(new SecuritySubType(resultSet.getString("AE_SECSUBTYPE")));
			System.out.println("10");
			trc.setField(new NoSides(resultSet.getInt("AE_NOSIDES")));
//			trc.setField(new Side(resultSet.getString("AE_SIDE")));
		

			QuoteRequest.NoRelatedSym noRelatedSym = new QuoteRequest.NoRelatedSym();

			Symbol symbol = resultSet.getString("AE_SYMBOL") == null ?  new Symbol(" ") :  new Symbol(resultSet.getString("AE_SYMBOL"));
//			noRelatedSym.set(symbol);
//			noRelatedSym.setField(new SecurityIDSource("M"));
//			noRelatedSym.isSetField(new LastQty(resultSet.getDouble("AE_LASTQTY")));
//			noRelatedSym.setField(new Side());
//			noRelatedSym.setField(new StringField(54, resultSet.getString("AE_SIDE")));
//			noRelatedSym.setField(new SecuritySubType(resultSet.getString("AE_SECSUBTYPE")));
//			noRelatedSym.setField(new NoPartyIDs());

			
			
			QuoteCancel.NoPartyIDs parte =  new QuoteCancel.NoPartyIDs();
//			QuoteRequest.NoRelatedSym.NoPartyIDs parte = new QuoteRequest.NoRelatedSym.NoPartyIDs();

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
				list.add(idAfiliado + "AE");

			}

			trc.addGroup(noRelatedSym);

			respuestaMessage.setMessage(trc);
			respuestaMessage.setListSessiones(list);

			System.out.println("***************");
			System.out.println("** AE CREADO  **");
			System.out.println(trc);
			System.out.println("***************");

			System.out.println(respuestaMessage);
			return respuestaMessage;

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
		
	}
	
}
