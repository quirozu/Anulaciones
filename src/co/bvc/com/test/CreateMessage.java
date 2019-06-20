package co.bvc.com.test;

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
import quickfix.StringField;
import quickfix.field.BeginString;
import quickfix.field.LastQty;
import quickfix.field.NoPartyIDs;
import quickfix.field.OrderQty;
import quickfix.field.PartyID;
import quickfix.field.PartyIDSource;
import quickfix.field.PartyRole;
import quickfix.field.QuoteReqID;
import quickfix.field.SecurityIDSource;
import quickfix.field.SecuritySubType;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.fix44.QuoteRequest;
import quickfix.fix44.Message.Header;

public class CreateMessage {

	public RespuestaConstrucccionMsgFIX createAE(ResultSet resultSet) throws FieldNotFound {
		
		RespuestaConstrucccionMsgFIX respuestaMessage = new RespuestaConstrucccionMsgFIX();

		String queryParties = "SELECT linea.ID_ESCENARIO, partes.RQ_PARTYID, partes.RQ_PARTYIDSOURCE, partes.RQ_PARTYROLE, partes.RECEIVER_SESSION\r\n"
				+ "FROM aut_fix_rfq_datos linea INNER JOIN aut_fix_rfqparty_datos partes\r\n"
				+ "	ON linea.ID_CASESEQ = partes.RFQ_IDCASE\r\n" + "WHERE linea.ID_CASESEQ ="
				+ BasicFunctions.getIdCaseSeq();

		System.out.println("******************\n ES ESTE "+ BasicFunctions.getIdCaseSeq() + "\n******************" );
		
		ResultSet resultSetParties;
		
		try {
			BasicFunctions.setIniciator(resultSet.getString("ID_AFILIADO"));
			resultSetParties = DataAccess.getQuery(queryParties);
			
			String strQuoteReqId = BasicFunctions.getIdEjecution() + resultSet.getString("ID_CASE") + "_AE";

			QuoteReqID quoteReqID = new QuoteReqID(strQuoteReqId); // 131
			QuoteRequest quoteRequest = new QuoteRequest(quoteReqID); // 35 --> R
			Header header = (Header) quoteRequest.getHeader();
			header.setField(new BeginString(Constantes.PROTOCOL_FIX_VERSION)); // 8
			QuoteRequest.NoRelatedSym noRelatedSym = new QuoteRequest.NoRelatedSym();

			Symbol symbol = resultSet.getString("AE_SYMBOL") == null ?  new Symbol("TFIT19PEDRO") :  new Symbol(resultSet.getString("AE_SYMBOL"));
			noRelatedSym.set(symbol);
			noRelatedSym.setField(new SecurityIDSource("M"));
			noRelatedSym.isSetField(new LastQty(resultSet.getDouble("AE_LASTQTY")));
			noRelatedSym.setField(new Side());
//			noRelatedSym.setField(new StringField(54, resultSet.getString("AE_SIDE")));
			noRelatedSym.setField(new SecuritySubType(resultSet.getString("AE_SECSUBTYPE")));
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

				parte.set(new PartyID(resultSetParties.getString("AE_PARTYID")));
//				parte.set(new PartyIDSource("AE_PARTYIDSOURCE"));
				parte.setField(new StringField(448, resultSet.getString("AE_PARTYIDSOURCE")));
				parte.set(new PartyRole(resultSetParties.getInt("AE_PARTYROLE")));

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

			quoteRequest.addGroup(noRelatedSym);

			respuestaMessage.setMessage(quoteRequest);
			respuestaMessage.setListSessiones(list);

			System.out.println("***************");
			System.out.println("** AE CREADO  **");
			System.out.println(quoteRequest);
			System.out.println("***************");

			return respuestaMessage;

		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		return null;
		
	}
	
}
