package co.bvc.com.xstreaminet.amp;

import com.omxgroup.syssrv.Disposer;
import com.omxgroup.syssrv.Trace;
import com.omxgroup.xstream.amp.AmpTradeCancelApprove;
import com.omxgroup.xstream.amp.AmpTradeCancelReject;
import com.omxgroup.xstream.amp.AmpTradeId;
import com.omxgroup.xstream.amp.AmpTransReqChoice;
import com.omxgroup.xstream.api.Session;
import com.omxgroup.xstream.api.SessionFactory;
import com.omxgroup.xstream.api.TransReply;
import com.omxgroup.xstream.api.TransRequest;

public class TradeCancelAmp {

	/**
	 * Metodo para realizar la aprobacion de la anulación de una operación por un
	 * MarketController.
	 */
	/**
	 * @param user
	 * @param password
	 * @param tradeDate
	 * @param tradeNo
	 * @param tradeNoSuffix
	 */
	public static boolean tradeCancelApprove(String user, String password, String trMatchId) {

		boolean respuesta = false;

		// Convert trMatchId parameter in AMP parameters methods.
		long tradeDate = Long.parseLong(trMatchId.substring(0, 8));
		long tradeNo = Long.parseLong(trMatchId.substring(trMatchId.length() - 1));
		long tradeNoSuffix = 1;

		/// unset set this to enable trace
		Trace.setInstance(Trace.noTraceInstance());

		/// Create a disposer object to ensure objects are neatly cleaned up
		final Disposer disposer = new Disposer();

		try {
			Session session = disposer.disposes(SessionFactory.create_session("resources/config.ini"));

			if (session.logon(user, password)) {

				// create the transaction request choice object
				AmpTransReqChoice tc = new AmpTransReqChoice();

				// create the ampTradeId request.
				AmpTradeId ampTradeId = new AmpTradeId(tradeDate, tradeNo, tradeNoSuffix);
				AmpTradeCancelApprove ampTradeCancelApprove = new AmpTradeCancelApprove(ampTradeId);
				tc.setTradeCancelApprove(ampTradeCancelApprove);

				/// attach to the top level transaction request object
				TransRequest req = new TransRequest(tc);
				/// print the outbound message
				req.print();
				/// send the transaction waiting for the response
				TransReply rep = session.transaction(req);

				// validate if response is replyBad or replyOK
				if (rep.message().isReplyOK()) {
					// if reply is ok
					respuesta = true;
				} else {
					System.out.println("Mensaje de error: " + rep.message().getReplyBad().getInfoMessage());
					respuesta = false;
				}

				/// print out the response
				rep.print();
				/// example complete, lets just logoff
				session.logoff();
			}
		} catch (com.omxgroup.xstream.api.XStreamException e) {
			System.err.println("API Exception: " + e);
			e.printStackTrace(System.err);
		} catch (java.lang.Exception e) {
			System.err.println("Java Exception: " + e);
			e.printStackTrace(System.err);
		} finally {
			disposer.dispose();
		}
		return respuesta;
	}

	/**
	 * Metodo para realizar la aprobacion de la anulación de una operación por un
	 * MarketController.
	 */
	/**
	 * @param user
	 * @param password
	 * @param tradeDate
	 * @param tradeNo
	 * @param tradeNoSuffix
	 */
	public boolean tradeCancelReject(String user, String password, String trMatchId) {

		boolean respuesta = false;

		// Convert trMatchId parameter in AMP parameters methods.
		long tradeDate = Long.parseLong(trMatchId.substring(0, 8));
		long tradeNo = Long.parseLong(trMatchId.substring(trMatchId.length() - 1));
		long tradeNoSuffix = 1;

		/// unset set this to enable trace
		Trace.setInstance(Trace.noTraceInstance());

		/// Create a disposer object to ensure objects are neatly cleaned up
		final Disposer disposer = new Disposer();

		try {
			Session session = disposer.disposes(SessionFactory.create_session("resources/config.ini"));

			if (session.logon(user, password)) {

				// create the transaction request choice object
				AmpTransReqChoice tc = new AmpTransReqChoice();

				// create the ampTradeId request.
				AmpTradeId ampTradeId = new AmpTradeId(tradeDate, tradeNo, tradeNoSuffix);
				AmpTradeCancelReject ampTradeCancelReject = new AmpTradeCancelReject(ampTradeId);
				tc.setTradeCancelReject(ampTradeCancelReject);

				/// attach to the top level transaction request object
				TransRequest req = new TransRequest(tc);
				/// print the outbound message
				req.print();
				/// send the transaction waiting for the response
				TransReply rep = session.transaction(req);

				// validate if response is replyBad or replyOK
				if (rep.message().isReplyOK()) {
					// if reply is ok
					respuesta = true;
				} else {
					System.out.println("Mensaje de error: " + rep.message().getReplyBad().getInfoMessage());
					respuesta = false;
				}

				/// print out the response
				rep.print();

				/// example complete, lets just logoff
				session.logoff();
			}
		} catch (com.omxgroup.xstream.api.XStreamException e) {
			System.err.println("API Exception: " + e);
			e.printStackTrace(System.err);
		} catch (java.lang.Exception e) {
			System.err.println("Java Exception: " + e);
			e.printStackTrace(System.err);
		} finally {
			disposer.dispose();
		}
		return respuesta;
	}
	
}
	
//	
//	 public static void main(String argv[]) {
//
//		 TradeCancelAmp tradeCancelAmp = new TradeCancelAmp();
//		 if (tradeCancelAmp.tradeCancelApprove("su1", "", "201907120000000006")) {
//		 System.out.println("Ejecución Exitosa...");
//		 } else
//		 System.out.println("Ejecución Fallida...");
//		 }
//
//		 * los parámetros a enviar son:
//		  p1= "su1" - Constante por el momento. -- Usuario. Después lo debemos obtener de la BD - tabla AUT_USUARIO.
//		  p2= "" - vacío Constante por el momento. .  -- Password.  Después lo debemos obtener de la BD - tabla AUT_USUARIO.
//		  p3=  AE_TRMATCHID de la tabla aut_fix_tcr_datos. -- Nro. de Operación.

	

