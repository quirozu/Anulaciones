package co.com.bvc.aut_tcr.basicfix;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;

import co.com.bvc.aut_tcr.orquestador.AutoEngine;
import quickfix.FieldNotFound;
import quickfix.SessionNotFound;

public class MainTCR {

	public static void main(String[] args) throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {
		System.out.println(" **********   ******  *******             ******   **      **   ****** "); 
		System.out.println("/////**///   **////**/**////**           /*////** /**     /**  **////**");
		System.out.println("    /**     **    // /**   /**           /*   /** /**     /** **    // ");
		System.out.println("    /**    /**       /*******     *****  /******  //**    ** /**       ");
		System.out.println("    /**    /**       /**///**    /////   /*//// ** //**  **  /**       ");
		System.out.println("    /**    //**    **/**  //**           /*    /**  //****   //**    **");
		System.out.println("    /**     //****** /**   //**          /*******    //**     //****** ");
		System.out.println("    //       //////  //     //           ///////      //       //////  \n\n");
		
		@SuppressWarnings("resource")
		Scanner reader = new Scanner(System. in);
		
		int escenarioInicial = 0;
		int escenarioFinal = 0;
				
		System.out.println("DIGITE PRIMER ESCENARIO A PROBAR...");
		escenarioInicial = reader.nextInt();
		System.out.println("DIGITE ULTIMO ESCENARIO A PROBAR...");
		escenarioFinal = reader.nextInt();
		
		if (escenarioFinal < escenarioInicial) {
			int aux = escenarioFinal;
			escenarioFinal = escenarioInicial;
			escenarioInicial = aux;
		}
			
		AutoEngine autoEngine = new AutoEngine();
		autoEngine.iniciarEjecucion(escenarioInicial, escenarioFinal);

	}
}
