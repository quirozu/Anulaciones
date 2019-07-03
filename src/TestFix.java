
import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;
import co.bvc.com.orquestador.AutoEngine;
import quickfix.FieldNotFound;
import quickfix.SessionNotFound;

public class TestFix {

	public static void main(String[] args) throws SQLException, SessionNotFound, InterruptedException, IOException, FieldNotFound {
		int escenarioEjecucion = 0;
		int escenarioFinal = 0;
		System.out.println("DIGITE ESCENARIO A PROBAR...");
		@SuppressWarnings("resource")
		Scanner reader = new Scanner(System. in);
		escenarioEjecucion = reader.nextInt();
		System.out.println("DIGITE ESCENARIO HASTA DONDE DESEA PROBAR PROBAR...");
		@SuppressWarnings("resource")
		Scanner reade = new Scanner(System. in);
		escenarioFinal = reade.nextInt();
		
		
		AutoEngine autoEngine = new AutoEngine();
		autoEngine.iniciarEjecucion(escenarioEjecucion, escenarioFinal);	

	}
}
