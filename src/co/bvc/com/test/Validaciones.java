package co.bvc.com.test;

import java.util.ArrayList;
import co.bvc.com.basicfix.DataAccess;

public class Validaciones {


	DataAccess data = new DataAccess();

	public ArrayList<String> FragmentarCadena(String cadena) {
		ArrayList<String> claveValor = new ArrayList<String>();
		for (int i = 0; i < cadena.split("").length; i++) {
			claveValor.add(cadena.split("")[i]);
//                    System.out.println(claveValor.get(i));
		}
		return claveValor;
	}

	public ArrayList<String> FragmentarCadena1(String cadena) {
		ArrayList<String> claveValor1 = new ArrayList<String>();
		for (int i = 0; i < cadena.split("").length; i++) {
			claveValor1.add(cadena.split("")[i]);
//                    System.out.println(claveValor.get(i));
		}
		return claveValor1;
	}
	
}
