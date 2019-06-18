package co.bvc.com.basicfix;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedList;

import javax.sound.midi.Soundbank;

public class ParametersRead {
	 File archivo = null;
     FileReader fr = null;
     BufferedReader br = null;
     String[] lin;
     String[] Parametros;
    		 
   public String[] leerConexion() {
	   try {
        archivo = new File ("resources\\conexion.info");
        fr = new FileReader (archivo);
        br = new BufferedReader(fr);
        String linea;
        while((linea=br.readLine())!=null) 
        	lin = linea.split(",");
     }
     catch(Exception e){
        e.printStackTrace();
     }finally{
        try{                    
           if( null != fr ){   
              fr.close();     
           }                  
        }catch (Exception e2){ 
           e2.printStackTrace();
        }
     }
	return lin;
     }
   
   public String leerConsultas() {
	   try {
        archivo = new File ("resources\\consulta.info");
        fr = new FileReader (archivo);
        br = new BufferedReader(fr);
        String linea;
        while((linea=br.readLine())!=null) 
        return linea;
     }
     catch(Exception e){
        e.printStackTrace();
     }finally{
        try{                    
           if( null != fr ){   
              fr.close();     
           }                  
        }catch (Exception e2){ 
           e2.printStackTrace();
        }
     }
	return null;

	
     }

}
