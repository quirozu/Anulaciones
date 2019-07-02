package co.bvc.com.orquestador;

public enum Path {
	
    PATH_DATADICTIONARY("resources\\datadictionary\\FIX44.xml"),
    PATH_LOGS("resources\\logs\\inicial"),
  
    
    PATH_CONFIG1_19("resources\\sessionSettings1_19.cfg"),
    PATH_CONFIG1_20("resources\\sessionSettings1_20.cfg"),
    PATH_CONFIG1_21("resources\\sessionSettings1_21.cfg"),
    PATH_CONFIG1_27("resources\\sessionSettings1_27.cfg"),
    PATH_CONFIG2_35("resources\\sessionSettings2_35.cfg"),
    PATH_CONFIG2_37("resources\\sessionSettings2_37.cfg");
	
    private String code;
    Path(String code) {
                   this.code = code;
    }
    public String getCode() {
                   return this.code;
    }


}
