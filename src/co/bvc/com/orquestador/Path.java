package co.bvc.com.orquestador;

public enum Path {
	
    PATH_DATADICTIONARY("resources\\datadictionary\\FIX44.xml"),
    PATH_LOGS("resources\\logs\\inicial"),
  
    
    PATH_CONFIG1_19("resources\\sessionSettings1_19.cfg"),
    PATH_CONFIG1_20("resources\\sessionSettings1_20.cfg"),
    PATH_CONFIG1_21("resources\\sessionSettings1_21.cfg"),
    PATH_CONFIG1_27("resources\\sessionSettings1_27.cfg"),
    PATH_CONFIG2_35("resources\\sessionSettings2_35.cfg"),
    PATH_CONFIG2_37("resources\\sessionSettings2_37.cfg"),
    PATH_CONFIG35_1("resources\\sessionSettings35_1.cfg"), 
    PATH_CONFIG71_1("resources\\sessionSettings71_1.cfg"),
    PATH_CONFIG73_1("resources\\sessionSettings73_1.cfg"),
    PATH_CONFIG29_17("resources\\sessionSettings29_17.cfg"),
    PATH_CONFIG37_18("resources\\sessionSettings37_18.cfg"),
    PATH_CONFIG45_17("resources\\sessionSettings45_17.cfg"),
    PATH_CONFIG51_17("resources\\sessionSettings51_17.cfg"),
    PATH_CONFIG72_1("resources\\sessionSettings72_1.cfg");
	
    private String code;
    Path(String code) {
                   this.code = code;
    }
    public String getCode() {
                   return this.code;
    }


}
