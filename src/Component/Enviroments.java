package Component;

import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SPGConect;
import org.json.JSONObject;

public class Enviroments
{
    public static final String COMPONENT = "enviroments";

    public Enviroments(JSONObject obj, SSSessionAbstract session) {
        switch (obj.getString("type")) {
            case "getVersion":
                getVersion(obj);
                break;
            default:
                break;
        }
    }
    

    public void getVersion(JSONObject obj){
        try{
            String consulta =  "select get_by_key('"+COMPONENT+"','version') as json";
            JSONObject data = SPGConect.ejecutarConsultaObject(consulta);
            obj.put("data", data.getJSONObject("version").getString("data"));
            obj.put("estado", "exito");
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static String getVersion(){
        try{
            String consulta =  "select get_by_key('"+COMPONENT+"','version') as json";
            JSONObject data = SPGConect.ejecutarConsultaObject(consulta);
            return data.getJSONObject("version").getString("data");
        }catch(Exception e){
            e.printStackTrace();
            return "0";
        }
    }
}
