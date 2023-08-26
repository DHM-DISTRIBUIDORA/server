package Component;

import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SConsole;
import Servisofts.SPGConect;
import Servisofts.SUtil;
import util.GPX;

public class background_location {
    public static final String COMPONENT = "background_location";

    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        switch (obj.getString("type")) {
            case "onChange":
                onChange(obj, session);
                break;
            case "getAll":
                getAll(obj, session);
                break;
            case "getByKey":
                getByKey(obj, session);
                break;
        }
    }

    public static void getAll(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select get_all('"+COMPONENT+"') as json";
            if(obj.has("key_usuario"))
                consulta = "select get_all('"+COMPONENT+"', 'key_usuario', '"+obj.getString("key_usuario")+"') as json";

            JSONObject data = SPGConect.ejecutarConsultaObject(consulta);
            obj.put("data", data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            e.printStackTrace();
        }
    }

    public static void getByKey(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select get_by('"+COMPONENT+"', 'key_usuario', '"+obj.getString("key_usuario")+"') as json";

            JSONObject data = SPGConect.ejecutarConsultaObject(consulta);
            obj.put("data", data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            e.printStackTrace();
        }
    }

    public static void onChange(JSONObject obj, SSSessionAbstract session) {
        // SConsole.log(obj.toString());

        try {
            JSONObject location = SPGConect.ejecutarConsultaObject("select get_by('" + COMPONENT + "','key_usuario','"
                    + obj.getString("key_usuario") + "') as json");

            if (!location.has("key")) {
                location.put("key", SUtil.uuid());
                location.put("fecha_on", SUtil.now());
                location.put("estado", 1);
                location.put("key_usuario", obj.getString("key_usuario"));
                SPGConect.insertObject("background_location", location);
            }
            if (obj.has("tipo")) {
                String tipo = obj.getString("tipo");
                if (tipo.equals("start") || tipo.equals("stop")) {
                    location.put("tipo", tipo);
                } else {
                    JSONObject data = obj.getJSONObject("data");
                    location.put("latitude", data.getDouble("latitude"));
                    location.put("longitude", data.getDouble("longitude"));
                    GPX.saveGPX(obj.getString("key_usuario"), data.getDouble("latitude"), data.getDouble("longitude"),
                            data.getDouble("rotation"));
                }
                location.put("fecha_last", SUtil.now());
            }
            SPGConect.editObject("background_location", location);
            obj.put("estado", "exito");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
