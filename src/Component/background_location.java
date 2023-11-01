package Component;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.json.JSONArray;
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
            case "uploadChanges":
                uploadChanges(obj, session);
                break;
            case "getByKey":
                getByKey(obj, session);
                break;
        }
    }

    public static void uploadChanges(JSONObject obj, SSSessionAbstract session) {
        try {
            // insertando datos nuevos

            if (obj.has("insert") && !obj.isNull("insert") && obj.getJSONArray("insert").length() > 0) {

                for (int i = 0; i < obj.getJSONArray("insert").length(); i++) {
                    
                    JSONObject location = SPGConect.ejecutarConsultaObject("select get_by('" + COMPONENT + "','key_usuario','"
                        + obj.getString("key_usuario") + "') as json");


                    if (location== null || !location.has("key")) {
                        location.put("key", SUtil.uuid());
                        location.put("fecha_on", obj.getJSONArray("insert").getJSONObject(i).get("fecha_on"));
                        location.put("estado", 1);
                        location.put("key_usuario", obj.getString("key_usuario"));
                        SPGConect.insertObject("background_location", location);
                    }
                    if (obj.getJSONArray("insert").getJSONObject(i).has("tipo")) {
                        JSONObject data = obj.getJSONArray("insert").getJSONObject(i);
                        String tipo = data.getString("tipo");
                        if (tipo.equals("start") || tipo.equals("stop")) {
                            location.put("tipo", tipo);
                            SPGConect.insertObject("location_info", new JSONObject(location.toString()).put("key", SUtil.uuid()).put("fecha_on", data.get("fecha_on")));
                        } else {
                            location.put("latitude", data.getDouble("latitude"));
                            location.put("longitude", data.getDouble("longitude"));
                            Date fecha = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(data.getString("fecha_on"));
                            GPX.saveGPX(obj.getString("key_usuario"), data.getDouble("latitude"), data.getDouble("longitude"), data.getDouble("rotation"), fecha);
                        }
                        location.put("fecha_last", SUtil.now());
                    }
                    SPGConect.editObject("background_location", location);

                }
              //  SPGConect.insertArray(COMPONENT, obj.getJSONArray("insert"));
            }

            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
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
