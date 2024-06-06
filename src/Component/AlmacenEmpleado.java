package Component;

import org.json.JSONArray;
import org.json.JSONObject;
import Servisofts.SPGConect;
import Servisofts.SUtil;
import Server.SSSAbstract.SSSessionAbstract;

public class AlmacenEmpleado {
    public static final String COMPONENT = "almacen_empleado";

    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        switch (obj.getString("type")) {
            case "getAll":
                getAll(obj, session);
                break;
            case "getByKey":
                getByKey(obj, session);
                break;
            case "registro":
                registro(obj, session);
                break;
            case "editar":
                editar(obj, session);
                break;
            case "save":
                save(obj, session);
                break;
        }
    }

    public static void getAll(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select get_all('" + COMPONENT + "') as json";
            JSONObject data = SPGConect.ejecutarConsultaObject(consulta);
            obj.put("data", data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getByKey(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select get_by_key('" + COMPONENT + "', '" + obj.getString("key") + "') as json";
            JSONObject data = SPGConect.ejecutarConsultaObject(consulta);
            obj.put("data", data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void save(JSONObject obj, SSSessionAbstract session) {
        try {
            // insertando datos nuevos

            if (obj.has("data") && !obj.isNull("data")) {

                switch(obj.getJSONObject("data").getString("sync_type")){
                    case "insert":{
                        obj.getJSONObject("data").put("key", SUtil.uuid());
                        obj.getJSONObject("data").put("fecha_on", SUtil.now());
                        obj.getJSONObject("data").put("estado", 1);

                        SPGConect.insertObject(COMPONENT, obj.getJSONObject("data"));
                        obj.put("estado", "exito");
                        obj.put("data", obj.getJSONObject("data"));
                        return;
                    }
                    case "update":{
                        SPGConect.editObject(COMPONENT, obj.getJSONObject("data"));
                        obj.getJSONObject("data").remove("sync_type");
                        obj.put("estado", "exito");
                        obj.put("data", obj.getJSONObject("data"));
                        return;
                    }
                    case "delete":{
                        SPGConect.editObject(COMPONENT, obj.getJSONObject("data").put("estado", 0));
                        obj.getJSONObject("data").remove("sync_type");
                        obj.put("estado", "exito");
                        obj.put("data", obj.getJSONObject("data"));
                        return;
                    }
                }
            }


            obj.put("estado", "error");
            obj.put("error", "No existe data");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void registro(JSONObject obj, SSSessionAbstract session) {
        try {
            JSONObject data = obj.getJSONObject("data");
            data.put("key", SUtil.uuid());
            data.put("estado", 1);
            data.put("fecha_on", SUtil.now());
            data.put("key_usuario", obj.getString("key_usuario"));
            SPGConect.insertArray(COMPONENT, new JSONArray().put(data));
            obj.put("data", data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void editar(JSONObject obj, SSSessionAbstract session) {
        try {
            JSONObject data = obj.getJSONObject("data");
            SPGConect.editObject(COMPONENT, data);
            obj.put("data", data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

}
