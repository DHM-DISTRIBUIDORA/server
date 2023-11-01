package Component;

import org.json.JSONArray;
import org.json.JSONObject;

import Servisofts.SPGConect;
import Servisofts.SUtil;
import Server.SSSAbstract.SSSessionAbstract;

public class VisitaTransportista {
    public static final String COMPONENT = "visita_transportista";

    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        switch (obj.getString("type")) {
            case "getAll":
                getAll(obj, session);
                break;
            case "getReporteVisitas":
                getReporteVisitas(obj, session);
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
            case "uploadChanges":
                uploadChanges(obj, session);
                break;
            case "save":
                save(obj, session);
                break;
        }
    }

    public static void save(JSONObject obj, SSSessionAbstract session) {
        try {
            // insertando datos nuevos

            if (obj.has("data") && !obj.isNull("data")) {
                
                obj.getJSONObject("data").put("estado", 1);
                obj.getJSONObject("data").put("fecha_on", SUtil.now());
                SPGConect.insertObject(COMPONENT, obj.getJSONObject("data"));
                obj.getJSONObject("data").remove("sync_type");
                obj.put("estado", "exito");
                obj.put("data", obj.getJSONObject("data"));
                return;
            }
            obj.put("estado", "error");
            obj.put("error", "no existe data");
            
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }
    
    public static void uploadChanges(JSONObject obj, SSSessionAbstract session) {
        try {
            // insertando datos nuevos

            if (obj.has("insert") && !obj.isNull("insert") && obj.getJSONArray("insert").length() > 0) {
                for (int i = 0; i < obj.getJSONArray("insert").length(); i++) {
                    obj.getJSONArray("insert").getJSONObject(i).put("estado", 1);
                    obj.getJSONArray("insert").getJSONObject(i).put("fecha_on", SUtil.now());
                }
                SPGConect.insertArray(COMPONENT, obj.getJSONArray("insert"));
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
            String consulta = "select get_all('" + COMPONENT + "', 'idemp', '"+obj.get("idemp")+"') as json";
            if(obj.has("fecha")){
                consulta = "select get_reporte_visitas_transportisas('"+obj.get("fecha")+"', '"+obj.get("fecha")+"', '"+obj.get("idemp")+"' ) as json";
            }
            JSONObject data = SPGConect.ejecutarConsultaObject(consulta);
            obj.put("data", data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getReporteVisitas(JSONObject obj, SSSessionAbstract session) {
        try {
            
            String consulta = "select get_reporte_visitas_transportistas_array('"+obj.get("fecha_inicio")+"', '"+obj.get("fecha_fin")+"') as json";
            if(obj.has("idemp")){
                consulta = "select get_reporte_visitas_transportistas_array('"+obj.get("fecha_inicio")+"', '"+obj.get("fecha_fin")+"', '"+obj.get("idemp")+"' ) as json";
            }
            JSONArray data = SPGConect.ejecutarConsultaArray(consulta);
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

    public static JSONObject getVisitas(String idemp, String fecha_inicio, String fecha_fin) {
        try {
            String consulta = "select get_visitas_transportista('" + fecha_inicio + "', '" + fecha_fin + "', '"+ idemp +"') as json";
            return SPGConect.ejecutarConsultaObject(consulta);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
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
