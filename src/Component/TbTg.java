package Component;

import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SUtil;

public class TbTg {
    public static final String COMPONENT = "tbtg";
    public static final String PK = "idtg";

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
            case "eliminar":
                eliminar(obj, session);
                break;
        }
    }

    public static void getAll(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select * from tbtg where tgest = 'DESPACHADO'";
            obj.put("data", Dhm.query(consulta));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getByKey(JSONObject obj, SSSessionAbstract session) {
        try {
            obj.put("data", Dhm.getByKey(COMPONENT, PK, obj.getString("key")));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void eliminar(JSONObject obj, SSSessionAbstract session) {
        try {
            Dhm.eliminar(COMPONENT, PK, obj.getString("key"));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void registro(JSONObject obj, SSSessionAbstract session) {
        try {
            JSONObject data = obj.getJSONObject("data");
            data.put("fecmod", SUtil.now());
            data.put("usumod", "Prueba");
            data.put("empest", 1);

            Dhm.registro(COMPONENT, PK, data);
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
            data.put("fecmod", SUtil.now());
            data.put("usumod", "Prueba");
            Dhm.editar(COMPONENT, PK, data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }
}
