package Component;

import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SUtil;

public class TbZon {
    public static final String COMPONENT = "tbzon";
    public static final String PK = "idz";

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
            String consulta = "select tbzon.*, ";
            consulta += "(";
            consulta += "    select count(tbven.idven) ";
            consulta += "        from tbven ";
            consulta += "    where tbven.vidzona = tbzon.idz ";
            consulta += "    and tbven.vtipo in ('VD', 'VF') ";
            consulta += "    and tbven.vefa not in ('A')  ";
            consulta += "    and tbven.idtg is null  ";
            consulta += ") as pedidos, ";
            consulta += "( ";
            consulta += "    select count(tbven.idven) ";
            consulta += "        from tbven ";
            consulta += "    where tbven.vidzona = tbzon.idz ";
            consulta += "    and tbven.vtipo in ('VD', 'VF') ";
            consulta += "    and tbven.vefa not in ('A')  ";
            consulta += "    and tbven.idtg is not null  ";
            consulta += ") as ventas ";
            consulta +="from tbzon ";

            if(obj.has("idemp") && !obj.isNull("idemp")){
                consulta += "where idemp = "+obj.get("idemp");
            }
            
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
            data.put("zfecmod", SUtil.now());
            data.put("usumod", "Prueba");

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
            data.put("zfecmod", SUtil.now());
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
