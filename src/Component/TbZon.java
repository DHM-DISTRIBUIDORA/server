package Component;

import org.json.JSONArray;
import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SPGConect;
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
            case "uploadChanges":
                uploadChanges(obj, session);
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

    public static void uploadChanges(JSONObject obj, SSSessionAbstract session) {
        try {
            //insertando datos nuevos
            
            if(obj.has("insert") && !obj.isNull("insert") && obj.getJSONArray("insert").length()>0){
                JSONArray arr = Dhm.query("select max("+PK+") as id from "+COMPONENT);
                int id = arr.getJSONObject(0).getInt("id");
                for (int i = 0; i < obj.getJSONArray("insert").length(); i++) {
                    id++;
                    obj.getJSONArray("insert").getJSONObject(i).put(PK, id);
                }
                Dhm.registroAll("dm_cabfac", "", obj.getJSONArray("insert"));
            }


            // Editar
            if(obj.has("update") && !obj.isNull("update") && obj.getJSONArray("update").length()>0){
                JSONObject tbzon;
                String editarClientes = "";
                for (int i = 0; i < obj.getJSONArray("update").length(); i++) {

                    // Verificamos si cambia el idemp para actualizar todos los clientes de la zona al nuevo empleado
                    tbzon = getByKey(obj.getJSONArray("update").getJSONObject(i).get("idz")+"");
                    if(tbzon!=null){
                        if(!tbzon.get("idemp").equals(obj.getJSONArray("update").getJSONObject(i).get("idemp"))){
                            editarClientes+= "update tbcli set fecmod='"+SUtil.now().replaceAll("T", " ").split("\\.")[0]+"', cliidemp = "+obj.getJSONArray("update").getJSONObject(i).get("idemp")+" where cliidemp = "+tbzon.get("idemp")+"; ";
                        }
                    }
                }
                if(editarClientes.length()>0){
                    Dhm.query(editarClientes);
                }
                
                Dhm.editarAll(COMPONENT, PK, obj.getJSONArray("update"));
            }


            // Eliminar
            if(obj.has("delete") && !obj.isNull("delete") && obj.getJSONArray("delete").length()>0){
                JSONArray del_ = new JSONArray();
                for (int i = 0; i < obj.getJSONArray("delete").length(); i++) {
                    del_.put(obj.getJSONArray("delete").getJSONObject(i).get(PK));
                }
                Dhm.eliminarAll(COMPONENT, PK, del_);
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
    public static JSONObject getByKey(String key) {
        try {
             return Dhm.getByKey(COMPONENT, PK, key).getJSONObject(0);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
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
