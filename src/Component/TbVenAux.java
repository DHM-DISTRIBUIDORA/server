package Component;

import org.json.JSONArray;
import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SUtil;

public class TbVenAux {
    public static final String COMPONENT = "tbvenaux";
    public static final String PK = "idven";

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
            String consulta = "select tbven.*, (select sum(tbvd) from tbvd where tbvd.idven = tbven.idven ) as monto from tbven";
            if (obj.has("idcli")) {
                consulta = "    select tbven.* ";
                consulta += "        from tbven ";
                consulta += "    where tbven.idcli =  " + obj.get("idcli");
            }

            if (obj.has("idemp")) {
                consulta = "    select tbven.* ";
                consulta += "        from tbven ";
                consulta += "    where tbven.idemp =  " + obj.get("idemp");
            }

            if (obj.has("idz")) {
                consulta = "    select tbven.* ";
                consulta += "        from tbven ";
                consulta += "    where tbven.vidzona =  " + obj.get("idz");
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

            Dhm.registro(COMPONENT, PK, data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static JSONObject registroPedido(int idcli, String vnit, String usumod, String vdet, double vtc)
            throws Exception {

        // generando el vdoc
        JSONArray vdoc = Dhm
                .query("select  MAX(CAST(tbven.vdoc AS INT)) as max  from tbven where tbven.vtipo in ('VD','VF')");
        // JSONArray vdoc = Dhm.getMax("tbven", "vdoc", "where vtipo = 'VD'");
        // String svdoc ;
        int ivdoc = vdoc.getJSONObject(0).getInt("max") + 1;

        // generando el vnum
        JSONArray vnum = Dhm.getMax("tbven", "vnum", "where vtipo in ('VD', 'VF')");
        int ivnum = vnum.getJSONObject(0).getInt("max") + 1;

        // Buscando el cliente
        JSONObject tbcli = TbCli.getByKey(idcli + "");

        JSONObject tbven = new JSONObject();
        tbven.put("vmpimp", 0);
        tbven.put("vpint", 0);
        tbven.put("vmpid", 1);
        tbven.put("vanudesfin", 0);
        tbven.put("vdocid", 1);
        tbven.put("vcuoini", 0);
        // tbven.put("idven", 77648);
        tbven.put("idcli", idcli);
        tbven.put("vnit", vnit);
        tbven.put("vemid", 0);
        tbven.put("vpla", 0);
        tbven.put("venest", 0);
        tbven.put("vnau", "0");
        tbven.put("vcli", tbcli.get("clinom"));
        tbven.put("vefa", "V");
        tbven.put("vtipp", 0);
        tbven.put("vtipo", "VD");
        tbven.put("vtog", 0);
        tbven.put("vmoneda", 0);
        tbven.put("vtc", vtc);
        tbven.put("vnum", ivnum);
        tbven.put("vtipa", 0);
        tbven.put("vdet", vdet);
        tbven.put("vdesc", 0);
        tbven.put("sucreg", 0);
        tbven.put("vanumpimp", 0);
        tbven.put("fecmod", SUtil.now());
        tbven.put("vdoc", "0" + ivdoc + "");
        tbven.put("vncuo", 0);
        tbven.put("vfec", SUtil.now().substring(0, 11)+"00:00:00.000");
        tbven.put("usumod", usumod);
        tbven.put("idemp", tbcli.get("cliidemp"));
        tbven.put("vcon", "0");
        tbven.put("vidzona", tbcli.get("idz"));

        Dhm.registro(COMPONENT, PK, tbven);
        return tbven;
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

    public static void generarNotaEntrega(){
        try{
            
        }catch(Exception e){

        }
    }
    
}
