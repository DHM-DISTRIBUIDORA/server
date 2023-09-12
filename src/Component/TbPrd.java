package Component;

import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SUtil;

public class TbPrd {
    public static final String COMPONENT = "tbprd";
    public static final String PK = "idprd";

    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        switch (obj.getString("type")) {
            case "getAll":
                getAll(obj, session);
                break;
                case "getAllSimple":
                getAllSimple(obj, session);
                break;
            case "getByKey":
                getByKey(obj, session);
                break;
                case "getProductosVendidos":
                getProductosVendidos(obj, session);
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
            obj.put("data", Dhm.getAll(COMPONENT));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getProductosVendidos(JSONObject obj, SSSessionAbstract session) {
        try {
            obj.put("data", Dhm.query("select sum(tbvd.vdcan) as cantidad,\n" + //
                    "tbprd.prdnom,\n" + //
                    "tbvd.idprd,\n" + //
                    "tbprd.prdcod,\n" + //
                    "sum(tbvd.vdpre) as monto\n" +
                    "from tbven,\n" + //
                    "tbvd,\n" + //
                    "tbprd\n" + //
                    "where vtipo in ('VF', 'VD')\n" + //
                    "and tbvd.idven = tbven.idven\n" + //
                    "and tbven.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" + //
                    "and tbprd.idprd = tbvd.idprd\n" + //
                    "group by tbprd.prdnom,\n" + //
                    "tbvd.idprd, tbprd.prdcod")); 
            
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getAllSimple(JSONObject obj, SSSessionAbstract session) {

        String almacen = "";
        if(obj.has("idalm") && !obj.isNull("idalm")){
            almacen = " where idalm = "+obj.get("idalm")+" ";
        }

        try {
            String consulta = "SELECT tbprd.prdcod, "+
            "tbprd.prduxcdes, "+
            "tbprd.idlinea, "+
            "tbprd.prdcxu, "+
            "tbprd.prduxd, "+
            "tbprd.prdcor, "+
            "tbprd.idprd, "+
            "tbprd.prdunid, "+
            "tbprd.prdpoficial, "+
            "tbprd.prdnom, "+
            "coalesce(sq1.stock, 0 ) as stock "+
            "FROM "+
            "tbprd  JOIN ( "+
            "SELECT  tbprd.idprd, "+
            "compras.cantidad-ventas.cantidad as stock "+
            "FROM  "+
            "( "+
            "select SUM(tbvd.vdcan) as cantidad, "+
            "tbvd.idprd "+
            "from tbvd JOIN tbven on tbvd.idven = tbven.idven "+
            ""+almacen+
            "group by tbvd.idprd "+
            ") ventas, "+
            "( "+
            "select SUM(tbcd.cdcan) as cantidad, "+
            "tbcd.idprd "+
            "from tbcd  "+
            " "+almacen+" "+
            "group by tbcd.idprd "+
            ") compras, "+
            "tbprd "+
            "where tbprd.idprd = ventas.idprd "+
            "and tbprd.idprd = compras.idprd "+
            ") sq1 ON tbprd.idprd = sq1.idprd ";

            obj.put("data", Dhm.query(consulta));
            obj.put("estado", "exito");

        }catch(Exception e){
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
