package Component;

import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SUtil;

public class TbVd {
    public static final String COMPONENT = "tbvd";
    public static final String PK = "idvd";

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

            String consulta="select tbvd.* from tbvd";
            if(obj.has("idven")){
                consulta = "    select tbvd.* ";
                consulta += "        from tbvd ";
                consulta += "    where tbvd.idven =  "+obj.get("idven");
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

    public static JSONObject registroPedido(String idven, int idprd,double vdpre, double vdcan, String usumod, String vdunid, double tc) {
        try {
            JSONObject tbvd = new JSONObject();
            tbvd.put("vdsercanus", 0);
            tbvd.put("vdanutice", 0);
            tbvd.put("vddesppus", 0);
            tbvd.put("vdanucan", 0);
            tbvd.put("vddesprus", 0);
            tbvd.put("vdanuimp", 0);
            tbvd.put("vdpre", vdpre);
            tbvd.put("idse", 0);
            tbvd.put("vdcxu", 1);
            tbvd.put("vdpreconus", 0);
            tbvd.put("vdice", 0);
            tbvd.put("vdpreus", vdpre/tc);
            tbvd.put("vdtpreus", 0);
            tbvd.put("vdunid", vdunid);
            tbvd.put("fecmod", SUtil.now());
            tbvd.put("vddesc", 0);
            tbvd.put("vdidobj", 0);
            tbvd.put("vdest", 0);
            tbvd.put("vdserpreus", 0);
            tbvd.put("vddesotus", 0);
            //tbvd.put("idvd", 0);
            tbvd.put("usumod", usumod);
            tbvd.put("vdimpconus", 0);
            tbvd.put("idcentro", 0);
            tbvd.put("vddescus", 0);
            tbvd.put("vdiceus", 0);
            tbvd.put("vdimpus", 0.5747126405282259);
            tbvd.put("vdtpre", 2);
            tbvd.put("vdcan", vdcan);
            tbvd.put("vddespp", 0);
            tbvd.put("idprd", idprd);
            tbvd.put("idalm", 1);
            tbvd.put("idven", idven);
            tbvd.put("vddespr", 0);
            tbvd.put("vdpcos", 2.719473);
            tbvd.put("vdanuice", 0);
            tbvd.put("vdpcosus", 0.390728877168804);
            tbvd.put("vdicetus", 0);
            tbvd.put("vdanudes", 0);
            tbvd.put("vdpreofus", 0);
            tbvd.put("vddescaus", 0);
            tbvd.put("vdpreof", 4);
            tbvd.put("sucreg", 0);
            tbvd.put("vdtipo", 0);
            tbvd.put("vdimp", 4);
            tbvd.put("vddesca", 0);
            tbvd.put("vdnfila", 1);
            tbvd.put("vdunitip", 1);
            tbvd.put("vdicet", 0);
            tbvd.put("vddesot", 0);
            
            return tbvd;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
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

    public static void generarNotaEntrega(){
        //La nota de entrega es una venta que no ha sido entregada ni pagada, como una cotizacion.
        //Afecta a las siguientes tablas
        // tbvc - tbven - tbcob - tbsucesos - tbvd

        //Primer creamos la venta maestro
        

        //Ahora creamos el detalle de la venta
        JSONObject tbvd = new JSONObject();
        tbvd.put("vdsercanus", 0);
        tbvd.put("vdanutice", 0);
        tbvd.put("vddesppus", 0);
        tbvd.put("vdanucan", 0);
        tbvd.put("vddesprus", 0);
        tbvd.put("vdanuimp", 0);
        tbvd.put("vdpre", 4);
        tbvd.put("idse", 0);
        tbvd.put("vdcxu", 1);
        tbvd.put("vdpreconus", 0);
        tbvd.put("vdice", 0);
        tbvd.put("vdpreus", 0.5747126405282259);
        tbvd.put("vdtpreus", 0);
        tbvd.put("vdunid", "BOLSA");
        tbvd.put("fecmod", "2023-07-19 02:22:38.0");
        tbvd.put("vddesc", 0);
        tbvd.put("vdidobj", 0);
        tbvd.put("vdest", 0);
        tbvd.put("vdserpreus", 0);
        tbvd.put("vddesotus", 0);
        tbvd.put("idvd", 167848);
        tbvd.put("usumod", "BISMARK");
        tbvd.put("vdimpconus", 0);
        tbvd.put("idcentro", 0);
        tbvd.put("vddescus", 0);
        tbvd.put("vdiceus", 0);
        tbvd.put("vdimpus", 0.5747126405282259);
        tbvd.put("vdtpre", 2);
        tbvd.put("vdcan", 1);
        tbvd.put("vddespp", 0);
        tbvd.put("idprd", 349);
        tbvd.put("idalm", 1);
        tbvd.put("idven", 77648);
        tbvd.put("vddespr", 0);
        tbvd.put("vdpcos", 2.719473);
        tbvd.put("vdanuice", 0);
        tbvd.put("vdpcosus", 0.390728877168804);
        tbvd.put("vdicetus", 0);
        tbvd.put("vdanudes", 0);
        tbvd.put("vdpreofus", 0);
        tbvd.put("vddescaus", 0);
        tbvd.put("vdpreof", 4);
        tbvd.put("sucreg", 0);
        tbvd.put("vdtipo", 0);
        tbvd.put("vdimp", 4);
        tbvd.put("vddesca", 0);
        tbvd.put("vdnfila", 1);
        tbvd.put("vdunitip", 1);
        tbvd.put("vdicet", 0);
        tbvd.put("vddesot", 0);
    }
}
