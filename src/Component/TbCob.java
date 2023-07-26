package Component;

import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SUtil;

public class TbCob {
    public static final String COMPONENT = "tbcob";
    public static final String PK = "idcob";

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
            obj.put("data", Dhm.getAll(COMPONENT));
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

            JSONObject tbcob = new JSONObject();
            tbcob.put("idac", 0);
            tbcob.put("idcob", 116537);
            tbcob.put("cobidtg", 0);
            tbcob.put("cimpus", 0);
            tbcob.put("bord", 73);
            tbcob.put("idven", 0);
            tbcob.put("idcli", 0);
            tbcob.put("idctabco", 0);
            tbcob.put("cdoc", "VC-068662");
            tbcob.put("cbtc", 0);
            tbcob.put("cnrocon", "0");
            tbcob.put("cimp", 0);
            tbcob.put("cidcen", 0);
            tbcob.put("cest", 1);
            tbcob.put("cfec", "2023-07-19 00:00:00.0");
            tbcob.put("cpagadocon", 4);
            tbcob.put("sucreg", 0);
            tbcob.put("fecmod", "2023-07-19 02:22:38.0");
            tbcob.put("ctipodoc", 0);
            tbcob.put("cbmoneda", 0);
            tbcob.put("usumod", "BISMARK");
            tbcob.put("idiv", 0);
            tbcob.put("cobcambio", 0);
            tbcob.put("idemp", 41);
            tbcob.put("cdet", "Pago al contado Doc: VD-068302");
            tbcob.put("ctipo", 1);

            Dhm.registro(COMPONENT, PK, tbcob);
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

    public static void generarNotaEntrega(){
        //La nota de entrega es una venta que no ha sido entregada ni pagada, como una cotizacion.
        //Afecta a las siguientes tablas
        // tbvc - tbven - tbcob - tbsucesos - tbvd

        //Primer creamos la venta maestro
        

        //Ahora creamos el detalle de la venta
        
    }
}
