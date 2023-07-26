package Component;

import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SUtil;

public class TbVc {
    public static final String COMPONENT = "tbvc";
    public static final String PK = "idvc";

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

            JSONObject tbvc = new JSONObject();
            tbvc.put("vcidcen", 0);
            tbvc.put("idog", 0);
            tbvc.put("vctipo", 0);
            tbvc.put("idpl", 0);
            tbvc.put("vctc", 1);
            tbvc.put("vcdet", "Pago al contado Doc: VD-068302");
            tbvc.put("idcob", 116537);
            tbvc.put("vcidcta", 0);
            tbvc.put("sucreg", 0);
            tbvc.put("idven", 77648);
            tbvc.put("vcimp", 4);
            tbvc.put("fecmod", "2023-07-19 02:22:38.0");
            tbvc.put("vcidg", 0);
            tbvc.put("vcest", 1);
            tbvc.put("vcimpus", 0.5747126405282259);
            tbvc.put("vcidiv", 0);
            tbvc.put("idvc", 117044);
            tbvc.put("usumod", "BISMARK");
            tbvc.put("idcmp", 0);

            Dhm.registro(COMPONENT, PK, tbvc);
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
