package Component;

import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SUtil;

public class TbSucesos {
    public static final String COMPONENT = "tbsucesos";
    public static final String PK = "idscs";

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
            Dhm.registro(COMPONENT, PK, data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static JSONObject registroPedido(int idven, String scsnrodoc, String scsusuario) {
        try {
            JSONObject tbsucesos = new JSONObject();
            tbsucesos.put("scsdetalle", "Registro de ventas");
            tbsucesos.put("scsredpc", "SERVIDOR-SERVISOFTS");
            tbsucesos.put("scsventana", "App");
            tbsucesos.put("scsversion", "1.0.0");
            tbsucesos.put("scsaccion", "NUEVO");
            tbsucesos.put("scstipo", "S");
            tbsucesos.put("scshora", SUtil.now());
            tbsucesos.put("scsiddoc", idven);
            tbsucesos.put("scstabla", "tbven,tbvd,tbcob,tbvc");
            tbsucesos.put("scsusuario", scsusuario);
            tbsucesos.put("scsfechadoc", SUtil.now());
            //tbsucesos.put("idscs", 142424);
            tbsucesos.put("scsnrodoc", scsnrodoc);
            Dhm.registro(COMPONENT, PK, tbsucesos);
            return tbsucesos;
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
        
    }
}
