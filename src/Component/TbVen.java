package Component;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.JsonObject;

import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SUtil;

public class TbVen {
    public static final String COMPONENT = "tbven";
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
            case "getVenta":
                getVenta(obj, session);
                break;
            case "editar":
                editar(obj, session);
                break;
            case "eliminar":
                eliminar(obj, session);
                break;
            case "generarNotaEntrega":
                generarNotaEntrega(obj, session);
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

    public static void getVenta(JSONObject obj, SSSessionAbstract session) {
        try {

            JSONArray venta = Dhm.getByKey(COMPONENT, PK, obj.getString("idven"));

            String consulta = "select * from tbvd where idven = "+obj.getString("idven");
            JSONArray ventaDetalle =  Dhm.query(consulta);

            venta.getJSONObject(0).put("tvvd", ventaDetalle);

            obj.put("data", venta);
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

    public static JSONObject registro(int idcli, String vnit, String usumod, String vdet) throws Exception {
        
        //generando el vdoc
        JSONArray vdoc = Dhm.getMax("tbven", "vdoc", "where vtipo = 'VD'");
        String svdoc = vdoc.getJSONObject(0).getString("max");
        int ivdoc = Integer.parseInt(svdoc)+1;

        //generando el vnum
        JSONArray vnum = Dhm.getMax("tbven", "vnum", "where vtipo = 'VD'");
        int ivnum = vnum.getJSONObject(0).getInt("max")+1;



        //Buscando el cliente
        JSONObject tbcli = TbCli.getByKey(idcli+"");

        JSONObject tbven = new JSONObject();
        tbven.put("vmpimp", 0);
        tbven.put("vpint", 0);
        tbven.put("vmpid", 1);
        tbven.put("vanudesfin", 0);
        tbven.put("vdocid", 1);
        tbven.put("vcuoini", 0);
        //tbven.put("idven", 77648);
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
        tbven.put("vtc", 6.96);
        tbven.put("vnum", ivnum);
        tbven.put("vtipa", 0);
        tbven.put("vdet", vdet);
        tbven.put("vdesc", 0);
        tbven.put("sucreg", 0);
        tbven.put("vanumpimp", 0);
        tbven.put("fecmod", SUtil.now());
        tbven.put("vdoc", ivdoc+"");
        tbven.put("vncuo", 0);
        tbven.put("vfec", SUtil.now());
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

    public static void generarNotaEntrega(JSONObject obj, SSSessionAbstract session){
        //La nota de entrega es una venta que no ha sido entregada ni pagada, como una cotizacion.
        //Afecta a las siguientes tablas
        // tbvc - tbven - tbcob - tbsucesos - tbvd
        try{
            JSONObject data = obj.getJSONObject("data");

            //Primer creamos la venta maestro
            JSONObject tbVen = TbVen.registro(data.getInt("idcli"),data.getString("vnit"), obj.getString("usumod"), data.getString("vdet"));
            
            tbVen.put("productos", new JSONArray());
            //Ahora creamos el detalle de la venta

            JSONArray json = new JSONArray();
            JSONObject tbVd;
            for (int i = 0; i < data.getJSONArray("productos").length(); i++) {
                tbVd = data.getJSONArray("productos").getJSONObject(i);
                //Solo lo arma
                tbVd = TbVd.registro(tbVen.getInt("idven"), tbVd.getInt("idprd") ,tbVd.getDouble("vdpre"), tbVd.getDouble("vdcan"), obj.getString("usumod"), tbVd.getString("vdunid"));
                //tbVen.getJSONArray("productos").put(tbVd);
                json.put(tbVd);
            }

            Dhm.registroAll("tbVd", "idvd", json);
            //Agregamos el hitorico del evento
            TbSucesos.registro(tbVen.getInt("idven"), tbVen.get("vdoc")+"", obj.getString("usumod")); 

            obj.put("estado", "exito");
            obj.put("data", tbVen);
        }catch(Exception e){
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
        }
    }
}
