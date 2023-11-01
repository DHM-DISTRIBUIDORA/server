package Component;

import org.json.JSONArray;
import org.json.JSONObject;
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
            case "getPedidosDelivery":
                getPedidosDelivery(obj, session);
                break;
            case "eliminar":
                eliminar(obj, session);
                break;
            case "generarNotaEntrega":
                generarNotaEntrega(obj, session);
                break;
            case "getConductor":
                getConductor(obj, session);
                break;
        }
    }

    public static void getPedidosDelivery(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select tbven.*\n" + //
            "from tbven,\n" + //
            "tbtg\n" + //
            "where tbven.idtg = tbtg.idtg\n" + //
            "and tbven.idcli = "+obj.get("idcli")+"\n" + //
            "and tbtg.tgest = 'DESPACHADO'\n" ;
           
            JSONArray tbven = Dhm.query(consulta);
            
            obj.put("data", tbven);
            
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
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

    public static void getVenta(JSONObject obj, SSSessionAbstract session) {
        try {


            JSONArray venta;
            if(obj.has("idtg") && !obj.isNull("idtg")){
                venta = Dhm.getByKey(COMPONENT, "idtg", obj.get("idtg") + "");
            }else{
                venta = Dhm.getByKey(COMPONENT, PK, obj.get("idven") + "");
                String consulta = "select * from tbvd where idven = " + venta.getJSONObject(0).get("idven");
                JSONArray ventaDetalle = Dhm.query(consulta);

                venta.getJSONObject(0).put("tvvd", ventaDetalle);
            }

            obj.put("data", venta);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getConductor(JSONObject obj, SSSessionAbstract session) {
        try {

            String consulta = "select tbemp.* "+
            "from tbven, tbtg, tbemp "+
            "where tbven.idven = " + obj.get("idven")+" "+
            "and tbven.idtg = tbtg.idtg\n" + //
            "and tbemp.idemp = tbtg.idemp\n" + //
            "and tbtg.tgest = 'DESPACHADO'\n" ; //

            obj.put("data", Dhm.query(consulta));
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
    
    public static void generarNotaEntrega(JSONObject obj, SSSessionAbstract session) {
        // La nota de entrega es una venta que no ha sido entregada ni pagada, como una
        // cotizacion.
        // Afecta a las siguientes tablas
        // tbvc - tbven - tbcob - tbsucesos - tbvd
        try {

            //double tc = tbTC.getTipoCambioDolares();
            double tc = 1;

            JSONObject data = obj.getJSONObject("data");

            // Primer creamos la venta maestro
            JSONObject tbVen = TbVen.registroPedido(data.getInt("idcli"), data.getString("vnit"),
                    obj.getString("usumod"), data.getString("vdet"), tc);

            tbVen.put("productos", new JSONArray());
            // Ahora creamos el detalle de la venta

            double vcimp = 0;

            JSONArray json = new JSONArray();
            JSONObject tbVd;
            for (int i = 0; i < data.getJSONArray("productos").length(); i++) {
                tbVd = data.getJSONArray("productos").getJSONObject(i);
                // Solo lo arma
                tbVd = TbVd.registroPedido(tbVen.get("idven") + "", tbVd.getInt("idprd"), tbVd.getDouble("vdpre"),
                        tbVd.getDouble("vdcan"), obj.getString("usumod"), tbVd.getString("vdunid"), tc);
                // tbVen.getJSONArray("productos").put(tbVd);
                vcimp += tbVd.getDouble("vdcan") * tbVd.getDouble("vdpre");
                tbVd.put("vdimp", tbVd.getDouble("vdcan") * tbVd.getDouble("vdpre"));
                json.put(tbVd);
            }

            final double fvcimp = vcimp;

            Dhm.registroAll("tbVd", "idvd", json);

            final Thread thread_ = new Thread() {
                public void run() {
                    System.out.println("Thread Running");

                    JSONObject tbCob = TbCob.registroPedido(tbVen.getString("vdoc"), obj.getString("usumod"),
                            tbVen.getInt("idemp"));

                    double vcimpus = fvcimp / tc;

                    TbVc.registroPedido(fvcimp, vcimpus, tbVen.getInt("idven"), tbCob.getInt("idcob"),
                            tbVen.getString("vdoc"), obj.getString("usumod"));

                    // Agregamos el hitorico del evento
                    TbSucesos.registroPedido(tbVen.getInt("idven"), tbVen.get("vdoc") + "", obj.getString("usumod"));
                }
            };

            thread_.start();

            obj.put("estado", "exito");
            obj.put("data", tbVen);
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
        }
    }
}
