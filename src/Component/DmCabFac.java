package Component;

import org.json.JSONArray;
import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SUtil;

public class DmCabFac {
    public static final String COMPONENT = "dm_cabfac";
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
            case "getPedido":
                getPedido(obj, session);
                break;
            case "getPedidos":
                getPedidos(obj, session);
                break;
            case "editar":
                editar(obj, session);
                break;
            case "eliminar":
                eliminar(obj, session);
                break;
        }
    }

    public static void getPedido(JSONObject obj, SSSessionAbstract session) {
        try {

            JSONArray venta = Dhm.getByKey(COMPONENT, PK, obj.get("idven") + "");

            String consulta = "select * from dm_detfac where idven = " + obj.get("idven");
            JSONArray dm_detfac = Dhm.query(consulta);

            venta.getJSONObject(0).put("dm_detfac", dm_detfac);

            obj.put("data", venta);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static JSONObject agregarVenta(JSONArray ventas, JSONArray detalle){
        JSONObject ventas_ = new JSONObject();
        JSONObject venta;
        for (int i = 0; i < ventas.length(); i++) {
            venta = ventas.getJSONObject(i);

            for (int j = 0; j < detalle.length(); j++) {
                if(detalle.getJSONObject(j).get("idven").equals(venta.get("idven"))){
                    if(!venta.has("detalle")){
                        venta.put("detalle", new JSONArray().put(detalle.getJSONObject(j)));  
                        detalle.remove(j);
                        j--;
                        continue;
                    }
                    venta.getJSONArray("detalle").put(detalle.getJSONObject(j));  
                    detalle.remove(j);
                    j--;
                }
            }

            ventas_.put(venta.get("idven")+"", venta);
        }
        return ventas_;
    }

    public static void getPedidos(JSONObject obj, SSSessionAbstract session) {
        try {

            String consulta = "";
            
            if(obj.has("idcli") && !obj.isNull("idcli")){
                consulta = "select dm_cabfac.*\n" + //
                    "from dm_cabfac,\n" + //
                    "tbcli\n" + //
                    "where tbcli.clicod = dm_cabfac.clicod\n" + //
                    "and tbcli.idcli = "+obj.get("idcli")+"\n"+ 
                    "order by idven desc";
            }
            
            if(obj.has("idemp") && !obj.isNull("idemp")){
                consulta = "select dm_cabfac.*\n" + //
                    "from dm_cabfac,\n" + //
                    "tbemp\n" + //
                    "where tbemp.empcod = dm_cabfac.codvendedor\n" + //
                    "and tbemp.idemp = "+obj.get("idemp")+"\n"+ 
                    "order by idven desc";
            }

            JSONArray dm_cabfac = Dhm.query(consulta);

            String idVentas = "";
            for (int i = 0; i < dm_cabfac.length(); i++) {
                idVentas+=dm_cabfac.getJSONObject(i).get("idven")+",";
            }
            idVentas=idVentas.length()>0?idVentas.substring(0, idVentas.length()-1):"";


            consulta = "select * from dm_detfac where idven in ("+idVentas+") order by idven desc";
            JSONArray dm_detfac = Dhm.query(consulta);

            //buscador detalle 
            JSONObject ventas = agregarVenta(dm_cabfac, dm_detfac);
            
        

            //venta.getJSONObject(0).put("dm_detfac", dm_detfac);

            obj.put("data", ventas);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
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
            double tc = 1;

            
            JSONArray arr = Dhm.query("select max(idven) as idven from dm_cabfac");
            int idven = arr.getJSONObject(0).getInt("idven")+1;

            String fecha_hora = SUtil.now();
            String  hora = fecha_hora.substring(11, 11+8);
            String fecha = fecha_hora.substring(0, 10);

            String consulta = " SELECT \n" + //
                    "\ttbemp.empcod,\n" + //
                    "\ttbzon.zcod,\n" + //
                    "\ttbcli.idcli, \n" + //
                    "\ttbcli.clidir, \n" + //
                    "\ttbcli.clicod,\n" + //
                    "\ttbcli.clinom,\n" + //
                    "\ttbcli.clinit,\n" + //
                    "\tcase when tbcat.catnom is null then \'Tienda de Barrio\'  else tbcat.catnom  end as catnom,\n" + //
                    "\ttbcli.clirazon,\n" + //
                    "\ttbcli.clitel,\n" + //
                    "\ttbcli.clidir\n" + //
                    "FROM tbcli \n" + //
                    "LEFT JOIN tbzon ON tbcli.idz = tbzon.idz\n" + //
                    "LEFT JOIN tbemp ON tbcli.cliidemp = tbemp.idemp\n" + //
                    "LEFT JOIN tbcat ON tbcli.idcat = tbcat.idcat\n" + //
                    "WHERE \n" + //
                    "\ttbcli.idcli = "+data.get("idcli");

            JSONArray dataPedidoJson = Dhm.query(consulta);
            JSONObject dataPedido = dataPedidoJson.getJSONObject(0);

            
            consulta = "SET DATEFORMAT 'YMD'; insert into dm_cabfac \n";

            consulta += "(vlongitud,vhora,vlatitud,direccion,vtipa,vzona,clicod,vdes,idven,codvendedor,razonsocial,vpla,nit,tipocliente,vfec,telefonos,vobs,nombrecliente,vtipo)";
            consulta += "values ";
            consulta += "(0, '1900-01-01 "+hora+"',0,'"+dataPedido.get("clidir")+"',0,'"+dataPedido.get("zcod")+"','"+dataPedido.get("clicod")+"',0,"+idven+",'"+dataPedido.get("empcod")+"','"+dataPedido.get("clirazon")+"',0,'"+dataPedido.get("clinit")+"','"+dataPedido.get("catnom")+"','"+fecha+" 00:00:00.0','"+dataPedido.get("clitel")+"','"+data.get("vdet")+"','"+dataPedido.get("clinom")+"',1);";


            JSONObject tbVd;
            for (int i = 0; i < data.getJSONArray("productos").length(); i++) {
                tbVd = data.getJSONArray("productos").getJSONObject(i);
                // Solo lo arma
                consulta += "insert into dm_detfac (idven,prdcod,vddesc,vdpre,vdcan) values ("+idven+",'"+tbVd.get("prdcod")+"',0,"+tbVd.getDouble("vdpre")+","+tbVd.getInt("vdcan")+"); ";    
                //consulta += "insert into dm_detfac (idven,prdcod,vddesc,vdpre,vdcan) values ("+idven+",'ITA-008',0,35,6)";
            }


            Dhm.query(consulta);

            obj.getJSONObject("data").put("idven", idven);
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
            Dhm.editar(COMPONENT, PK, data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }
}
