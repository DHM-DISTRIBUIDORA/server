package Component;

import org.json.JSONArray;
import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SConfig;
import Servisofts.SUtil;

public class TbCli {
    public static final String COMPONENT = "tbcli";
    public static final String PK = "idcli";

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
            case "getPerfil":
                getPerfil(obj, session);
                break;
            case "getAllPedidos":
                getAllPedidos(obj, session);
                break;
            case "getSinPedidos":
                getSinPedidos(obj, session);
                break;
        }
    }


    public static void getAllPedidos(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select \n" + //
                    "sum(tbvd.vdcan) as cantidad,\n" + //
                    "sum(tbvd.vdpre) as monto,\n" + //
                    "tbcli.idcli,\n" + //
                    "tbcli.clidir,\n" +
                    "tbcli.clicod,\n" +
                    "tbcli.clinom,\n" + //
                    "tbcli.clilat,\n" + //
                    "tbcli.clilon\n" + //
                    "from tbven,\n" + //
                    "tbvd,\n" + //
                    "tbcli\n" + //
                    "where tbven.vtipo = 'VD'\n" + //
                    "and tbvd.idven = tbven.idven\n" + //
                    "and tbvd.vdcan > 0\n" + //
                    "and tbcli.idcli = tbven.idcli\n" + //
                    "and tbven.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" ; //
                    if(obj.has("idemp")){
                        consulta += "and tbcli.cliidemp = "+obj.get("idemp")+"\n"; //    
                    }
                    consulta += "group by \n" + //
                    "tbcli.clinom,\n" + //
                    "tbcli.clilat,\n" + //
                    "tbcli.idcli,\n" + //
                    "tbcli.clidir,\n" +
                    "tbcli.clicod,\n" +
                    "tbcli.clilon";

            obj.put("data", Dhm.query(consulta));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getSinPedidos(JSONObject obj, SSSessionAbstract session) {
        try {
            obj.put("data", Dhm.query("select tbcli.idcli,\n" + //
                    "tbcli.clicod,\n" + //
                    "tbcli.clinom,\n" + //
                    "tbcli.clidir,\n" + //
                    "tbcli.clilat,\n" + //
                    "tbcli.clilon\n" + //
                    "from tbcli\n" + //
                    "where tbcli.cliidemp = 52\n" + //
                    "and tbcli.idcli not in (\n" + //
                    "\tselect \n" + //
                    "tbcli.idcli\n" + //
                    "from tbven,\n" + //
                    "tbvd,\n" + //
                    "tbcli\n" + //
                    "where tbven.vtipo = 'VD'\n" + //
                    "and tbvd.idven = tbven.idven\n" + //
                    "and tbvd.vdcan > 0\n" + //
                    "and tbcli.idcli = tbven.idcli\n" + //
                    "and tbven.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n"+ //
                    "and tbcli.cliidemp = "+obj.get("idemp")+"\n" + //
                    "group by \n" + //
                    "tbcli.idcli\n" + //
                    ")"));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }
    public static void getAll(JSONObject obj, SSSessionAbstract session) {
        try {
            
            if(obj.has("cliidemp")){
                String consulta = "select tbcli.*, ";
                consulta += "(";
                consulta += "    select count(tbven.idven) ";
                consulta += "        from tbven ";
                consulta += "    where tbven.idcli = tbcli.idcli ";
                consulta += "    and tbven.vtipo in ('VD') ";
                consulta += ") as pedidos, ";
                consulta += "( ";
                consulta += "    select count(tbven.idven) ";
                consulta += "        from tbven ";
                consulta += "    where tbven.idcli = tbcli.idcli ";
                consulta += "    and tbven.vtipo in ('VF') ";
                consulta += ") as ventas ";
                consulta +="from tbcli where cliidemp = "+obj.get("cliidemp");
                

                obj.put("data", Dhm.query(consulta));
            }else if(obj.has("idz")){
                String consulta = "select tbcli.*, ";
                consulta += "(";
                consulta += "    select count(tbven.idven) ";
                consulta += "        from tbven ";
                consulta += "    where tbven.idcli = tbcli.idcli ";
                consulta += "    and tbven.vtipo in ('VD') ";
                consulta += ") as pedidos, ";
                consulta += "( ";
                consulta += "    select count(tbven.idven) ";
                consulta += "        from tbven ";
                consulta += "    where tbven.idcli = tbcli.idcli ";
                consulta += "    and tbven.vtipo in ('VF') ";
                consulta += ") as ventas ";
                consulta += "from tbcli where idz = "+obj.get("idz");
                obj.put("data", Dhm.query(consulta));
            }
            else{
                obj.put("data", Dhm.getAll(COMPONENT));
            }

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
    
    public static void getByKeyEmpleado(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select * from tbcli where cliidemp = "+obj.get("key");
            obj.put("data", Dhm.query(consulta));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static JSONObject getByKey(String key) {
        try {
            JSONArray json = Dhm.getByKey(COMPONENT, PK, key);
            if(json.length()>0){
                return json.getJSONObject(0);
            }
            return null;
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
    public static void getPerfil(JSONObject obj, SSSessionAbstract session) {
        try {
            String url = SConfig.getJSON("sqlServerApi").getString("url")+"api/select";
            String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");


            String consulta = ""+

            "select tbcli.idcli, "+
            "( "+
            "    select count(tbven.idven) "+
            "    from tbven "+
            "    where tbven.idcli = tbcli.idcli "+
            "    and tbven.vtipo in ('VD') "+
            ") as  cantidad_pedidos, "+
            "( "+
            "    select count(tbven.idven) "+
            "    from tbven "+
            "    where tbven.idcli = tbcli.idcli "+
            "    and tbven.vtipo in ('VF') "+
            ") as  cantidad_ventas, "+
            "( "+
            "select top 1 ventas.monto "+
            "from ( "+
            "    select sum(tbvd.vdpreus*tbvd.vdcan) as monto "+
//            "    tbven.idven "+
            "    from tbven,         "+
            "    tbvd         "+
            "    where tbven.idcli = tbcli.idcli "+
            "    and tbven.vtipo in ('VF') "+
            "    and tbvd.idven =  tbven.idven  "+
            "    group by tbven.idven "+
            ") ventas  "+
            "order by ventas.monto desc "+
            ") as  maxima_venta, "+
            "( "+
            "select top 1 ventas.monto "+
            "from ( "+
            "    select sum(tbvd.vdpreus*tbvd.vdcan) as monto "+
//            "    tbven.idven "+
            "    from tbven,         "+
            "    tbvd         "+
            "    where tbven.idcli = tbcli.idcli "+
            "    and tbven.vtipo in ('VF') "+
            "    and tbvd.idven =  tbven.idven  "+
            "    group by tbven.idven "+
            ") ventas  "+
            "order by ventas.monto asc "+
            ") as  minima_venta, "+
            "( "+
            "select top 1 tbven.vfec "+
            "    from tbven "+
            "    where tbven.idcli = tbcli.idcli "+
            "    and tbven.vtipo in ('VF') "+
            "    order by tbven.vfec desc "+
            ") as  ultima_venta, "+
            "( "+
            "select top 1 tbven.vfec "+
            "    from tbven "+
            "    where tbven.idcli = tbcli.idcli "+
            "    and tbven.vtipo in ('VF') "+
            "    order by tbven.vfec asc "+
            ") as  primer_venta, ";

            consulta += "( ";
            consulta += "    select sum(tbvd.vdpre*tbvd.vdcan) ";
            consulta += "        from tbven, tbvd ";
            consulta += "    where tbven.idcli = tbcli.idcli ";
            consulta += "    and tbven.vtipo in ('VD') ";
            consulta += "    and tbven.idven = tbvd.idven ";
            consulta += ") as monto_total_pedidos, ";
            consulta += "( ";
            consulta += "    select sum(tbvd.vdpre*tbvd.vdcan) ";
            consulta += "        from tbven, tbvd ";
            consulta += "    where tbven.idcli = tbcli.idcli ";
            consulta += "    and tbven.vtipo in ('VF') ";
            consulta += "    and tbven.idven = tbvd.idven ";
            consulta += ") as monto_total_ventas ";

            consulta +="from tbcli "+
            "where tbcli.idcli = "+obj.get("idcli");

            JSONArray data = Http.send_(url, consulta, apiKey);
            
            obj.put("data", data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }
}
