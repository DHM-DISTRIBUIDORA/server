package Component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SConfig;
import Servisofts.SPGConect;

public class Reporte {
    public static final String COMPONENT = "reporte";

    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        switch (obj.getString("type")) {
            case "execute_function":
                execute_function(obj, session);
                break;
            case "getClienteConPedidos":
                getClienteConPedidos(obj);
                break;
            case "getClienteSinPedidos":
                getClienteSinPedidos(obj);
                break;
            case "getPedidosRebotados":
                getPedidosRebotados(obj);
                break;
            case "getPedidosRebotadosVendedor":
                getPedidosRebotadosVendedor(obj);
                break;
        }
    }

    public static void getPedidosRebotados(JSONObject obj) {

        try {

            String consulta = "SELECT tbemp.empnom, \n" + //
                    "tbemp.idemp, \n" + //
                    "tbtg.idemp as idtransportista, \n" + //
                    "trans.empnom as transportista, \n" + //
                    "count(tbven.idven) as cantidad ,\n" + //
                    " ( \n"+ 
                    "     select sum(tbvd.vdpre*tbvd.vdcan) \n"+ 
                    "     from tbven ttv JOIN tbtg ON ttv.idtg=tbtg.idtg  \n"+ 
                    "     JOIN tbvd ON ttv.idven=tbvd.idven  \n"+ 
                    "     where tbven.idemp = ttv.idemp   \n"+ 
                    "     and ttv.vtipo LIKE 'V%'  \n"+ 
                    "     and ttv.vfec BETWEEN '" + obj.getString("fecha_inicio") + "T00:00:00' and '"+ obj.getString("fecha_fin") + "T23:59:59' \n" +
                    " ) monto_pedidos, \n"+ 
                    "    STUFF((SELECT ',' + CAST(tbvvv.idven AS VARCHAR(MAX)) \n"
                    +
                    "           FROM tbven tbvvv LEFT JOIN tbtg ON tbvvv.idtg=tbtg.idtg \n" +
                    "           WHERE tbvvv.vfec BETWEEN '" + obj.getString("fecha_inicio") + "T00:00:00' and '"+ obj.getString("fecha_fin") + "T23:59:59' \n" +
                    "           and tbvvv.vtipo LIKE 'V%' \n" +
                    "           AND tbven.idemp = tbvvv.idemp  \n" +
                    "           FOR XML PATH('')), 1, 1, '') AS idven \n" +
                    "FROM tbven  LEFT JOIN tbemp ON tbven.idemp=tbemp.idemp \n" + //
                    "LEFT JOIN tbtg ON tbven.idtg=tbtg.idtg \n" + //
                    "LEFT JOIN tbemp trans ON tbtg.idemp=trans.idemp \n" + //
                    // "WHERE tbven.vtipo LIKE 'V%' AND vdest=2 AND
                    // tbven.vfec='"+obj.get("fecha")+"' AND tbtg.idtg="+tbtg.get("idtg")+" AND
                    // idalm=1 \n" + //
                    "WHERE tbven.vtipo LIKE 'V%' " +
                    "AND tbven.vfec between '" + obj.get("fecha_inicio") + "T00:00:00' AND '" + obj.get("fecha_fin")
                    + "T23:59:59'    \n" + //
                    "GROUP BY tbemp.idemp, tbemp.empnom, tbtg.idemp,trans.empnom, tbven.idemp ";

            obj.put("data", Dhm.query(consulta));

            JSONArray pedidos = obj.getJSONArray("data");

            consulta = "select jsonb_object_agg(tabla.idemp, to_json(tabla.*))::json as json from ( \n";

            JSONObject pedido;
            for (int i = 0; i < pedidos.length(); i++) {
                pedido = pedidos.getJSONObject(i);
                if (!pedido.has("idtransportista") || pedido.isNull("idtransportista"))
                    continue;
                if (!pedido.has("idven") || pedido.isNull("idven"))
                    continue;
                consulta += "select '" + pedido.get("idemp") + "_"+pedido.get("idtransportista")+"' as idemp, \n" +
                        "array_to_json(array_agg(visita_transportista.*)) as visitas, \n" +
                        "count(visita_transportista.idven) as cantidad, \n" +
                        "sum(visita_transportista.monto) as monto ,\n" +
                        pedido.get("monto_pedidos")+" as monto_pedidos ,\n" +
                        "sum(CASE WHEN visita_transportista.tipo  in ('ENTREGADO')  THEN 1 ELSE 0 END) as cantidad_entregados, \n" +
                        "sum(CASE WHEN visita_transportista.tipo  in ('ENTREGADO')  THEN visita_transportista.monto ELSE 0 END) as monto_entregados, \n" +
                        "sum(CASE WHEN visita_transportista.tipo  in ( 'ENTREGADO PARCIALMENTE')  THEN 1 ELSE 0 END) as cantidad_entregados_parciales, \n" +
                        "sum(CASE WHEN visita_transportista.tipo  in ( 'ENTREGADO PARCIALMENTE')  THEN visita_transportista.monto ELSE 0 END) as monto_entregados_parciales, \n" +
                        "sum(CASE WHEN visita_transportista.tipo not in ('ENTREGADO', 'ENTREGADO PARCIALMENTE')  THEN 1 ELSE 0 END) as cantidad_rebotados \n" +
                        "from visita_transportista \n" +
                        "where CAST(visita_transportista.idven AS INTEGER) in (" + pedido.getString("idven") + ") \n" +
                        "and visita_transportista.idven is not null \n" +
                        "and visita_transportista.estado > 0 \n" +
                        "and visita_transportista.idemp = '" + pedido.get("idtransportista") + "' \n" +
                        "and visita_transportista.idven not in ('undefined') \n";

                if (i < pedidos.length() - 1) {
                    consulta += "UNION ALL\n";
                }
            }

            consulta += ") tabla \n";

            obj.put("data_rebotados", SPGConect.ejecutarConsultaObject(consulta));

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void getPedidosRebotadosVendedor(JSONObject obj) {

        try {

            String consulta = "SELECT tbemp.empnom, \n" + //
                    "tbemp.idemp, \n" + //
                    "tbtg.idemp as idtransportista, \n" + //
                    "trans.empnom as transportista, \n" + //
                    "tbven.idven, \n" +
                    "tbven.vobs, \n" +
                    "tbven.vfec, \n" +
                    "tbven.vnum, \n" +
                    "tbven.vdoc, \n" +
                    "tbcli.idcli, \n" +
                    "tbcli.clicod, \n" +
                    "tbcli.clinom \n" +
                    "FROM tbven  LEFT JOIN tbemp ON tbven.idemp=tbemp.idemp \n" + //
                    "LEFT JOIN tbcli ON tbven.idcli=tbcli.idcli \n" + //
                    "LEFT JOIN tbtg ON tbven.idtg=tbtg.idtg \n" + //
                    "LEFT JOIN tbemp trans ON tbtg.idemp=trans.idemp \n" + //
                    // "WHERE tbven.vtipo LIKE 'V%' AND vdest=2 AND
                    // tbven.vfec='"+obj.get("fecha")+"' AND tbtg.idtg="+tbtg.get("idtg")+" AND
                    // idalm=1 \n" + //
                    "WHERE tbven.vtipo LIKE 'V%' " +
                    "AND tbven.vfec between '" + obj.get("fecha_inicio") + "T00:00:00' AND '" + obj.get("fecha_fin")+ "T23:59:59'    \n" + //
                    "AND tbven.idemp = " + obj.get("idvendedor") + "    \n" + //
                    "AND tbtg.idemp = " + obj.get("idtransportista") + "  \n" + //
                    "";

            obj.put("data", Dhm.query(consulta));

            
            JSONArray pedidos = obj.getJSONArray("data");
            
            
            
            
            JSONObject pedido;
            
            String idvens = "";
            for (int i = 0; i < pedidos.length(); i++) {
                pedido = pedidos.getJSONObject(i);
                if(!pedido.has("idtransportista") || pedido.isNull("idtransportista"))
                continue;
                if(!pedido.has("idven") || pedido.isNull("idven")) continue;
                idvens+=""+pedido.get("idven")+",";
            }
            
            if(idvens.length()>0)idvens = idvens.substring(0, idvens.length()-1);
            
            consulta = "select jsonb_object_agg(visita_transportista.idven, to_json(visita_transportista.*))::json as json \n"+
            "from visita_transportista \n" +
            "where CAST(visita_transportista.idven AS INTEGER) in (" + idvens + ") \n" +
            "and visita_transportista.idven is not null \n" +
            "and visita_transportista.estado > 0 \n" +
            "and  CAST(visita_transportista.idemp AS INTEGER) in (" + obj.get("idtransportista") + ") \n" +
            "and visita_transportista.idven not in ('undefined') \n";
            
            
            
            obj.put("data_rebotados", SPGConect.ejecutarConsultaObject(consulta));
            
      } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void getClienteSinPedidos(JSONObject obj) {
        try {

            String idz = "";
            if (obj.has("idemp")) {
                String consulta = "select array_to_json(array_agg(idz))::json as json from zona_empleado where idemp = "
                        + obj.get("idemp");
                idz = SPGConect.ejecutarConsultaArray(consulta) + "";
                if (idz.length() > 0) {
                    idz = idz.substring(1, idz.length() - 1);
                }
            }

            String consulta = "select tbcli.idcli,\n" + //
                    "tbcli.clicod,\n" + //
                    "tbcli.clinom,\n" + //
                    "tbcli.clidir,\n" + //
                    "tbcli.clilat,\n" + //
                    "tbcli.clilon\n" + //
                    "from tbcli\n" + //
                    "where tbcli.idcli not in (\n" + //

                    "select  \n" +
                    "tbcli.idcli \n" +
                    "from  \n" +
                    "tbcli, \n" +
                    "dm_cabfac, \n" +
                    "dm_detfac, \n" +
                    "tbemp \n" +
                    "where  dm_detfac.idven = dm_cabfac.idven \n" +
                    "and dm_detfac.vdcan > 0 \n" +
                    "and tbcli.clicod = dm_cabfac.clicod \n";
            if (obj.has("idz")) {
                consulta += "and tbcli.idz = " + obj.get("idz") + " \n";
            }

            consulta += "and dm_cabfac.vfec between '" + obj.getString("fecha_inicio") + "' and '"
                    + obj.getString("fecha_fin") + "'\n" + //
                    "and tbemp.empcod = dm_cabfac.codvendedor \n" +
                    "and dm_cabfac.idven not in (\n" +
                    "    select dm_cabfac.idven\n" +
                    "    from dm_cabfac,\n" +
                    "    tbven\n" +
                    "    where tbven.idpeddm = dm_cabfac.idven)\n" +
                    "group by  \n" +
                    "tbcli.idcli \n" +

                    ") \n";
            if (obj.has("idz")) {
                consulta += "and tbcli.idz = " + obj.get("idz") + " ";
            }
            if (idz.length() > 0) {
                consulta += "and tbcli.idz in( " + idz + " )";
            }
            obj.put("data", Dhm.query(consulta));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getClienteConPedidos(JSONObject obj) {
        try {

            String consulta = "select dm_cabfac.idven,\n" +
                    "tbcli.clinom,\n" +
                    "tbcli.clicod,\n" +
                    "tbcli.clilat,\n" +
                    "tbcli.clilon,\n" +
                    "tbprd.prdcod,\n" +
                    "tbprd.prdnom,\n" +
                    "tbprdlin.lincod,\n" +
                    "tbprdlin.linnom,\n" +
                    "tbemp.empcod,\n" +
                    "tbemp.empnom,\n" +
                    "dm_cabfac.vfec,\n" +
                    "dm_cabfac.vhora,\n" +
                    "vlatitud as pedidolat,\n" +
                    "vlongitud as pedidolon\n" +
                    "from dm_cabfac,\n" +
                    "dm_detfac,\n" +
                    "tbcli,\n" +
                    "tbprd,\n" +
                    "tbprdlin,\n" +
                    "tbemp\n" +
                    "where dm_cabfac.vfec between '" + obj.getString("fecha_inicio") + "T00:00:00' and '"
                    + obj.getString("fecha_fin") + "T00:00:00' \n" +
                    "and tbcli.clicod = dm_cabfac.clicod\n" +
                    "and dm_cabfac.idven = dm_detfac.idven\n" +
                    "and tbprd.idlinea = tbprdlin.idlinea\n" + //
                    "and tbprd.prdcod = dm_detfac.prdcod\n" +
                    "and tbemp.empcod = dm_cabfac.codvendedor";

            obj.put("data", Dhm.query(consulta));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static String getParamRecursive(JSONObject obj, String param) {
        String[] points = param.split("\\.");
        if (points.length == 1) {
            return obj.get(points[0]).toString();
        }

        return getParamRecursive(obj.getJSONObject(points[0]), param.replaceAll(points[0] + ".", ""));
    }

    public static void execute_function(JSONObject obj, SSSessionAbstract session) {
        try {

            if (obj.has("service") && !obj.getString("service").equals(SConfig.getJSON().getString("nombre")))
                return;

            if (!obj.has("func"))
                throw new Exception("[func] Parameter not found.");
            if (obj.isNull("func"))
                throw new Exception("[func] Parameter required.");

            String params = "";
            if (obj.has("params") && !obj.isNull("params")) {
                JSONArray arr = obj.getJSONArray("params");
                for (int i = 0; i < arr.length(); i++) {
                    String valParam = arr.get(i).toString();
                    if (valParam.contains("$")) {
                        Pattern patron = Pattern.compile("\\$\\{(.+?)\\}");
                        Matcher matcher = patron.matcher(valParam);
                        if (matcher.find()) {
                            String resultado = matcher.group(1);
                            String p = getParamRecursive(obj, resultado);
                            String p2 = valParam.replaceAll("\\$\\{(.+?)\\}", p);
                            valParam = p2;
                        }
                    }
                    params += valParam;

                    if (i + 1 < arr.length()) {
                        params += ",";
                    }
                }
            }
            String func = obj.getString("func");
            obj.put("data", SPGConect.ejecutarConsultaArray("select " + func + "(" + params + ") as json"));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("error", e.getLocalizedMessage());
            obj.put("estado", "error");
        }
    }
}
