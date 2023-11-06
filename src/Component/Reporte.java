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
        }
    }

    public static void getClienteSinPedidos(JSONObject obj) {
        try {
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
                    if(obj.has("idz")){
                        consulta += "and tbcli.idz = "+obj.get("idz")+" \n" ;
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
                    if(obj.has("idz")){
                        consulta += "and tbcli.idz = "+obj.get("idz")+" ";
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
            String consulta = "SET DATEFORMAT ymd; select \n" + //
                    "count(dm_detfac.vdcan) as cantidad,\n" + //
                    "sum(dm_detfac.vdpre) as monto,\n" + //
                    "tbcli.idcli,\n" + //
                    "tbcli.clidir,\n" +
                    "tbcli.clicod,\n" +
                    "tbcli.clinom,\n" + //
                    "tbcli.clilat,\n" + //
                    "tbcli.clilon\n" + //
                    "from tbcli,\n" + //
                    "dm_cabfac,\n" + //
                    "dm_detfac,\n" + //
                    "tbemp\n" + //
                    "where  dm_detfac.idven = dm_cabfac.idven \n" +
                    "and dm_detfac.vdcan > 0 \n" +
                    "and tbcli.clicod = dm_cabfac.clicod \n" +
                    "and dm_cabfac.vfec between '" + obj.getString("fecha_inicio") + "' and '"
                    + obj.getString("fecha_fin") + "' \n" +
                    "and tbemp.empcod = dm_cabfac.codvendedor \n" +
                    "    and dm_cabfac.idven not in (\n" +
                    "    select dm_cabfac.idven\n" +
                    "    from dm_cabfac,\n" +
                    "    tbven\n" +
                    "    where tbven.idpeddm = dm_cabfac.idven)\n";
            if (obj.has("idemp")) {
                consulta += "and tbemp.idemp = " + obj.get("idemp") + "\n"; //
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
