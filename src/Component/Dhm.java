package Component;

import org.json.JSONArray;
import org.json.JSONObject;
import Servisofts.SConfig;
import Servisofts.SUtil;
import Server.SSSAbstract.SSSessionAbstract;

public class Dhm {
    public static final String COMPONENT = "dhm";
    public static JSONObject metaData = new JSONObject();

    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        switch (obj.getString("type")) {
            case "get":
                get(obj, session);
                break;
            case "perfilEmp":
                perfilEmp(obj, session);
                break;
            case "perfilTransportista":
                perfilTransportista(obj, session);
                break;
        }
    }

    public static void perfilEmp(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "" +

                    "select tbemp.idemp, " +
                    "( " +
                    "    select count(tbcli.idcli) " +
                    "    from tbcli " +
                    "    where cliidemp = tbemp.idemp " +
                    ") as  cantidad_clientes, " +
                    "( " +
                    "    select count(tbzon.idz) " +
                    "    from tbzon " +
                    "    where tbzon.idemp = tbemp.idemp " +
                    ") as  cantidad_zonas, " +
                    "( " +
                    "    select count(tbven.idven) " +
                    "    from tbven " +
                    "    where tbven.idemp = tbemp.idemp " +
                    "    and tbven.vtipo in ('VD', 'VF') " +
                    "    and tbven.vefa not in ('A') " +
                    "    and tbven.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" +
                    ") as  cantidad_ventas, " +
                    "( " +
                    "    select count(tbcom.idcom) " +
                    "    from tbcom " +
                    "    where tbcom.idemp = tbemp.idemp " +
                    "    and tbcom.cfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" +
                    ") as  cantidad_compras, "+
                    "( " +
                    "    select count(dm_cabfac.idven) " +
                    "    from dm_cabfac  " +
                    "    where  dm_cabfac.codvendedor = tbemp.empcod " +
                    "    and dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" +
                    "    and dm_cabfac.idven not in (\n"+
	                "    select dm_cabfac.idven\n"+
                    "    from dm_cabfac,\n"+
                    "    tbven\n"+
                    "    where tbven.idpeddm = dm_cabfac.idven)\n"+
                    ") as  cantidad_pedidos, \n"+
                    "( " +
                    "    select sum(dm_detfac.vdpre*dm_detfac.vdcan)" +
                    "    from dm_cabfac, dm_detfac  " +
                    "    where  dm_cabfac.codvendedor = tbemp.empcod " +
                    "    and  dm_cabfac.idven = dm_detfac.idven " +
                    "    and dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" +
                    "    and dm_cabfac.idven not in (\n"+
	                "    select dm_cabfac.idven\n"+
                    "    from dm_cabfac,\n"+
                    "    tbven\n"+
                    "    where tbven.idpeddm = dm_cabfac.idven)\n"+
                    ") as  monto_pedidos, ";

            consulta += "(select sum(tbvd.vdpre*tbvd.vdcan)";
            consulta += "from tbven, tbvd  \n";
            consulta += "where tbven.vtipo in ('VF', 'VD') and tbven.vefa not in ('A') \n";
            consulta += "and tbven.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" ;
            consulta += "and tbven.idemp = tbemp.idemp and tbvd.idven = tbven.idven) \n";
            consulta += "as monto_total_ventas  \n";

            consulta += "from tbemp " +
                    "where tbemp.idemp = " + obj.get("idemp");

            JSONArray data = Dhm.query(consulta);

            obj.put("data", data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void perfilTransportista(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select tbemp.idemp, \n"+
                    "(  \n"+
                    "select count(tbvd.idven) \n"+
                    "from tbven, \n"+
                    "tbvd, \n"+
                    "tbtg \n"+
                    "where tbvd.idven = tbven.idven  \n"+
                    "and tbven.idtg = tbtg.idtg \n"+
                    "and tbtg.tgest = 'DESPACHADO' \n"+
                    "and tbtg.idemp = tbemp.idemp \n"+
                    "and tbtg.idemp = tbemp.idemp \n"+
                    "and tbtg.tgfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" +
                    ") as cantidad_total_items, \n"+
                    "(  \n"+
                    "select sum(tbvd.vdpre*tbvd.vdcan) \n"+
                    "from tbven, \n"+
                    "tbvd, \n"+
                    "tbtg \n"+
                    "where tbvd.idven = tbven.idven  \n"+
                    "and tbven.idtg = tbtg.idtg \n"+
                    "and tbtg.tgest = 'DESPACHADO' \n"+
                    "and tbtg.idemp = tbemp.idemp \n"+
                    "and tbtg.tgfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" +
                    ") as monto_total_items, \n"+
                    "(  \n"+
                    "select count(tabla.idcli) \n"+
                    "from ( \n"+
                    "select count(tbven.idven) cant, \n"+
                    "tbven.idcli \n"+
                    "from tbven, \n"+
                    "tbvd, \n"+
                    "tbtg \n"+
                    "where tbvd.idven = tbven.idven  \n"+
                    "and tbven.idtg = tbtg.idtg \n"+
                    "and tbtg.tgest = 'DESPACHADO' \n"+
                    "and tbtg.idemp = tbemp.idemp \n"+
                    "and tbtg.tgfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" +
                    "group by tbven.idcli \n"+
                    ") tabla \n"+
                    ") as cantidad_clientes_con_pedido \n"+
                    "from tbemp \n"+
                    "where tbemp.idemp = " + obj.get("idemp")+ "\n";

            JSONArray data = Dhm.query(consulta);

            obj.put("data", data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    

    public static void get(JSONObject obj, SSSessionAbstract session) {
        try {
            String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
            String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
            JSONArray data = Http.send_(url, obj.getString("select"), apiKey);

            obj.put("data", data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static JSONArray getAll(String nombreTabla) {
        try {
            String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
            String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
            return Http.send_(url, "select * from " + nombreTabla, apiKey);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static JSONArray query(String consulta) {
        try {
            String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
            String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
            return Http.send_(url, "SET DATEFORMAT 'YMD'; " + consulta, apiKey);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static JSONArray getMax(String nombreTabla, String column, String where) {
        try {
            String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
            String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
            return Http.send_(url, "select max(" + column + ") as max from " + nombreTabla + " " + where, apiKey);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static JSONArray getAll(String nombreTabla, String order) {
        try {
            String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
            String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
            return Http.send_(url, "select  * from " + nombreTabla + " order by " + order, apiKey);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static JSONArray getAll(String nombreTabla, int top) {
        try {
            String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
            String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
            return Http.send_(url, "select top " + top + " * from " + nombreTabla, apiKey);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static JSONArray getAll(String nombreTabla, int top, String order) {
        try {
            String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
            String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
            return Http.send_(url, "select top " + top + " * from " + nombreTabla + " order by " + order, apiKey);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static JSONArray getByKey(String nombreTabla, String PK, String key) throws Exception {
    
        String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
        String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
        return Http.send_(url, "select * from " + nombreTabla + " where " + PK + " = '" + key + "'", apiKey);
    
    }

    public static JSONArray getMetaData(String nombreTabla) throws Exception {

        if(metaData.has(nombreTabla) && !metaData.isNull(nombreTabla)){
            return metaData.getJSONArray(nombreTabla);
        }

        String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
        String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
        JSONArray data = Http.send_(url, "select * from information_schema.columns where table_name = '" + nombreTabla + "' ",apiKey);
        metaData.put(nombreTabla, data);
        return data;
    }

    public static boolean registro(String nombreTabla, String PK, JSONObject data) throws Exception {
        String names = "";
        String values = "";

        JSONArray medatada = getMetaData(nombreTabla);
        JSONObject medatada_ = new JSONObject();
        for (int i = 0; i < medatada.length(); i++) {
            medatada_.put(medatada.getJSONObject(i).getString("COLUMN_NAME"), medatada.getJSONObject(i));
        }

        String dato;
        for (int i = 0; i < JSONObject.getNames(data).length; i++) {
            dato = JSONObject.getNames(data)[i];

            if (!medatada_.has(dato)) {
                continue;
            }
            if (!dato.equals(PK)) {
                names += dato + ",";
            }

            Object valor = data.get(dato);
            if (valor instanceof String) {
                // Tipo de dato: String

                try {
                    // Verificando si es fecha
                    SUtil.parseTimestamp(valor.toString());
                    System.out.println("Falta insertar una fecha");
                    values += "CAST('" + valor + "' AS DATETIME2), ";
                } catch (Exception e) {
                    if (!dato.equals(PK)) {
                        values += "'" + valor + "',";
                    }
                }

            } else {
                if (!dato.equals(PK)) {
                    values += valor + ",";
                }
            }
        }

        names = names.substring(0, names.length() - 1);
        values = values.substring(0, values.length() - 1);

        String consulta = "SET DATEFORMAT 'YMD'; insert into " + nombreTabla + " ( " + names + " ) values ( " + values
                + " );  SELECT MAX(" + PK + ") AS curval FROM " + nombreTabla;
        System.out.println(consulta);
        String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
        String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
        JSONArray last = Http.send_(url, consulta, apiKey);
        data.put(PK, last.getJSONObject(0).getInt("curval"));
        return true;

    }

    public static void registroAll(String nombreTabla, String PK, JSONArray obj) throws Exception {

        String consulta = "SET DATEFORMAT 'YMD';\n";

        JSONArray medatada = getMetaData(nombreTabla);

        for (int k = 0; k < obj.length(); k++) {

            String names = "";
            String values = "";

            JSONObject data = obj.getJSONObject(k);

            JSONObject medatada_ = new JSONObject();

            for (int i = 0; i < medatada.length(); i++) {
                medatada_.put(medatada.getJSONObject(i).getString("COLUMN_NAME"), medatada.getJSONObject(i));
            }

            for (int i = 0; i < JSONObject.getNames(data).length; i++) {
                String dato = JSONObject.getNames(data)[i];

                if (!medatada_.has(dato)) {
                    continue;
                }
                if (!dato.equals(PK)) {
                    names += dato + ",";
                }

                Object valor = data.get(dato);
                if (valor instanceof String) {
                    // Tipo de dato: String

                    try {
                        // Verificando si es fecha
                        SUtil.parseTimestamp(valor.toString());
                        values += "CAST('" + valor + "' AS DATETIME2), ";
                    } catch (Exception e) {
                        if (!dato.equals(PK)) {
                            values += "'" + valor + "',";
                        }
                    }

                } else {
                    if (!dato.equals(PK)) {
                        values += valor + ",";
                    }
                }
            }

            names = names.substring(0, names.length() - 1);
            values = values.substring(0, values.length() - 1);

            consulta += "insert into " + nombreTabla + " ( " + names + " ) values ( " + values + " ); \n";
        }

        String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
        String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
        Http.send_(url, consulta, apiKey);

    }

    public static boolean editar(String nombreTabla, String PK, JSONObject data) throws Exception {
        String set = "";
        String pk = "";

        JSONArray medatada = getMetaData(nombreTabla);
        JSONObject medatada_ = new JSONObject();
        for (int i = 0; i < medatada.length(); i++) {
            medatada_.put(medatada.getJSONObject(i).getString("COLUMN_NAME"), medatada.getJSONObject(i));
        }

        String dato;
        for (int i = 0; i < JSONObject.getNames(data).length; i++) {
            dato = JSONObject.getNames(data)[i];

            if (!medatada_.has(dato)) {
                continue;
            }

            if (!dato.equals(PK)) {
                set += dato + " = ";
            }
            Object valor = data.get(dato);
            if (valor instanceof String) {
                // Tipo de dato: String
                try {
                    // Verificando si es fecha
                    SUtil.parseTimestamp(valor.toString());
                    if (dato.equals(PK)) {
                        pk = valor + "";
                    } else {
                        set += "CAST('" + valor + "' AS DATETIME2), ";
                    }

                } catch (Exception e) {
                    if (dato.equals(PK)) {
                        pk = "'" + valor + "'";
                    } else {
                        set += "'" + valor + "',";
                    }
                }
            } else {
                if (dato.equals(PK)) {
                    pk = valor + "";
                } else {
                    set += valor + ",";
                }
            }
        }

        set = set.substring(0, set.length() - 1);

        String consulta = "SET DATEFORMAT 'YMD'; update " + nombreTabla + " set " + set + " where " + PK + " = " + pk;
        System.out.println(consulta);
        String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
        String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
        Http.send_(url, consulta, apiKey);
        return true;
    }

    public static boolean editarAll(String nombreTabla, String PK, JSONArray datas) throws Exception {
        String set = "";
        String pk = "";

        String consulta = "SET DATEFORMAT 'YMD';\n";

        JSONArray medatada = getMetaData(nombreTabla);
        JSONObject medatada_ = new JSONObject();
        for (int i = 0; i < medatada.length(); i++) {
            medatada_.put(medatada.getJSONObject(i).getString("COLUMN_NAME"), medatada.getJSONObject(i));
        }

        for (int h = 0; h < datas.length(); h++) {

            String dato;
            JSONObject data = datas.getJSONObject(h);

            set = "";
            for (int i = 0; i < JSONObject.getNames(data).length; i++) {
                dato = JSONObject.getNames(data)[i];

                if (!medatada_.has(dato)) {
                    continue;
                }

                if (!dato.equals(PK)) {
                    set += dato + " = ";
                }
                Object valor = data.get(dato);
                if (valor instanceof String) {
                    // Tipo de dato: String
                    try {
                        // Verificando si es fecha
                        SUtil.parseTimestamp(valor.toString());
                        if (dato.equals(PK)) {
                            pk = valor + "";
                        } else {
                            set += "CAST('" + valor + "' AS DATETIME2), ";
                        }

                    } catch (Exception e) {
                        if (dato.equals(PK)) {
                            pk = "'" + valor + "'";
                        } else {
                            set += "'" + valor + "',";
                        }
                    }
                } else {
                    if (dato.equals(PK)) {
                        pk = valor + "";
                    } else {
                        set += valor + ",";
                    }
                }
            }
            
            set = set.substring(0, set.length() - 1);

            consulta += "update " + nombreTabla + " set " + set + " where " + PK + " = " + pk+";\n";
        }

    
        String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
        String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
        Http.send_(url, consulta, apiKey);
        return true;
    }

    public static boolean eliminar(String nombreTabla, String PK, String key) throws Exception {
        String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
        String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
        Http.send_(url, "delete " + nombreTabla + " where " + PK + " = '" + key + "'", apiKey);
        return true;
    }

    public static boolean eliminarAll(String nombreTabla, String PK, JSONArray keys) throws Exception {
        String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
        String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
        String consulta = "";
        for (int i = 0; i < keys.length(); i++) {
            consulta += "delete " + nombreTabla + " where " + PK + " = '" + keys.get(i) + "';";
        }
        if(consulta.length() <= 0){
            return true;
        }
        Http.send_(url, consulta, apiKey);
        return true;
    }
}
