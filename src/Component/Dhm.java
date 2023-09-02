package Component;

import java.util.Calendar;
import java.util.GregorianCalendar;

import org.json.JSONArray;
import org.json.JSONObject;
import Servisofts.SConfig;
import Servisofts.SUtil;
import Server.SSSAbstract.SSSessionAbstract;

public class Dhm {
    public static final String COMPONENT = "dhm";

    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        switch (obj.getString("type")) {
            case "get":
                get(obj, session);
                break;
            case "perfilEmp":
                perfilEmp(obj, session);
                break;
        }
    }

    public static void perfilEmp(JSONObject obj, SSSessionAbstract session) {
        try {
            String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
            String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");

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
                    ") as  cantidad_ventas, " +
                    "( " +
                    "    select count(tbcom.idcom) " +
                    "    from tbcom " +
                    "    where tbcom.idemp = tbemp.idemp " +
                    ") as  cantidad_compras, ";

            consulta += "(select sum(tbvd.vdpre*tbvd.vdcan)  from tbven, tbvd where tbven.vtipo in ('VF', 'VD') and tbven.vefa not in ('A')  and tbven.idemp = tbemp.idemp and tbvd.idven = tbven.idven) as monto_total_ventas ";

            consulta += "from tbemp " +
                    "where tbemp.idemp = " + obj.get("idemp");

            JSONArray data = Http.send_(url, consulta, apiKey);

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

    public static JSONArray getByKey(String nombreTabla, String PK, String key) {
        try {
            String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
            String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
            return Http.send_(url, "select * from " + nombreTabla + " where " + PK + " = '" + key + "'", apiKey);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static JSONArray getMetaData(String nombreTabla) throws Exception {
        String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
        String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
        return Http.send_(url, "select * from information_schema.columns where table_name = '" + nombreTabla + "' ",
                apiKey);
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

        String consulta = "";

        for (int k = 0; k < obj.length(); k++) {

            String names = "";
            String values = "";

            JSONObject data = obj.getJSONObject(k);

            JSONArray medatada = getMetaData(nombreTabla);
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

            consulta += "SET DATEFORMAT 'YMD'; insert into " + nombreTabla + " ( " + names + " ) values ( " + values
                    + " ); ";
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

    public static boolean eliminar(String nombreTabla, String PK, String key) throws Exception {
        String url = SConfig.getJSON("sqlServerApi").getString("url") + "api/select";
        String apiKey = SConfig.getJSON("sqlServerApi").getString("apiKey");
        Http.send_(url, "delete " + nombreTabla + " where " + PK + " = '" + key + "'", apiKey);
        return true;
    }
}
