package Component;

import org.json.JSONArray;
import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SUtil;

public class TbTg {
    public static final String COMPONENT = "tbtg";
    public static final String PK = "idtg";

    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        switch (obj.getString("type")) {
            case "getAll":
                getAll(obj, session);
                break;
            case "getByKey":
                getByKey(obj, session);
                break;
            case "getPedidosDespachados":
                getPedidosDespachados(obj, session);
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
            String consulta = "select * from tbtg where tgest = 'DESPACHADO' and idemp = "+obj.getInt("idemp");
            obj.put("data", Dhm.query(consulta));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getAllVentas(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select * from tbtg where tbtg.tgest = 'DESPACHADO' and tbtg.idemp = "+obj.getInt("idemp");
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


    public static void getPedidosDespachados(JSONObject obj, SSSessionAbstract session) {
        try {

            

            String consulta = "select tbcli.*\n"+
            "from tbcli\n" + //
            "where tbcli.idcli in (\n" + //
            "select tbven.idcli\n" + //
            "from tbven,\n" + //
            "tbvd,\n" + //
            "tbtg,\n" + //
            "tbcli\n" + //
            "where tbvd.idven = tbven.idven \n" + //
            "and tbven.idtg = tbtg.idtg\n" + //
            "and tbtg.tgest = 'DESPACHADO'\n" + //
            "and tbtg.idemp = "+obj.get("idemp")+"\n" + //
            "and tbtg.tgfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" +
            "group by tbven.idcli\n" + //
            ") order by tbcli.idcli desc\n";
            
            JSONArray tbclis = Dhm.query(consulta);
            JSONObject tbcli;

            consulta = "select tbvd.*, tbven.idcli\n" + //
            "from tbven,\n" + //
            "tbvd,\n" + //
            "tbtg\n" + //
            "where tbvd.idven = tbven.idven \n" + //
            "and tbven.idtg = tbtg.idtg\n" + //
            "and tbtg.tgest = 'DESPACHADO'\n" + //
            "and tbtg.tgfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" +
            "and tbtg.idemp = "+obj.get("idemp")+"\n" + //
            "order by tbven.idcli desc\n";

            JSONObject clientes = new JSONObject();

            JSONArray tbvds = Dhm.query(consulta);
            JSONObject tbvd;

            for (int i = 0; i < tbclis.length(); i++) {
                tbcli = tbclis.getJSONObject(i);

                for (int j = 0; j < tbvds.length(); j++) {
                    tbvd = tbvds.getJSONObject(j);

                    if(tbvd.get("idcli").equals(tbcli.get("idcli"))){
                        
                        if(!tbcli.has("tbvd")){
                            tbcli.put("tbvd", new JSONArray().put(tbvd));   
                            tbvds.remove(j);
                            j--;
                            continue;
                        }
                        tbcli.getJSONArray("tbvd").put(tbvd);  
                        tbvds.remove(j);
                        j--;
                    }
                }

                clientes.put(tbcli.get("idcli")+"", tbcli);
            }


            obj.put("data", clientes);
            JSONObject visitas = VisitaTransportista.getVisitas(obj.get("idemp")+"", obj.get("fecha_inicio")+"", obj.get("fecha_fin")+"");
            obj.put("visitas", visitas);
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


    
}
