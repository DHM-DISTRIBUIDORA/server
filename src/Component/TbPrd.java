package Component;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.JSONArray;
import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SPGConect;
import Servisofts.SUtil;

public class TbPrd {
    public static final String COMPONENT = "tbprd";
    public static final String PK = "idprd";

    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        switch (obj.getString("type")) {
            case "getAll":
                getAll(obj, session);
                break;
                case "getAllSimple":
                getAllSimple(obj, session);
                break;
            case "getByKey":
                getByKey(obj, session);
                break;
                case "getProductosVendidos":
                getProductosVendidos(obj, session);
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

    public static void getProductosVendidos(JSONObject obj, SSSessionAbstract session) {
        try {
            String filtro = "";

            if(obj.has("idemp")){
                filtro = "and tbven.idemp = "+obj.getInt("idemp")+"\n";
            }

            obj.put("data", Dhm.query("select sum(tbvd.vdcan) as cantidad,\n" + //
                    "tbprd.prdnom,\n" + //
                    "tbvd.idprd,\n" + //
                    "tbprd.prdcod,\n" + //
                    "sum(tbvd.vdpre*tbvd.vdcan) as monto\n" +
                    "from tbven,\n" + //
                    "tbvd,\n" + //
                    "tbprd\n" + //
                    "where vtipo in ('VF', 'VD')\n" + //
                    "and tbvd.idven = tbven.idven\n" + //
                    "and tbven.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" + //
                    "and tbprd.idprd = tbvd.idprd\n" + filtro +//
                    
                    
                    "group by tbprd.prdnom,\n" + //
                    "tbvd.idprd, tbprd.prdcod")); 
            
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getAllSimple(JSONObject obj, SSSessionAbstract session) {

       

        try {

             
            String almacen = " and idalm in (1) ";
            
            if(obj.has("idemp") && !obj.isNull("idemp")){
                String consulta = "select array_to_json(array_agg(idalm)) as json\n" + //
                                    "from almacen_empleado\n" + //
                                    "where almacen_empleado.estado > 0\n" + //
                                    "and almacen_empleado.idemp = "+obj.get("idemp")+"\n" + //
                                    "and almacen_empleado.dia =  date_part('dow',current_date)";
                JSONArray idalms = SPGConect.ejecutarConsultaArray(consulta);
                
                if(idalms!=null && !idalms.isEmpty()){
                    almacen = " and idalm in ("+idalms.toString().replaceAll("\\[", "").replaceAll("\\]", "")+")";
                }
            }
    
            SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");


            String consulta = "SELECT \n" + //
                    "    tbprd.prdcod,\n" + //
                    "    tbprd.prduxcdes,\n" + //
                    "    tbprd.idlinea,\n" + //
                    "    tbprd.prdcxu,\n" + //
                    "    tbprd.prduxd,\n" + //
                    "    tbprd.prdcor,\n" + //
                    "    tbprd.idprd,\n" + //
                    "    tbprd.prdunid,\n" + //
                    "    tbprd.prdpoficial,\n" + //
                    "    tbprd.prdnom,\n" + //
                    "    COALESCE(compras.cantidad, 0)-COALESCE(ventas.cantidad, 0) AS stock\n" + //
                    "FROM \n" + //
                    "    tbprd\n" + //
                    "LEFT JOIN \n" + //
                    "    (\n" + //
                    "        SELECT  tbvd.idprd,\n" + //
                    "            SUM(tbvd.vdcan) AS cantidad\n" + //
                    "        FROM tbvd \n" + //
                    "        JOIN tbven ON tbvd.idven = tbven.idven\n" + //
                    "        where  tbven.vfec <=  '"+formato.format(new Date())+"' "+
                    ""+almacen+
                    "        GROUP BY  tbvd.idprd\n" + //
                    "    ) ventas ON tbprd.idprd = ventas.idprd\n" + //
                    "LEFT JOIN \n" + //
                    "    (\n" + //
                    "        SELECT tbcd.idprd,\n" + //
                    "            SUM(tbcd.cdcan) AS cantidad\n" + //
                    "        FROM tbcd \n" + //
                    "        JOIN tbcom ON tbcom.idcom = tbcd.idcom\n" + //
                    "        where tbcom.cfec <=  '"+formato.format(new Date())+"' "+
                    ""+almacen+
                    "        GROUP BY tbcd.idprd\n" + //
                    "    ) compras ON tbprd.idprd = compras.idprd;\n" + //
                    "";


            obj.put("data", Dhm.query(consulta));
            obj.put("estado", "exito");

        }catch(Exception e){
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
