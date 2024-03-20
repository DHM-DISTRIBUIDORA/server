package Component;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.json.JSONArray;
import org.json.JSONObject;
import Servisofts.SConfig;
import Servisofts.SPGConect;
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
            case "dashboardVendedor":
                dashboardVendedor(obj, session);
                break;
            case "dashboardTransportista":
                dashboardTransportista(obj, session);
                break;
            case "perfilTransportista":
                perfilTransportista(obj, session);
                break;
            case "getPedidosProveedor":
                getPedidosProveedor(obj, session);
                break;
            case "getPedidosProveedorProductos":
                getPedidosProveedorProductos(obj, session);
                break;
            case "getPedidosProveedorClientes":
                getPedidosProveedorClientes(obj, session);
                break;
            case "getEntregasTransportista":
                getEntregasTransportista(obj, session);
                break;
        }
    }
    public static void getEntregasTransportista(JSONObject obj, SSSessionAbstract session) {
        try{

            String consulta = "SET DATEFORMAT 'YMD';\n" + //
                    "SELECT tbtg.idemp,  \n" + //
                    "tbemp.empnom, \n" + //
                    "tbemp.empcod, \n" + //
                    "count(tbven.idven) as total_pedidos,\n" + //
                    "sum(isnull(vr.TimpR,0)) as monto_productos,\n" + //
                    "sum(isnull(vr.Tcanven,0)) as cantidad_productos\n" + //
                    "\n" + //
                    "--sum(vr.Tcanven) as cantidad_productos \n" + //
                    "FROM tbven LEFT JOIN tbtg ON tbven.idtg=tbtg.idtg \n" + //
                    "LEFT JOIN tbemp ON tbtg.idemp=tbemp.idemp \n" + //
                    "LEFT JOIN tbcli ON tbven.idcli=tbcli.idcli \n" + //
                    "LEFT JOIN ( \n" + //
                    "\tSELECT tbven.idven, \n" + //
                    "\tsum(vdimp) AS TimpR, \n" + //
                    "\tsum(vdcan) AS Tcanven \n" + //
                    "\tFROM tbven, tbvd\n" + //
                    "\tWHERE tbven.idven=tbvd.idven \n" + //
                    "\tAND vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'  \n" + //
                    "\tgroup by tbven.idven, tbven.vtipp\n" + //
                    ") AS VR ON tbven.idven=vr.idven \n" + //
                    "\n" + //
                    "WHERE  tbtg.tgfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'  \n" + //
                    "group by tbtg.idemp ,tbemp.empnom, tbemp.empcod";

            JSONArray pedidos = Dhm.query(consulta);

            consulta = "select jsonb_object_agg(tabla.idemp, to_json(tabla.*))::json as json\n"+
                "from (\n"+
                "select  \n"+
                "visita_transportista.idemp,\n"+
                "count(visita_transportista.key) as visitas,\n"+
                "sum(case when tipo = 'ENTREGADO' then monto else 0 end) as monto_visitas_exitosas,\n" + //
                "sum(case when tipo != 'ENTREGADO' then monto else 0 end) as monto_visitas_fallidas,\n" + //
                "sum(case when tipo = 'ENTREGADO' then 1 else 0 end) as visitas_exitosas,\n" + //
                "sum(case when tipo != 'ENTREGADO' then 1 else 0 end) as visitas_fallidas\n"+
                "from visita_transportista\n"+
                "where estado > 0\n"+
                "AND fecha::date between '"+obj.getString("fecha_inicio")+"'::date and '"+obj.getString("fecha_fin")+"'::date  \n" + //
                "group by idemp\n"+
                ") tabla";
            JSONObject visitas = SPGConect.ejecutarConsultaObject(consulta);

            obj.put("visitas", visitas);
            obj.put("pedidos", pedidos);

        }catch(Exception e){
            e.printStackTrace();
        }   
    }

    public static void getPedidosProveedorProductos(JSONObject obj, SSSessionAbstract session) {
        try{

            String empCod = "";
            if(obj.has("empcod")){
                empCod = "and dm_cabfac.codvendedor = '"+obj.get("empcod")+"'\n";
            }

            String consulta = "\n" + //
                 "select tbprd.prdcod,\n" + //
                         "tbprd.prdnom,\n" + //
                         "sum(dm_detfac.vdcan) as cant,\n" + //
                         "sum(dm_detfac.vdpre*dm_detfac.vdcan) as monto\n" + //
                         "from dm_cabfac, \n" + //
                         "dm_detfac,\n" + //
                         "tbprd,\n" + //
                         "tbprdlin\n" + //
                         "where dm_cabfac.idven = dm_detfac.idven     \n" + //
                         "and dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n"+
                         "and dm_detfac.prdcod = tbprd.prdcod\n" + empCod+//
                         "and tbprd.idlinea = tbprdlin.idlinea\n" + //
                         "and tbprdlin.lincod like '"+obj.get("lincod")+"%'\n" + //
                         "group by tbprd.prdcod,\n" + //
                         "tbprd.prdnom\n" ;

            JSONArray data = Dhm.query(consulta);
            System.out.println("respondio");
            obj.put("data", data);
            obj.put("estado", "exito");
        }catch(Exception e){
            obj.put("estado", "error");
            obj.put("error", e.getLocalizedMessage());
        }

    }

    public static void getPedidosProveedorClientes(JSONObject obj, SSSessionAbstract session) {
        try{

            String empCod = "";
            if(obj.has("empcod")){
                empCod = "and dm_cabfac.codvendedor = '"+obj.get("empcod")+"'\n";
            }
            
            String consulta = "SET DATEFORMAT 'YMD';\n"+
            "select  \n"+
            "    tbprdlin.lincod, \n"+
            "    sq1.clicod, \n"+
            "    sq1.clinom, \n"+
            "    sq1.idcli, \n"+
            "    sq1.productos, \n"+
            "    sq1.monto \n"+
            "from tbprdlin JOIN ( \n"+
            "    select  \n"+
            "    tbprdlin.lincod, \n"+
            "    dm_cabfac.clicod, \n"+
            "    tbcli.idcli, \n"+
            "    tbcli.clinom, \n"+
            "    sum(dm_detfac.vdcan) as productos, \n"+
            "    sum(dm_detfac.vdcan*dm_detfac.vdpre) as monto \n"+
            "    from  \n"+
            "    dm_cabfac  \n"+
            "    left join dm_detfac on  dm_cabfac.idven = dm_detfac.idven   \n"+
            "    LEFT JOIN tbprd ON dm_detfac.prdcod = tbprd.prdcod \n"+
            "    LEFT JOIN tbprdlin ON tbprd.idlinea = tbprdlin.idlinea \n"+
            "    LEFT JOIN tbcli ON dm_cabfac.clicod = tbcli.clicod \n"+
            "where dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n"+empCod+
            "     \n"+
            "--and tbprdlin.lincod like '05%' \n"+
            "    group by tbprdlin.lincod, 	dm_cabfac.clicod,	tbcli.clinom, tbcli.idcli \n"+
            " \n"+
            " \n"+
            " \n"+
            ") sq1 ON sq1.lincod like tbprdlin.lincod+'%' \n"+
            " where tbprdlin.linniv = 1 \n"+
            "and tbprdlin.lincod like '"+obj.get("lincod")+"%'\n" + //
            "--group by tbprdlin.lincod";
                 

            JSONArray data = Dhm.query(consulta);
            System.out.println("respondio");
            obj.put("data", data);
            obj.put("estado", "exito");
        }catch(Exception e){
            obj.put("estado", "error");
            obj.put("error", e.getLocalizedMessage());
        }

    }
    
    
    public static void getPedidosProveedor(JSONObject obj, SSSessionAbstract session) {

        String empCod = "";
        if(obj.has("empcod")){
            empCod = "and dm_cabfac.codvendedor = '"+obj.get("empcod")+"'\n";
        }

         String consulta = "\n" + //
                 "select tbprdlin.lincod,\n" + //
                 "tbprdlin.linnom,\n" + //
                 "COALESCE(resultado.clientes,0) as clientes,\n" +
                 "COALESCE(resultado.montos,0) as montos,\n" +
                 "COALESCE(resultado.productos,0) as productos\n" + //
                 "from (\n" + //
                 "select tabla1.cod,\n" + //
                 "sum(tabla1.productos) as productos,\n" + //
                 "sum(tabla1.clientes) as clientes,\n" + //
                 "sum(tabla1.montos) as montos\n" + //
                 "from (\n" + //
                 "select SUBSTRING(tabla.lincod, 0, 3) as cod,\n" + //
                 "tabla.productos,\n" + //
                 "tabla.clientes,\n" + //
                 "tabla.montos\n" + //
                 "from (\n" + //
                 "select tbprdlin.lincod,\n" + //
                 "(\n" + //
                 "select sum(dm_detfac.vdcan)\n" + //
                 "from tbprd,\n" + //
                 "dm_detfac,\n" + //
                 "dm_cabfac\n" + //
                 "where dm_detfac.prdcod = tbprd.prdcod\n" + empCod+//
                 "and tbprd.idlinea = tbprdlin.idlinea\n" + //
                 "and dm_cabfac.idven = dm_detfac.idven\n" + //
                 "and dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n"+
                 ") as productos,\n" + //
                 "(\n" + //
                 "select sum(dm_detfac.vdcan*dm_detfac.vdpre)\n" + //
                 "from tbprd,\n" + //
                 "dm_detfac,\n" + //
                 "dm_cabfac\n" + //
                 "where dm_detfac.prdcod = tbprd.prdcod\n" +empCod+ //
                 "and tbprd.idlinea = tbprdlin.idlinea\n" + //
                 "and dm_cabfac.idven = dm_detfac.idven\n" + //
                 "and dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n"+
                 ") as montos,\n" + //
                 "(\n" + //
                 "select count(tabla.clicod) as clicod\n" + //
                 "from (select dm_cabfac.clicod\n" + //
                 "from tbprd,\n" + //
                 "dm_detfac,\n" + //
                 "dm_cabfac\n" + //
                 "where dm_detfac.prdcod = tbprd.prdcod\n" + empCod+//
                 "and tbprd.idlinea = tbprdlin.idlinea\n" + //
                 "and dm_cabfac.idven = dm_detfac.idven\n" + //
                 "and dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n"+
                 "group by dm_cabfac.clicod) tabla\n" + //
                 ") as clientes\n" + //
                 "from tbprdlin\n" + //
                 ") tabla\n" + //
                 ") tabla1\n" + //
                 "group by tabla1.cod\n" + //
                 ") resultado,\n" + //
                 "tbprdlin\n" + //
                 "where tbprdlin.linniv = 1\n" + //
                 "and tbprdlin.lincod = resultado.cod";
                 

            JSONArray data = Dhm.query(consulta);
            System.out.println("respondio");
            obj.put("data", data);
            obj.put("estado", "exito");

    }

    public static void dashboardVendedor(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select \n"+
            "count(idcli) as clientes, \n"+
            "sum(case when tbcli.clilat is null or tbcli.clilat = 0 or tbcli.clilon is null or tbcli.clilon = 0 then 1 else 0 end) as clientes_sin_ubicacion, \n"+
            "sum(case when tbcli.clilat is not null and tbcli.clilat != 0 and tbcli.clilon is not null and tbcli.clilon != 0 then 1 else 0 end) as clientes_con_ubicacion, \n"+
            "idz  \n"+
            "from tbcli  \n"+
            "group by idz";

            JSONArray clientesZona = Dhm.query(consulta);
            JSONObject zonas = new JSONObject();
            String key;
            for (int i = 0; i < clientesZona.length(); i++) {
                key = clientesZona.getJSONObject(i).get("idz")+"";
                zonas.put(key, clientesZona.getJSONObject(i));
            }

            consulta = "select jsonb_object_agg(tabla.idemp, to_json(tabla.*))::json as json\n"+
            "from (\n"+
            "select  \n"+
            "visita_vendedor.idemp,\n"+
            "count(visita_vendedor.key) as visitas,\n"+
            "sum(case when tipo = 'REALIZO PEDIDO' then 1 else 0 end) as visitas_exitosas,\n" + //
            "sum(case when tipo != 'REALIZO PEDIDO' then 1 else 0 end) as visitas_fallidas\n"+
            "from visita_vendedor\n"+
            "where estado > 0\n"+
            "and fecha::date = '"+obj.getString("fecha")+"'\n"+
            "group by idemp\n"+
            ") tabla";
           JSONObject visitas = SPGConect.ejecutarConsultaObject(consulta);

           consulta = "select tbemp.idemp,\n" + //
                   "count(dm_cabfac.idven) as cantidad_ventas\n"+
                   "from dm_cabfac,\n" + //
                   " tbemp\n" + //
                   "where dm_cabfac.vfec = '"+obj.getString("fecha")+"'\n" + //
                   //"and dm_cabfac.vobs like '%SAPP%'\n" + //
                   "and dm_cabfac.codvendedor = tbemp.empcod\n" + //
                   "group by tbemp.idemp";
            JSONArray pedidos_ = Dhm.query(consulta);
            JSONObject objPedido = new JSONObject();
            JSONObject pedidos = new JSONObject();

            for (int i = 0; i < pedidos_.length(); i++) {
                objPedido = pedidos_.getJSONObject(i);
                pedidos.put(objPedido.get("idemp")+"", objPedido);
            }

            JSONObject vendedores = new JSONObject();

            
            consulta = "select idemp, empnom, empcod from tbemp";
            JSONArray vendedores_ = Dhm.query(consulta);
            for (int i = 0; i < vendedores_.length(); i++) {
                String idemp = vendedores_.getJSONObject(i).get("idemp")+"";
                vendedores.put(idemp, vendedores_.getJSONObject(i));
            }
        

           Calendar cal = new GregorianCalendar();
           SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");
           cal.setTime(formato.parse(obj.getString("fecha")));
           cal.setFirstDayOfWeek(Calendar.MONDAY);


            consulta = "select array_to_json(array_agg(tabla.*))::json as json\n"+
            "from (\n"+
            "select  \n"+
            "zona_empleado.idemp,\n"+
            "array_to_json(array_agg(zona_empleado.idz)) as zonas\n"+
            "from zona_empleado\n"+
            "where estado > 0\n"+
            "and dia = "+(cal.get(Calendar.DAY_OF_WEEK)-1)+"\n"+
            "group by idemp\n"+
            ") tabla";


            JSONObject empleados = new JSONObject();

            JSONArray zonas_empleado = SPGConect.ejecutarConsultaArray(consulta);
            JSONObject zona_empleado;
            
            for (int i = 0; i < zonas_empleado.length(); i++) {
                zona_empleado = zonas_empleado.getJSONObject(i);
                key = zona_empleado.get("idemp")+"";
                if(!empleados.has(key)){

                    JSONObject empdata = new JSONObject();

                    if(vendedores.has(key)){
                        empdata.put("empnom", vendedores.getJSONObject(key).getString("empnom"));
                        empdata.put("empcod", vendedores.getJSONObject(key).getString("empcod"));
                    }

                    empdata.put("cantidad_clientes", 0);
                    empdata.put("cantidad_zonas", 0);
                    if(visitas.has(key)){
                        empdata.put("cantidad_visitas", visitas.getJSONObject(key).getInt("visitas"));
                        empdata.put("visitas_exitosas", visitas.getJSONObject(key).getInt("visitas_exitosas"));
                        empdata.put("visitas_fallidas", visitas.getJSONObject(key).getInt("visitas_fallidas"));
                    }else{
                        empdata.put("cantidad_visitas", 0);
                        empdata.put("visitas_exitosas", 0);
                        empdata.put("visitas_fallidas", 0);
                    }
                    if(pedidos.has(key)){
                        empdata.put("cantidad_pedidos", pedidos.getJSONObject(key).getInt("cantidad_ventas"));
                        //empdata.put("cantidad_productos", pedidos.getJSONObject(key).getInt("cantidad_productos"));
                        //empdata.put("monto_pedido", pedidos.getJSONObject(key).getDouble("monto_pedido"));
                    }else{
                        empdata.put("cantidad_pedidos", 0);
                        //empdata.put("cantidad_productos", 0);
                        //empdata.put("monto_pedido", 0);
                    }
                    empdata.put("idemp", key);
                    empleados.put(key, empdata);
                }

                for (int j = 0; j < zona_empleado.getJSONArray("zonas").length(); j++) {
                    
                    int idz = zona_empleado.getJSONArray("zonas").getInt(j);
                    JSONObject vendedor = empleados.getJSONObject(key);

                    if(!zonas.has(idz+"")){
                        continue;
                    }

                    vendedor.put("cantidad_clientes", vendedor.getInt("cantidad_clientes")+zonas.getJSONObject(idz+"").getInt("clientes"));
                    vendedor.put("cantidad_zonas", vendedor.getInt("cantidad_zonas")+1);
                    if(!vendedor.has("clientes_sin_ubicacion")){
                        vendedor.put("clientes_sin_ubicacion", 0);
                    }
                    if(zonas.getJSONObject(idz+"").has("clientes_sin_ubicacion")){
                        vendedor.put("clientes_sin_ubicacion", vendedor.getInt("clientes_sin_ubicacion")+zonas.getJSONObject(idz+"").getInt("clientes_sin_ubicacion"));    
                    }
                    if(!vendedor.has("clientes_con_ubicacion")){
                        vendedor.put("clientes_con_ubicacion", 0);
                    }
                    if(zonas.getJSONObject(idz+"").has("clientes_con_ubicacion")){
                        vendedor.put("clientes_con_ubicacion", vendedor.getInt("clientes_con_ubicacion")+zonas.getJSONObject(idz+"").getInt("clientes_con_ubicacion"));    
                    }
                    
                }
            }

            System.out.println(empleados);

            obj.put("data", empleados);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void dashboardTransportista(JSONObject obj, SSSessionAbstract session) {
        try {

            

            String consulta = "SET DATEFORMAT 'YMD';\n" + //
                    "SELECT tbtg.idemp,  \n" + //
                    "tbemp.empnom, \n" + //
                    "tbemp.empcod, \n" + //
                    "count(distinct emp.idemp) as cantidad_vendedores,\n" + //
                    "count(tbven.idven) as total_pedidos,\n" + //
                    "sum(isnull(vr.TimpR,0)) as monto_productos,\n" + //
                    "sum(isnull(vr.Tcanven,0)) as cantidad_productos\n" + //
                    "\n" + //
                    "--sum(vr.Tcanven) as cantidad_productos \n" + //
                    "FROM tbven LEFT JOIN tbtg ON tbven.idtg=tbtg.idtg \n" + //
                    "LEFT JOIN tbemp emp ON tbven.idemp=emp.idemp \n" + //
                    "LEFT JOIN tbemp ON tbtg.idemp=tbemp.idemp \n" + //
                    "LEFT JOIN tbcli ON tbven.idcli=tbcli.idcli \n" + //
                    "LEFT JOIN ( \n" + //
                    "\tSELECT tbven.idven, \n" + //
                    "\tsum(vdimp) AS TimpR, \n" + //
                    "\tsum(vdcan) AS Tcanven \n" + //
                    "\tFROM tbven, tbvd\n" + //
                    "\tWHERE tbven.idven=tbvd.idven \n" + //
                    "\tAND vfec='"+obj.getString("fecha")+"'  \n" + //
                    "\tgroup by tbven.idven, tbven.vtipp\n" + //
                    ") AS VR ON tbven.idven=vr.idven \n" + //
                    "\n" + //
                    "WHERE  tbtg.tgfec='"+obj.getString("fecha")+"'\n" + //
                    "group by tbtg.idemp ,tbemp.empnom, tbemp.empcod";

            JSONArray pedidos = Dhm.query(consulta);

            consulta = "select jsonb_object_agg(tabla.idemp, to_json(tabla.*))::json as json\n"+
            "from (\n"+
            "select  \n"+
            "visita_transportista.idemp,\n"+
            "count(visita_transportista.key) as visitas,\n"+
            "sum(case when tipo = 'ENTREGADO' then monto else 0 end) as monto_visitas_exitosas,\n" + //
            "sum(case when tipo != 'ENTREGADO' then monto else 0 end) as monto_visitas_fallidas,\n" + //
            "sum(case when tipo = 'ENTREGADO' then 1 else 0 end) as visitas_exitosas,\n" + //
            "sum(case when tipo != 'ENTREGADO' then 1 else 0 end) as visitas_fallidas\n"+
            "from visita_transportista\n"+
            "where estado > 0\n"+
            "and fecha::date = '"+obj.getString("fecha")+"'\n"+
            "group by idemp\n"+
            ") tabla";
           JSONObject visitas = SPGConect.ejecutarConsultaObject(consulta);

           for (int i = 0; i < pedidos.length(); i++) {
                JSONObject transportista = pedidos.getJSONObject(i);
                if(visitas.has(transportista.get("idemp")+"")){
                    JSONObject visita = visitas.getJSONObject(transportista.get("idemp")+"");
                    transportista.put("visitas", visita.get("visitas"));
                    transportista.put("visitas_exitosas", visita.get("visitas_exitosas"));
                    transportista.put("visitas_fallidas", visita.get("visitas_fallidas"));
                    transportista.put("monto_visitas_exitosas", visita.get("monto_visitas_exitosas"));
                    transportista.put("monto_visitas_fallidas", visita.get("monto_visitas_fallidas"));
                }else{
                    transportista.put("visitas", 0);
                    transportista.put("visitas_exitosas", 0);
                    transportista.put("visitas_fallidas", 0);
                    transportista.put("monto_visitas_exitosas", 0);
                    transportista.put("monto_visitas_fallidas", 0);
                }
           }

            obj.put("data", pedidos);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void perfilEmp(JSONObject obj, SSSessionAbstract session) {
        try {

            Calendar cal = new GregorianCalendar();
            SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");
            cal.setTime(formato.parse(obj.getString("fecha_fin")));
            cal.setFirstDayOfWeek(Calendar.MONDAY);


            String consulta = "select array_to_json(array_agg(tabla.idz))::json as json\n"+
            "from (\n"+
            "select  \n"+
            "zona_empleado.*\n"+
            "from zona_empleado\n"+
            "where estado > 0\n"+
            "and dia = "+(cal.get(Calendar.DAY_OF_WEEK)-1)+"\n"+
            "and idemp = "+obj.get("idemp")+"\n"+
            ") tabla";

            JSONArray zonas_empleado = SPGConect.ejecutarConsultaArray(consulta);
            
            String idzs = zonas_empleado.toString().substring(1,zonas_empleado.toString().length()-1);


            consulta = "" +

                    "select tbemp.idemp, " +
                    "( " +
                    "    select count(tbcli.idcli) " +
                    "    from tbcli " +
                    "    where idz in ("+idzs+")" +
                    ") as  cantidad_clientes, " +
                    "( " +
                    "    select count(tbzon.idz) " +
                    "    from tbzon " +
                    "    where tbzon.idz in ("+idzs+") " +
                    ") as  cantidad_zonas, " +
                    "( " +
                    "    select count(dm_cabfac.idven)  as cant" +
                    "    from dm_cabfac  " +
                    "    where  dm_cabfac.codvendedor = tbemp.empcod " +
                    "    and dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" +
                    ") as  cantidad_pedidos, \n"+
                    "( " +
                    "    select sum(dm_detfac.vdpre*dm_detfac.vdcan) as cant" +
                    "    from dm_cabfac, dm_detfac  " +
                    "    where  dm_cabfac.codvendedor = tbemp.empcod " +
                    "    and  dm_cabfac.idven = dm_detfac.idven " +
                    "    and dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n" +
                    ") as  monto_pedidos ";

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
                    //"and tbtg.tgest = 'DESPACHADO' \n"+
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
                    //"and tbtg.tgest = 'DESPACHADO' \n"+
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
                    //"and tbtg.tgest = 'DESPACHADO' \n"+
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
