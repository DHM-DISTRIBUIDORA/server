package Component;

import java.util.Date;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SPGConect;
import Servisofts.SUtil;

public class DmCabFac {
    public static final String COMPONENT = "dm_cabfac";
    public static final String PK = "idven";


    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        switch (obj.getString("type")) {
            case "getAll":
                getAll(obj, session);
                break;
            case "getPedidosVendedor":
                getPedidosVendedor(obj, session);
                break;
            case "getPedidosVendedorDetalle":
                getPedidosVendedorDetalle(obj, session);
                break;
            case "getByKey":
                getByKey(obj, session);
                break;
            case "registro":
                registro(obj, session);
                break;
            case "uploadChanges":
                uploadChanges(obj, session);
                break;
            case "save":
                save(obj, session);
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
            case "getAllPedidosCliente":
                getAllPedidosCliente(obj, session);
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

            String consulta = "select dm_cabfac.*\n" + //
                    "from dm_cabfac,\n" + //
                    "tbemp\n" + //
                    "where tbemp.empcod = dm_cabfac.codvendedor\n" + //
                    "and dm_cabfac.idven not in (\n"+
	                "select dm_cabfac.idven\n"+
                    "from dm_cabfac,\n"+
                    "tbven\n"+
                    "where tbven.idpeddm = dm_cabfac.idven)\n"+
                    "and dm_cabfac.vfec = '"+obj.get("fecha")+"'\n"+
                    "order by idven desc";
            
            if(obj.has("idcli") && !obj.isNull("idcli")){
                consulta = "select dm_cabfac.*\n" + //
                    "from dm_cabfac,\n" + //
                    "tbcli\n" + //
                    "where tbcli.clicod = dm_cabfac.clicod\n" + //
                    "and tbcli.idcli = "+obj.get("idcli")+"\n"+ 
                    "and dm_cabfac.idven not in (\n"+
	                "select dm_cabfac.idven\n"+
                    "from dm_cabfac,\n"+
                    "tbven\n"+
                    "where tbven.idpeddm = dm_cabfac.idven)\n"+
                    "and dm_cabfac.vfec = '"+obj.get("fecha")+"'\n"+
                    "order by idven desc";
            }
            
            if(obj.has("idemp") && !obj.isNull("idemp")){
                consulta = "select dm_cabfac.*\n" + //
                    "from dm_cabfac,\n" + //
                    "tbemp\n" + //
                    "where tbemp.empcod = dm_cabfac.codvendedor\n" + //
                    "and tbemp.idemp = "+obj.get("idemp")+"\n"+ 
                    "and dm_cabfac.vfec = '"+obj.get("fecha")+"'\n"+
                    //"and dm_cabfac.idven not in (\n"+
	                //"select dm_cabfac.idven\n"+
                    //"from dm_cabfac,\n"+
                    //"tbven\n"+
                    //"where tbven.idpeddm = dm_cabfac.idven)\n"+
                    "order by idven desc";
            }

            

            JSONArray dm_cabfac = Dhm.query(consulta);

            String idVentas = "";
            for (int i = 0; i < dm_cabfac.length(); i++) {
                if(dm_cabfac.getJSONObject(i).has("idven")){
                    idVentas+="'"+dm_cabfac.getJSONObject(i).get("idven")+"',";
                }
            }
            idVentas=idVentas.length()>0?idVentas.substring(0, idVentas.length()-1):"";

            JSONObject ventas = new JSONObject();
            if(idVentas.length()>0){
                consulta = "select * from dm_detfac where idven in ("+idVentas+") order by idven desc";
                JSONArray dm_detfac = Dhm.query(consulta);

                //buscador detalle 
                ventas = agregarVenta(dm_cabfac, dm_detfac);
            }
            
        

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

    public static void getPedidosVendedor(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select tbemp.empnom, tbemp.empcod, tbemp.idemp,\n" + //
                    "sum(case when dm_cabfac.vobs like '%SAPP%' then 1 else 0 end) cantidad_ss,\n" + //
                    "sum(case when dm_cabfac.vobs not like '%SAPP%' then 1 else 0 end) cantidad_otros,\n" + //
                    "count(dm_cabfac.idven) cantidad,\n" + //
                    "max(cast(dm_cabfac.vfec as DATEtime)+cast(dm_cabfac.vhora as TIME)) fecha_ultimo,\n" + //
                    "min(cast(dm_cabfac.vfec as DATEtime)+cast(dm_cabfac.vhora as TIME)) fecha_primero\n" + //
                    "from dm_cabfac,\n" + //
                    " tbemp\n" + //
                    "where dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and  '"+obj.getString("fecha_fin")+"'\n" + //
                    "and dm_cabfac.codvendedor = tbemp.empcod\n" + //
                    "group by tbemp.empnom, tbemp.empcod, tbemp.idemp";
            obj.put("data", Dhm.query(consulta));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getPedidosVendedorDetalle(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select dm_cabfac.*\n" + //
                    "from dm_cabfac, tbemp\n" + //
                    "where dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and  '"+obj.getString("fecha_fin")+"'\n" + //
                    "and tbemp.idemp = "+obj.get("idemp")+"\n" + //
                    "and dm_cabfac.codvendedor = tbemp.empcod\n";
                    
            obj.put("data", Dhm.query(consulta));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getAllPedidosCliente(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select dm_cabfac.*,\n" + //
                    "(select sum(dm_detfac.vdcan) from dm_detfac where dm_detfac.idven = dm_cabfac.idven) cantidad,\n" + //
                    "tbven.vdet,\n" + //
                    "tbven.idven as idpeddm,\n" + //
                    "(select sum(dm_detfac.vdcan*dm_detfac.vdpre) from dm_detfac where dm_detfac.idven = dm_cabfac.idven) monto\n" + //
                    "from dm_cabfac, tbven\n" + //
                    "where dm_cabfac.clicod = '"+obj.get("clicod")+"'\n" + //
                    "and dm_cabfac.idven = tbven.idpeddm\n" + //
                    "\n";

            obj.put("data", Dhm.query(consulta));

             consulta = "select tbcli.idcli\n" + //
                    "from tbcli\n" + //
                    "where tbcli.clicod = '"+obj.get("clicod")+"'\n";

            JSONArray idvens  = Dhm.query(consulta);
            int idcli = 0;
            
            if(idvens.length()>0) idcli = idvens.getJSONObject(0).getInt("idcli");
            
            if(idcli > 0){

                consulta = "select jsonb_object_agg(visita_transportista.idven, to_json(visita_transportista.*))::json as json from visita_transportista where idcli ='"+idcli+"'";
                obj.put("visitas", SPGConect.ejecutarConsultaObject(consulta));
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

    public static void uploadChanges(JSONObject obj, SSSessionAbstract session) {
        try {
            //insertando datos nuevos
            
            if(obj.has("insert") && !obj.isNull("insert") && obj.getJSONArray("insert").length()>0){

                JSONObject dm_cabfac;
                JSONArray arr = Dhm.query("select max(idven) as idven from dm_cabfac");
                int idven = arr.getJSONObject(0).getInt("idven");

                String consulta = "SET DATEFORMAT 'YMD';  select dm_cabfac.vhora\n" + //
                        "from dm_cabfac\n" + //
                        "where dm_cabfac.vfec = '"+obj.getJSONArray("insert").getJSONObject(0).getString("vfec").substring(0,10)+"'\n" + //
                        "and dm_cabfac.codvendedor = '"+obj.getJSONArray("insert").getJSONObject(0).getString("codvendedor")+"'";

                JSONArray hoy = Dhm.query(consulta);

                HashMap<String, Boolean> pedidos = new HashMap<String,Boolean>();
                for (int i = 0; i < hoy.length(); i++) {
                    pedidos.put(hoy.getJSONObject(i).get("vhora")+"", true);
                }

                consulta = "SET DATEFORMAT 'YMD'; \n";

                for (int i = 0; i < obj.getJSONArray("insert").length(); i++) {
                    dm_cabfac = obj.getJSONArray("insert").getJSONObject(i);

                    if(pedidos.get(dm_cabfac.get("vhora")+"") != null){
                        System.out.println("*******//////////////********** Pedido duplicado");
                        continue;
                    }
                    System.out.println("*******//////////////********** Pedido EXITOSO");
                    pedidos.put(dm_cabfac.get("vhora")+"", true);

                    idven++;
                    System.out.println("Pedido # "+(i+1)+" idven:"+idven); 

                    obj.getJSONArray("insert").getJSONObject(i).put("idven", idven);
                    
                    consulta += "insert into dm_cabfac (vlongitud,vhora,vlatitud,direccion,vtipa,vzona,clicod,vdes,idven,codvendedor,razonsocial,vpla,nit,tipocliente,vfec,telefonos,vobs,nombrecliente,vtipo)";
                    consulta += " values ";
                    consulta += " (0, '"+dm_cabfac.get("vhora")+"',0,'"+(dm_cabfac.has("direccion")?dm_cabfac.get("direccion"):"")+"',0,'"+dm_cabfac.get("vzona")+"','"+dm_cabfac.get("clicod")+"',0,"+idven+",'"+dm_cabfac.get("codvendedor")+"','"+dm_cabfac.get("razonsocial")+"',0,'"+dm_cabfac.get("nit")+"','"+dm_cabfac.get("tipocliente")+"','"+dm_cabfac.get("vfec")+"','"+(dm_cabfac.has("telefonos")?dm_cabfac.get("telefonos"):"")+"','"+dm_cabfac.get("vobs")+"','"+dm_cabfac.get("nombrecliente")+"',1);\n";
                    
                    if(dm_cabfac.has("detalle") && !dm_cabfac.isNull("detalle")){    
                        JSONObject detalle;
                        
                        for (int j = 0; j < dm_cabfac.getJSONArray("detalle").length(); j++) {
                            detalle = dm_cabfac.getJSONArray("detalle").getJSONObject(j);
                            System.out.println("Pedido # "+(i+1)+" idven:"+idven+" detalle ///// "+(j+1));
                            consulta += "insert into dm_detfac (idven,prdcod,vddesc,vdpre,vdcan) values ("+idven+",'"+detalle.get("prdcod")+"',0,"+detalle.getDouble("vdpre")+","+detalle.getInt("vdcan")+");\n";    
                        }
                    }
                }

                
                Dhm.query(consulta);

                //System.out.println("registrando dm_cabfac .. "+new Date());
                //Dhm.registroAll("dm_cabfac", "", obj.getJSONArray("insert"));
                //System.out.println("registrando  dm_detfac"+new Date());
                //Dhm.registroAll("dm_detfac", "", dm_detfac_arr);
                //System.out.println("finnalizando "+new Date());

                obj.put("insert", "exito");
            }


            // Editar
            if(obj.has("update") && !obj.isNull("update") && obj.getJSONArray("update").length()>0){
                JSONArray dm_detfac_arr = new JSONArray();
                JSONArray dm_detfac_arr_obj = new JSONArray();
                
                for (int i = 0; i < obj.getJSONArray("update").length(); i++) {

                    if(obj.getJSONArray("update").getJSONObject(i).has("detalle") && !obj.getJSONArray("update").getJSONObject(i).isNull("detalle")){    
                        for (int j = 0; j < obj.getJSONArray("update").getJSONObject(i).getJSONArray("detalle").length(); j++) {
                            dm_detfac_arr.put(obj.getJSONArray("update").getJSONObject(i).getJSONArray("detalle").getJSONObject(j).get("idven"));
                            dm_detfac_arr_obj.put(obj.getJSONArray("update").getJSONObject(i).getJSONArray("detalle").getJSONObject(j));
                        }
                    }
                }

                Dhm.editarAll("dm_cabfac", "idven", obj.getJSONArray("update"));
                Dhm.eliminarAll("dm_detfac", "idven", dm_detfac_arr);
                Dhm.registroAll("dm_detfac", "id", dm_detfac_arr_obj);
                
            }


            // Eliminar
            if(obj.has("delete") && !obj.isNull("delete") && obj.getJSONArray("delete").length()>0){
                JSONArray del_dm_cabfac = new JSONArray();
                JSONArray del_dm_detfac = new JSONArray();

                for (int i = 0; i < obj.getJSONArray("delete").length(); i++) {
                    
                    del_dm_cabfac.put(obj.getJSONArray("delete").getJSONObject(i).get("idven"));

                    if(obj.getJSONArray("delete").getJSONObject(i).has("detalle") && !obj.getJSONArray("delete").getJSONObject(i).isNull("detalle")){    
                        for (int j = 0; j < obj.getJSONArray("delete").getJSONObject(i).getJSONArray("detalle").length(); j++) {
                            del_dm_detfac.put(obj.getJSONArray("delete").getJSONObject(i).getJSONArray("detalle").get(j));
                        }
                    }
                }

                Dhm.eliminarAll("dm_cabfac", "idven", del_dm_cabfac);
                Dhm.eliminarAll("dm_detfac", "idven", del_dm_detfac);                
                
            }
            
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void save(JSONObject obj, SSSessionAbstract session) {
        try {
            
            if(!obj.has("data")){
                obj.put("estado", "error");
                obj.put("error", "No existe data");
                return;
            }
            if(!obj.getJSONObject("data").has("sync_type")){
                obj.put("estado", "error");
                obj.put("error", "No existe event_type");
                return;
            }
            switch(obj.getJSONObject("data").getString("sync_type")){
                case "insert":{
                    JSONObject dm_cabfac;
                    JSONArray arr = Dhm.query("select max(CONVERT(int,idven)) as idven from dm_cabfac");
                    int idven = arr.getJSONObject(0).getInt("idven");

                    String consulta = "select to_json(historico_clicod.*) as json\n" + //
                            "from historico_clicod\n" + //
                            "where historico_clicod.clicod_old = '"+obj.getJSONObject("data").getString("clicod")+"'";

                    JSONObject historico_clicod = SPGConect.ejecutarConsultaObject(consulta);
                    if(historico_clicod!= null &&  !historico_clicod.isEmpty()){
                        obj.getJSONObject("data").put("clicod", historico_clicod.getString("clicod_new"));
                    }

                    consulta = "SET DATEFORMAT 'YMD'; \n";
                    dm_cabfac = obj.getJSONObject("data");

                    System.out.println("*******//////////////********** Pedido EXITOSO");
                    idven++;

                    dm_cabfac.put("idven", idven+"");
                    consulta += "insert into dm_cabfac (vlongitud,vhora,vlatitud,direccion,vtipa,vzona,clicod,vdes,idven,codvendedor,razonsocial,vpla,nit,tipocliente,vfec,telefonos,vobs,nombrecliente,vtipo)";
                    consulta += " values ";
                    consulta += " ("+dm_cabfac.get("vlongitud")+", '"+dm_cabfac.get("vhora")+"',"+dm_cabfac.get("vlatitud")+",'"+(dm_cabfac.has("direccion")?dm_cabfac.get("direccion"):"")+"',0,'"+dm_cabfac.get("vzona")+"','"+dm_cabfac.get("clicod")+"',0,"+idven+",'"+dm_cabfac.get("codvendedor")+"','"+dm_cabfac.get("razonsocial")+"',0,'"+dm_cabfac.get("nit")+"','"+dm_cabfac.get("tipocliente")+"','"+dm_cabfac.get("vfec")+"','"+(dm_cabfac.has("telefonos")?dm_cabfac.get("telefonos"):"")+"','"+dm_cabfac.get("vobs")+"','"+dm_cabfac.get("nombrecliente")+"',1);\n";
                    
                    JSONArray cab = Dhm.query(consulta);
                    if(cab == null){
                        throw new Exception("Error al insertar el dm_cabfac");
                    }

                    consulta = "";

                    if(dm_cabfac.has("detalle") && !dm_cabfac.isNull("detalle")){    
                        JSONObject detalle;
                        
                        for (int j = 0; j < dm_cabfac.getJSONArray("detalle").length(); j++) {
                            detalle = dm_cabfac.getJSONArray("detalle").getJSONObject(j);
                            System.out.println("Pedido # "+(j+1)+" idven:"+idven+" detalle ///// "+(j+1));
                            consulta += "insert into dm_detfac (idven,prdcod,vddesc,vdpre,vdcan) values ('"+idven+"','"+detalle.get("prdcod")+"',0,"+detalle.getDouble("vdpre")+","+detalle.getInt("vdcan")+");\n";    
                        }
                    }

                    Dhm.query(consulta);

                    //System.out.println("registrando dm_cabfac .. "+new Date());
                    //Dhm.registroAll("dm_cabfac", "", obj.getJSONArray("insert"));
                    //System.out.println("registrando  dm_detfac"+new Date());
                    //Dhm.registroAll("dm_detfac", "", dm_detfac_arr);
                    //System.out.println("finnalizando "+new Date());

                    obj.getJSONObject("data").remove("sync_type");
                    obj.put("data", obj.getJSONObject("data"));
                    obj.put("estado", "exito");
                    return;
                }
                case "update":{
                    JSONArray dm_detfac_arr = new JSONArray();
                    JSONArray dm_detfac_arr_obj = new JSONArray();
                    
                    if(obj.getJSONObject("data").has("detalle") && !obj.getJSONObject("data").isNull("detalle")){    
                        for (int j = 0; j < obj.getJSONObject("data").getJSONArray("detalle").length(); j++) {
                            String idven = obj.getJSONObject("data").get("idven")+"";
                            
                            dm_detfac_arr.put(idven);
                            dm_detfac_arr_obj.put(obj.getJSONObject("data").getJSONArray("detalle").getJSONObject(j).put("idven", idven));
                        }
                    }
                

                    Dhm.editar("dm_cabfac", "idven", obj.getJSONObject("data"));
                    Dhm.eliminarAll("dm_detfac", "idven", dm_detfac_arr);
                    Dhm.registroAll("dm_detfac", "id", dm_detfac_arr_obj);
                    
                    obj.getJSONObject("data").remove("sync_type");
                    obj.put("data", obj.getJSONObject("data"));
                    obj.put("estado", "exito");
                    return;
                }
                case "delete":{
                    JSONArray del_dm_cabfac = new JSONArray();
                    JSONArray del_dm_detfac = new JSONArray();

                    
                        
                    del_dm_cabfac.put(obj.getJSONObject("data").get("idven"));

                    if(obj.getJSONObject("data").has("detalle") && !obj.getJSONObject("data").has("detalle")){    
                        for (int j = 0; j < obj.getJSONObject("data").getJSONArray("detalle").length(); j++) {
                            del_dm_detfac.put(obj.getJSONObject("data").getJSONArray("detalle").get(j));
                        }
                    }
                

                    Dhm.eliminarAll("dm_cabfac", "idven", del_dm_cabfac);
                    Dhm.eliminarAll("dm_detfac", "idven", del_dm_detfac);                
                    
                    obj.getJSONObject("data").remove("sync_type");
                    obj.put("data", obj.getJSONObject("data"));
                    obj.put("estado", "exito");
                    return;
                }
            }
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
                    "\ttbemp.idemp,\n" + //        
                    "\ttbemp.empcod,\n" + //
                    "\ttbemp.empnom,\n" + //
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

            JSONObject tags = new JSONObject();

            new Notification().sendTags(
                "DHM-Distribuidora",
                "Realizaste exitosamente el pedido   # "+ idven+" a " + dataPedido.getString("clicod") + " " + dataPedido.getString("clinom") ,
                "https://dhm.servisofts.com/dm_cabfac/recibo?pk=" + idven,
                "dhm://app/dm_cabfac/recibo?pk=" + idven, 
                new JSONObject().put("idvendedor", dataPedido.get("idemp")));

            new Notification().sendTags(
                "DHM-Distribuidora",
                "El empleado " + dataPedido.getString("empnom") + " realizó un pedido  exitosamente, # "+ idven,
                "https://dhm.servisofts.com/dm_cabfac/recibo?pk=" + idven,
                "dhm://app/dm_cabfac/recibo?pk=" + idven, 
                new JSONObject().put("idcli", data.get("idcli")));

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
