package Component;

import java.util.Date;
import java.util.UUID;

import org.json.JSONArray;
import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SConfig;
import Servisofts.SPGConect;
import Servisofts.SUtil;

public class TbCli {
    public static final String COMPONENT = "tbcli";
    public static final String PK = "idcli";
    public static final String selecttbcli = " idcli,clicod,clinom,cliape, clirazon, clidir,clicel,clinit,clitel,clifax, cliemail,tbcli.idz,idcat,usumod ,fecmod,clilat,clilon ";

    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        switch (obj.getString("type")) {
            case "getAll":
                getAll(obj, session);
                break;
            case "getByKeys":
                getByKeys(obj, session);
                break;
            case "getByCliCod":
                getByCliCod(obj, session);
                break;
            case "getByKey":
                getByKey(obj, session);
                break;
            case "getByCode":
                getByCode(obj, session);
                break;
            case "uploadChanges":
                uploadChanges(obj, session);
                break;
            case "save":
                save(obj, session);
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
            case "validarTelefono":
                validarTelefono(obj, session);
                break;
            case "getAllPedidos":
                getAllPedidos(obj, session);
                break;
            case "getClientesDia":
                getClientesDia(obj, session);
                break;
        }
    }

    public static void getAllPedidos(JSONObject obj, SSSessionAbstract session) {
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

    public static void getClientesDia(JSONObject obj, SSSessionAbstract session) {
        try {

            String consulta = "select tbcli.* \n" + //
                    "            from tbzon,\n" + //
                    "            tbcli\n" + //
                    "            where tbzon.idemp = " + obj.get("idemp") + " \n" + //
                    "            and tbzon.zdia = " + obj.get("dia") + "\n" + //
                    "            and tbcli.idz = tbzon.idz";
            obj.put("data", Dhm.query(consulta));

            JSONObject visitas = VisitaVendedor.getVisitas(obj.get("idemp") + "", obj.get("fecha") + "");
            obj.put("visitas", visitas);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void uploadChanges(JSONObject obj, SSSessionAbstract session) {
        try {
            // insertando datos nuevos

            if (obj.has("insert") && !obj.isNull("insert") && obj.getJSONArray("insert").length() > 0) {

                JSONObject cantCliZonas = new JSONObject();

                JSONArray arr = Dhm.query("select max(" + PK + ") as id from " + COMPONENT);
                int id = arr.getJSONObject(0).getInt("id");
                JSONObject tbcli;

                for (int i = 0; i < obj.getJSONArray("insert").length(); i++) {
                    id++;
                    tbcli = obj.getJSONArray("insert").getJSONObject(i);
                    tbcli.put(PK, id);
                    if (tbcli.has("idz") && !tbcli.isNull("idz")) {

                        if (!cantCliZonas.has(tbcli.get("idz") + "") && cantCliZonas.isNull(tbcli.get("idz") + "")) {

                            String consulta = "select tabla.cant,\n" + //
                                    " tabla.znom\n" + //
                                    "from (\n" + //
                                    "select tbzon.znom,\n" + //
                                    "count(idcli) as cant\n" + //
                                    "from tbcli,\n" + //
                                    "tbzon\n" + //
                                    "where tbzon.idz = " + tbcli.get("idz") + "\n" + //
                                    "and tbzon.idz = tbcli.idz\n" + //
                                    "group by tbzon.znom\n" + //
                                    ") tabla";
                            JSONArray clicods = Dhm.query(consulta);

                            cantCliZonas.put(tbcli.get("idz") + "", clicods.getJSONObject(0));
                        }

                        cantCliZonas.getJSONObject(tbcli.get("idz") + "").put("cant", cantCliZonas.getJSONObject(tbcli.get("idz") + "").getInt("cant") + 1);

                        if(cantCliZonas.getJSONObject(tbcli.get("idz") + "").has("znom")){
                            tbcli.put("clicod", cantCliZonas.getJSONObject(tbcli.get("idz") + "").getString("znom") + " - "
                                + cantCliZonas.getJSONObject(tbcli.get("idz") + "").getInt("cant") + 1);
                        }else{
                            tbcli.put("clicod", "_" + " - "
                                + cantCliZonas.getJSONObject(tbcli.get("idz") + "").getInt("cant") + 1);
                        }
                        
                    }
                }
                Dhm.registroAll(COMPONENT, PK, obj.getJSONArray("insert"));
            }

            // Editar
            if (obj.has("update") && !obj.isNull("update") && obj.getJSONArray("update").length() > 0) {
                Dhm.editarAll(COMPONENT, PK, obj.getJSONArray("update"));
            }

            // Eliminar
            if (obj.has("delete") && !obj.isNull("delete") && obj.getJSONArray("delete").length() > 0) {
                JSONArray del_ = new JSONArray();
                for (int i = 0; i < obj.getJSONArray("delete").length(); i++) {
                    del_.put(obj.getJSONArray("delete").getJSONObject(i).get(PK));
                }
                Dhm.eliminarAll(COMPONENT, PK, del_);
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
            // insertando datos nuevos

            if (obj.has("data") && !obj.isNull("data")) {

                switch(obj.getJSONObject("data").getString("sync_type")){
                    case "insert":{

                        if(obj.has("key_usuario")){
                            JSONObject historico_cli_edit = new JSONObject();
                            historico_cli_edit.put("key", SUtil.uuid());
                            historico_cli_edit.put("fecha_on", SUtil.now());
                            historico_cli_edit.put("estado", 1);
                            historico_cli_edit.put("key_usuario", obj.getString("key_usuario"));
                            
                            historico_cli_edit.put("idvendedor", obj.get("idvendedor"));
                            historico_cli_edit.put("idtransportista", obj.get("idtransportista"));

                            historico_cli_edit.put("tipo", "insert");
                            historico_cli_edit.put("data", obj.getJSONObject("data"));
                            try {
                                SPGConect.insertArray("historico_cli_edit", new JSONArray().put(historico_cli_edit));
                              } catch (Exception e) {
                                 e.printStackTrace();
                              }
                            
                        }

                        JSONObject cantCliZonas = new JSONObject();
                        JSONArray arr = Dhm.query("select max(" + PK + ") as id from " + COMPONENT);
                        int id = arr.getJSONObject(0).getInt("id");
                        JSONObject tbcli = obj.getJSONObject("data");
                        id++;
                        tbcli.put(PK, id);
                        tbcli.put("clitipdoc", "Documento");
                        tbcli.put("cliforpag", "Contado");

                        String consulta = "select tabla.cant,\n" + //
                                " tabla.znom\n" + //
                                "from (\n" + //
                                "select tbzon.znom,\n" + //
                                "count(idcli) as cant\n" + //
                                "from tbcli,\n" + //
                                "tbzon\n" + //
                                "where tbzon.idz = " + tbcli.get("idz") + "\n" + //
                                "and tbzon.idz = tbcli.idz\n" + //
                                "group by tbzon.znom\n" + //
                                ") tabla";
                        JSONArray clicods = Dhm.query(consulta);

                        cantCliZonas.put(tbcli.get("idz") + "", clicods.getJSONObject(0));
                    

                        cantCliZonas.getJSONObject(tbcli.get("idz") + "").put("cant", cantCliZonas.getJSONObject(tbcli.get("idz") + "").getInt("cant") + 1);
                        String znom = "nozone";

                        String old_clicod = tbcli.getString("clicod");

                        JSONObject historico_clicod = SPGConect.ejecutarConsultaObject("select to_json(historico_clicod.*) as json from historico_clicod where historico_clicod.clicod_old = '"+old_clicod+"'");

                        if(historico_clicod!= null && !historico_clicod.isEmpty()){
                            obj.put("estado", "exito");

                            obj.getJSONObject("data").put("clicod", historico_clicod.getString("clicod_new"));
                            Dhm.editar(COMPONENT, PK, obj.getJSONObject("data"));

                            obj.put("data", obj.getJSONObject("data"));
                            return;
                        }

                        try {
                            znom = cantCliZonas.getJSONObject(tbcli.get("idz") + "").getString("znom");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        tbcli.put("clicod",  znom+ " - " + (cantCliZonas.getJSONObject(tbcli.get("idz") + "").getInt("cant") + 1));

                        String new_clicod = tbcli.getString("clicod");

                        Dhm.registro(COMPONENT, PK, tbcli);

                        historico_clicod = new JSONObject();
                        historico_clicod.put("key", UUID.randomUUID());
                        historico_clicod.put("clicod_old", old_clicod);
                        historico_clicod.put("clicod_new", new_clicod);
                        historico_clicod.put("fecha_on", SUtil.formatTimestamp(new Date()));
                        SPGConect.insertObject("historico_clicod", historico_clicod);

                        obj.getJSONObject("data").remove("sync_type");
                        obj.put("estado", "exito");
                        obj.put("data", obj.getJSONObject("data"));
                        return;
                    }
                    case "update":{
                        
                        if(obj.has("key_usuario")){
                            JSONObject historico_cli_edit = new JSONObject();
                            historico_cli_edit.put("key", SUtil.uuid());
                            historico_cli_edit.put("fecha_on", SUtil.now());
                            historico_cli_edit.put("estado", 1);
                            historico_cli_edit.put("key_usuario", obj.getString("key_usuario"));
                            
                            historico_cli_edit.put("idvendedor", obj.get("idvendedor"));
                            historico_cli_edit.put("idtransportista", obj.get("idtransportista"));

                            historico_cli_edit.put("tipo", "edit");
                            historico_cli_edit.put("data", obj.getJSONObject("data"));
                            try {
                                System.out.println(historico_cli_edit.toString());
                              SPGConect.insertArray("historico_cli_edit", new JSONArray().put(historico_cli_edit));
                            } catch (Exception e) {
                               e.printStackTrace();
                            }
                        }

                        /*
                         * WARNING
                         * Desde la app es posible que nos está llegando el update para el cliente 001230351654 que es un cliente temporal en
                         * la base de datos del celular pero el cliente ya se registró y se le cambió el código por esto no lo va a encontrar.
                         * 
                         * Para evitar este problema verificamos si el codigo del cliente se encuentra en la base de datos de historico y obtenemos el nuevo.
                         */

                        JSONObject historico_clicod = SPGConect.ejecutarConsultaObject("select to_json(historico_clicod.*) as json from historico_clicod where historico_clicod.clicod_old = '"+obj.getJSONObject("data").get("clicod")+"'");

                        if(historico_clicod!=null && !historico_clicod.isEmpty()){
                            obj.getJSONObject("data").put("clicod", historico_clicod.getString("clicod_new"));
                        }
                        

                        Dhm.editar(COMPONENT, PK, obj.getJSONObject("data"));
                        obj.getJSONObject("data").remove("sync_type");
                        obj.put("estado", "exito");
                        obj.put("data", obj.getJSONObject("data"));
                        return;
                    }
                    case "delete":{
                        if(obj.has("key_usuario")){
                            JSONObject historico_cli_edit = new JSONObject();
                            historico_cli_edit.put("key", SUtil.uuid());
                            historico_cli_edit.put("fecha_on", SUtil.now());
                            historico_cli_edit.put("key_usuario", obj.getString("key_usuario"));

                            historico_cli_edit.put("idvendedor", obj.get("idvendedor"));
                            historico_cli_edit.put("idtransportista", obj.get("idtransportista"));

                            historico_cli_edit.put("estado", 1);
                            historico_cli_edit.put("tipo", "delete");
                            historico_cli_edit.put("data", obj.getJSONObject("data"));
                            try {
                                System.out.println(historico_cli_edit.toString());
                                SPGConect.insertArray("historico_cli_edit", new JSONArray().put(historico_cli_edit));
                              } catch (Exception e) {
                                  // TODO: handle exception
                                  e.printStackTrace();
                              }
                        }

                        Dhm.eliminar(COMPONENT, PK, obj.getJSONObject("data").get(PK)+"");
                        obj.getJSONObject("data").remove("sync_type");
                        obj.put("estado", "exito");
                        obj.put("data", obj.getJSONObject("data"));
                        return;
                    }
                }
            }


            obj.put("estado", "error");
            obj.put("error", "No existe data");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void validarTelefono(JSONObject obj, SSSessionAbstract session) {
        try {

            String consulta = "select tbcli.* \n" + //
                    "            from tbcli\n" + //
                    "            where tbcli.idcli = " + obj.get("idcli");

            JSONArray clientes = Dhm.query(consulta);
            if (clientes.length() == 0) {
                obj.put("error", "No existe el codigo de cliente.");
                obj.put("estado", "error");
                return;
            }

            JSONObject tbcli = clientes.getJSONObject(0);

            if (tbcli == null) {
                // Error al introducir el telefono
                obj.put("error", "No existe el cliente.");
                obj.put("estado", "error");
                return;
            }

            if (tbcli.get("clitel").equals("0")) {
                // El telefono del cliente es 0
                tbcli.put("clitel", obj.get("clitel"));
                consulta = "update tbcli set clitel = '" + obj.get("clitel") + "' where idcli = " + obj.getInt("idcli");
                Dhm.query(consulta);
                obj.put("cliente", tbcli);
                obj.put("estado", "exito");
                return;
            }

            if (!tbcli.get("clitel").equals(obj.get("clitel"))) {
                // Error al introducir el telefono
                obj.put("error", "Error al introducir el teléfono.");
                obj.put("estado", "error");
                return;
            }

            obj.put("cliente", tbcli);
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
                    "and tbcli.clicod = dm_cabfac.clicod \n" +
                    "and dm_cabfac.vfec between '" + obj.getString("fecha_inicio") + "' and '"
                    + obj.getString("fecha_fin") + "'\n" + //
                    "and tbemp.empcod = dm_cabfac.codvendedor \n" +
                    "and dm_cabfac.idven not in (\n" +
                    "    select dm_cabfac.idven\n" +
                    "    from dm_cabfac,\n" +
                    "    tbven\n" +
                    "    where tbven.idpeddm = dm_cabfac.idven)\n" +
                    "group by  \n" +
                    "tbcli.idcli \n" +

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

            if (obj.has("cliidemp")) {

                String consulta = "select array_to_json(array_agg(idz)) as json \n"+
                "from zona_empleado \n"+
                "where zona_empleado.idemp = "+obj.get("cliidemp")+"\n"+
                "and zona_empleado.dia = "+obj.get("dia")+"\n"+
                "and zona_empleado.estado > 0";

                JSONArray idzs = SPGConect.ejecutarConsultaArray(consulta);
                if(idzs.length()>0){
                    consulta = "select "+selecttbcli+"";
                    consulta += "from tbcli where tbcli.idz in ("+idzs.toString().substring(1,idzs.toString().length()-1)+")";

                    obj.put("data", Dhm.query(consulta));
                }else{
                    obj.put("data", new JSONArray());
                }
                
            } else if (obj.has("idz")) {
                String consulta = "select "+selecttbcli+" ";
                consulta += "from tbcli where tbcli.idz = " + obj.get("idz");
                obj.put("data", Dhm.query(consulta));
            } else if (obj.has("fecmod")) {
                obj.put("data",
                        Dhm.query("select * from tbcli where tbcli.fecmod > '" + obj.getString("fecmod") + "'"));
            } else {
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

    public static void getByCode(JSONObject obj, SSSessionAbstract session) {
        try {
            obj.put("data", Dhm.getByKey(COMPONENT, PK, obj.getString("code")));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            // e.printStackTrace();
        }
    }
    
    public static void getByCliCod(JSONObject obj, SSSessionAbstract session) {
        try{
            String consulta = "select * from tbcli where tbcli.clicod = '"+obj.get("clicod")+"' order by idcli";
            obj.put("data", Dhm.query(consulta));
            obj.put("estado", "exito");
        }catch(Exception e){
            e.printStackTrace();
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
        }
    }

    public static void getByKeys(JSONObject obj, SSSessionAbstract session) {
        try {

            JSONArray array = obj.getJSONArray("keys");

            String keys = "";
            for (int i = 0; i < array.length(); i++) {
                keys += array.get(i) + ",";
            }

            if (keys.length() > 0){
                keys = keys.substring(0, keys.length() - 1);
                String consulta = "select * from tbcli where idcli in (" + keys + ")";

                obj.put("data", Dhm.query(consulta));
                obj.put("estado", "exito");
                return;
            }
                
            obj.put("data", "sin keys");
            obj.put("estado", "error");

            
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getByKeyEmpleado(JSONObject obj, SSSessionAbstract session) {
        try {
            String consulta = "select * from tbcli where cliidemp = " + obj.get("key");
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
            if (json.length() > 0) {
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

            String consulta = "select tabla.cant,\n" + //
                    " tabla.znom\n" + //
                    "from (\n" + //
                    "select tbzon.znom,\n" + //
                    "count(idcli) as cant\n" + //
                    "from tbcli,\n" + //
                    "tbzon\n" + //
                    "where tbzon.idz = " + data.get("idz") + "\n" + //
                    "and tbzon.idz = tbcli.idz\n" + //
                    "group by tbzon.znom\n" + //
                    ") tabla";
            JSONArray clicods = Dhm.query(consulta);

            JSONObject clicod = clicods.getJSONObject(0);
            int cant = clicod.getInt("cant");
            cant++;

            String old_clicod  = data.getString("clicod");

            String sclicod = (clicod.has("znom")?clicod.getString("znom"):"") + "-" + cant;

            data.put("clicod", sclicod);
            data.put("usumod", "");
            data.put("empest", 1);

            Dhm.registro(COMPONENT, PK, data);

            String new_clicod  = data.getString("clicod");

            JSONObject historico_clicod = new JSONObject();
            historico_clicod.put("clicod_old", old_clicod);
            historico_clicod.put("clicod_new", new_clicod);
            historico_clicod.put("fecha_on", SUtil.formatTimestamp(new Date()));
            SPGConect.insertObject("historico_clicod", historico_clicod);

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

            String consulta = "" +

                    "select tbcli.idcli, \n" +
                    "( \n" +
                    " select count(dm_cabfac.idven) \n" +
                    " from dm_cabfac \n" +
                    " where dm_cabfac.clicod = tbcli.clicod \n" +
                    " and dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n"+
                    ") as  cantidad_pedidos, \n" +
                    "( \n" +
                    " select sum(dm_detfac.vdcan) \n" +
                    " from dm_cabfac, dm_detfac \n" +
                    " where dm_cabfac.clicod = tbcli.clicod \n" +
                    " and dm_cabfac.idven = dm_detfac.idven \n" +
                    " and dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n"+
                    ") as  cantidad_productos, \n" +
                    "( \n" +
                    " select sum(dm_detfac.vdcan*dm_detfac.vdpre) \n" +
                    " from dm_cabfac, dm_detfac \n" +
                    " where dm_cabfac.clicod = tbcli.clicod \n" +
                    " and dm_cabfac.idven = dm_detfac.idven \n" +
                    " and dm_cabfac.vfec between '"+obj.getString("fecha_inicio")+"' and '"+obj.getString("fecha_fin")+"'\n"+
                    ") as  monto_pedidos \n" +

                    "from tbcli \n" +
                    "where tbcli.idcli = " + obj.get("idcli");

            JSONArray data = Dhm.query(consulta);

            obj.put("data", data);
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }
}
