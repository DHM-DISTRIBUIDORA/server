package Component;

import org.json.JSONArray;
import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;
import Servisofts.SUtil;

public class TbEmp {
    public static final String COMPONENT = "tbemp";
    public static final String PK = "idemp";

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
            case "picklist":
                picklist(obj, session);
                break;
            case "picklist2":
                picklist2(obj, session);
                break;
            case "getVentasFactura":
                getVentasFactura(obj, session);
                break;
            case "entregas":
                entregas(obj, session);
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

            String consulta = "select *, ";
            consulta += "(select sum(tbvd.vdpre*tbvd.vdcan)  from tbven, tbvd where tbven.vtipo in ('VF') and tbven.idemp = tbemp.idemp and tbvd.idven = tbven.idven) as monto_total_ventas, ";
            consulta += "(select sum(tbvd.vdpre*tbvd.vdcan)  from tbven, tbvd where tbven.vtipo in ('VD') and tbven.idemp = tbemp.idemp and tbvd.idven = tbven.idven) as monto_total_pedidos ";
            consulta += "from tbemp";

            obj.put("data", Dhm.query(consulta));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void getVentasFactura(JSONObject obj, SSSessionAbstract session) {
        try {

            if(!obj.has("idemp")){
                obj.put("estado", "error");
                obj.put("error", "Sin idemp");
                return;
            }

            String consulta = "SELECT * FROM tbtg WHERE idemp="+obj.get("idemp")+" AND tgfec='"+obj.get("fecha")+"'";
            JSONArray tbtg_ = Dhm.query(consulta);
            if(tbtg_.length()==0){
                return;
            }

            JSONObject tbtg = tbtg_.getJSONObject(0);
            consulta = "SELECT tbven.idven, \n"+
                    "tbven.vtipp, \n" + //
                    " tbven.vfec AS fecha,\n" + //
                    " tbcli.clilat, \n" + //
                    "tbcli.clilon, \n" + //
                    "tbven.vobs, \n" + //
                    "tbcli.clitel, \n" + //
                    "tbcli.idcli, \n" + //
                    "tbcli.clicod AS codigo, \n" + //
                    "zcod AS zona, \n" + //
                    "tbven.vtipo AS tipo, \n" + //
                    "vdoc AS docum, \\n" + //
                    "clinom, vcli AS razon_social, \n" + //
                    "CASE VR.vtipp WHEN 0 THEN TimpR-ISNULL(Tdesc,0) ELSE 0 END AS contado, \n" + //
                    "CASE VR.vtipp WHEN 1 THEN TimpR-ISNULL(Tdesc,0) ELSE 0 END AS credito, vdesc AS descen,clidir AS direccion, vnit  \n" + //
                    "FROM tbven LEFT JOIN tbvenaux ON tbven.idven=tbvenaux.idven  LEFT JOIN tbemp ON tbven.idemp=tbemp.idemp \n" + //
                    "LEFT JOIN tbtg ON tbven.idtg=tbtg.idtg \n" + //
                    "LEFT JOIN tbzon ON tbven.vidzona=tbzon.idz \n" + //
                    "LEFT JOIN tbcli ON tbven.idcli=tbcli.idcli \n" + //
                    "LEFT JOIN ( \n" + //
                    "    SELECT tbven.idven, tbven.vtipp, round(isnull(SUM(vdimp),0),2) AS TimpR, round(SUM(vdcan),2) AS Tcanven \n" + //
                    "    FROM tbven INNER JOIN tbvd ON tbven.idven=tbvd.idven \n" + //
                    "    WHERE vdest=2 AND vfec='"+obj.get("fecha")+"'  GROUP BY tbven.idven, tbven.vtipp \n" + //
                    ") AS VR ON tbven.idven=vr.idven \n" + //
                    "LEFT JOIN ( \n" + //
                    "    SELECT tbvc.idven, round(isnull(SUM(vcimp),0),2) AS Tdesc \n" + //
                    "    FROM tbvc INNER JOIN tbcob ON tbvc.idcob=tbcob.idcob \n" + //
                    "    WHERE ctipo=6 GROUP BY tbvc.idven \n" + //
                    ") AS DF ON tbven.idven=DF.idven \n" + //
                    "\n" + //
                    "WHERE tbven.vtipo LIKE 'V%' AND vdest=2 AND tbven.vfec='"+obj.get("fecha")+"'  AND tbtg.idtg="+tbtg.get("idtg")+"  AND idalm=1   \n" + //
                    "GROUP BY tbven.idven, tbven.vtipp,tbcli.idcli,tbven.vfec,vnit, tbcli.clicod, tbven.vobs,tbcli.clitel, tbcli.clilat, tbcli.clilon, zcod, tbven.vtipo, vdoc, clinom, vcli, TimpR, tcanven, VR.vtipp,Tdesc, vdesc, zcod, clidir, clidirnro, cliadic , clilat, clilon  ORDER BY zcod, vdoc  ";

            obj.put("data", Dhm.query(consulta));

            consulta = "SELECT tbvd.idven, tbvd.idvd, tbvd.idprd,  tbvd.vdcan, tbvd.vdpre \n" + //
                    "FROM tbven LEFT JOIN tbvenaux ON tbven.idven=tbvenaux.idven  LEFT JOIN tbemp ON tbven.idemp=tbemp.idemp \n" + //
                    "LEFT JOIN tbtg ON tbven.idtg=tbtg.idtg \n" + //
                    "LEFT JOIN tbvd ON tbven.idven=tbvd.idven \n" + //
                    "LEFT JOIN tbzon ON tbven.vidzona=tbzon.idz \n" + //
                    "LEFT JOIN tbcli ON tbven.idcli=tbcli.idcli \n" + //
                    "LEFT JOIN ( \n" + //
                    "    SELECT tbven.idven, tbven.vtipp, round(isnull(SUM(vdimp),0),2) AS TimpR, round(SUM(vdcan),2) AS Tcanven \n" + //
                    "    FROM tbven INNER JOIN tbvd ON tbven.idven=tbvd.idven \n" + //
                    "    WHERE tbvd.vdest=2 AND vfec='"+obj.get("fecha")+"'  GROUP BY tbven.idven, tbven.vtipp \n" + //
                    ") AS VR ON tbven.idven=vr.idven \n" + //
                    "LEFT JOIN ( \n" + //
                    "    SELECT tbvc.idven, round(isnull(SUM(vcimp),0),2) AS Tdesc \n" + //
                    "    FROM tbvc INNER JOIN tbcob ON tbvc.idcob=tbcob.idcob \n" + //
                    "    WHERE ctipo=6 GROUP BY tbvc.idven \n" + //
                    ") AS DF ON tbven.idven=DF.idven \n" + //
                    "\n" + //
                    "WHERE tbven.vtipo LIKE 'V%' AND tbvd.vdest=2 AND tbven.vfec='"+obj.get("fecha")+"'  AND tbtg.idtg="+tbtg.get("idtg")+"  AND tbvd.idalm=1   \n" + //
                    "GROUP BY  tbvd.idvd, tbvd.idprd, tbvd.vdcan, tbvd.vdpre, tbvd.idven   ";

            obj.put("detalle", Dhm.query(consulta));
            obj.put("estado", "exito");
        } catch (Exception e) {
            obj.put("estado", "error");
            obj.put("error", e.getMessage());
            e.printStackTrace();
        }
    }

    public static void picklist2(JSONObject obj, SSSessionAbstract session) {

        String consulta = "SELECT * FROM tbtg WHERE idemp="+obj.get("idemp")+" AND tgfec='"+obj.get("fecha")+"'";
        JSONArray tbtg_ = Dhm.query(consulta);
        if(tbtg_.length()==0){
            
            return;
        }

        JSONObject tbtg = tbtg_.getJSONObject(0);
        consulta = "SELECT  tbprdlin.lincod,  \n" + //
                "tbvd.idprd,  \n" + //
                "prdcxu,  \n" + //
                "vfec, \n" + //
                "prdcod,  \n" + //
                "prdnom,  \n" + //
                "prdunid,  \n" + //
                "prduxcdes, \n" + //
                "SUM(vdcan) AS cantidad_vendido,\n" + //
                "SUM(vdimp) AS total_vendido \n" + //
                "FROM (((((tbven LEFT JOIN  tbvd \n" + //
                "ON tbven.idven=tbvd.idven) LEFT JOIN tbcli ON tbven.idcli=tbcli.idcli) \n" + //
                "LEFT JOIN tbprd ON tbvd.idprd=tbprd.idprd) LEFT JOIN tbprdlin ON tbprd.idlinea=tbprdlin.idlinea) \n" + //
                "LEFT JOIN tbtg ON tbven.idtg=tbtg.idtg) LEFT JOIN tbzon ON tbcli.idz=tbzon.idz LEFT JOIN tbemp ON tbven.idemp=tbemp.idemp \n" + //
                "WHERE tbven.vtipo LIKE 'V%' AND tbven.idven > 0   AND idalm=1  AND tbtg.idtg="+tbtg.get("idtg")+" \n" + //
                "GROUP BY tbprdlin.lincod, tbvd.idprd, prdcxu, vfec, prdcod, prdnom, prdunid, prduxcdes \n" + //
                " ORDER BY  prdcod";

        JSONArray qery2 = Dhm.query(consulta);

        obj.put("data", qery2);
        obj.put("estado", "exito");
    }
    public static void picklist(JSONObject obj, SSSessionAbstract session) {

        String consulta = "SELECT * FROM tbtg WHERE idemp="+obj.get("idemp")+" AND tgfec='"+obj.get("fecha")+"'";
        JSONArray tbtg_ = Dhm.query(consulta);
        if(tbtg_.length()==0){
            
            return;
        }

        JSONObject tbtg = tbtg_.getJSONObject(0);
        consulta = "SELECT  tbprdlin.lincod, tbvd.idprd, prdcxu, ' ', vfec AS Fecha, \n" + //
                "prdcod AS Codigo, prdnom AS [Nombre Producto], prdunid AS Unidad, SUM(vdcan)*prdkxu AS [Peso Neto Kgs], SUM(vdcan)*prdklxc AS [Peso Bruto Kgs], \n" + //
                "0 AS [Cjs.], prduxcdes AS Unidad, 0 AS [Unid.], prdunid AS Unidad_2, 0 AS [Unidad Minima], \n" + //
                "0 AS [Unidad Mediana], 0 AS [Unidad Maxima], 0 AS [Cantidad Preventa], SUM(vdcan) AS [Cantidad Vendida], 0 AS [Diferencia Devolucion], \n" + //
                "0 AS [Precio de Venta], 0 AS [Total Preventa Bs.], SUM(vdimp) AS [Total Vendido Bs.], 0 AS [Diferencia Bs.], '' AS [Documentos de Preventa], '' AS [Documentos de Venta] \n" + //
                "FROM (((((tbven LEFT JOIN  tbvd \n" + //
                "ON tbven.idven=tbvd.idven) LEFT JOIN tbcli ON tbven.idcli=tbcli.idcli) \n" + //
                "LEFT JOIN tbprd ON tbvd.idprd=tbprd.idprd) LEFT JOIN tbprdlin ON tbprd.idlinea=tbprdlin.idlinea) \n" + //
                "LEFT JOIN tbtg ON tbven.idtg=tbtg.idtg) LEFT JOIN tbzon ON tbcli.idz=tbzon.idz LEFT JOIN tbemp ON tbven.idemp=tbemp.idemp \n" + //
                "WHERE tbven.vtipo LIKE 'V%' AND tbven.idven > 0   AND idalm=1  AND tbtg.idtg="+tbtg.get("idtg")+" \n" + //
                "GROUP BY vfec, tbvd.idprd, prdcod, prdnom, prdunid, prdcxu, prdkxu, lincod,prdklxc,prduxcdes ORDER BY  prdcod";

        JSONArray qery2 = Dhm.query(consulta);

        obj.put("data", qery2);
        obj.put("estado", "exito");
    }


    public static void entregas(JSONObject obj, SSSessionAbstract session) {

        String consulta = "SELECT * FROM tbtg WHERE idemp="+obj.get("idemp")+" AND tgfec='"+obj.get("fecha")+"'";
        JSONArray tbtg_ = Dhm.query(consulta);
        JSONObject tbtg = tbtg_.getJSONObject(0);

        consulta = "SELECT  tbprdlin.lincod, tbvd.idprd, prdcxu, ' ', vfec AS Fecha, \n" + //
                "prdcod AS Codigo, prdnom AS [Nombre Producto], prdunid AS Unidad, SUM(vdcan)*prdkxu AS [Peso Neto Kgs], SUM(vdcan)*prdklxc AS [Peso Bruto Kgs], \n" + //
                "0 AS [Cjs.], prduxcdes AS Unidad, 0 AS [Unid.], prdunid AS Unidad_2, 0 AS [Unidad Minima], \n" + //
                "0 AS [Unidad Mediana], 0 AS [Unidad Maxima], 0 AS [Cantidad Preventa], SUM(vdcan) AS [Cantidad Vendida], 0 AS [Diferencia Devolucion], \n" + //
                "0 AS [Precio de Venta], 0 AS [Total Preventa Bs.], SUM(vdimp) AS [Total Vendido Bs.], 0 AS [Diferencia Bs.], '' AS [Documentos de Preventa], '' AS [Documentos de Venta] \n" + //
                "FROM (((((tbven LEFT JOIN \n" + //
                "tbvd ON tbven.idven=tbvd.idven) LEFT JOIN tbcli ON tbven.idcli=tbcli.idcli) \n" + //
                "LEFT JOIN tbprd ON tbvd.idprd=tbprd.idprd) LEFT JOIN tbprdlin ON tbprd.idlinea=tbprdlin.idlinea) \n" + //
                "LEFT JOIN tbtg ON tbven.idtg=tbtg.idtg) LEFT JOIN tbzon ON tbcli.idz=tbzon.idz LEFT JOIN tbemp ON tbven.idemp=tbemp.idemp \n" + //
                "WHERE tbven.vtipo LIKE 'V%' AND tbven.idven > 0   AND idalm=1  AND tbtg.idtg="+tbtg.get("idtg")+" \n" + //
                "GROUP BY vfec, tbvd.idprd, prdcod, prdnom, prdunid, prdcxu, prdkxu, lincod,prdklxc,prduxcdes ORDER BY  prdcod";

        JSONArray qery2 = Dhm.query(consulta);

        obj.put("data", qery2);
        obj.put("estado", "exito");
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
