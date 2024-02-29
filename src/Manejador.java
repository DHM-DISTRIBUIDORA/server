import Component.*;
import Servisofts.SConsole;
import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;

public class Manejador {
    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        if (session != null) {
            SConsole.log(session.getIdSession(), "\t|\t", obj.getString("component"), obj.getString("type"));
        } else {
            if(!obj.getString("type").equals("onLocationChange")){
                SConsole.log("http-server", "-->", obj.getString("component"), obj.getString("type"));
            }
        }
        if (obj.isNull("component")) {
            return;
        }
        switch (obj.getString("component")) {
            case "usuario": usuario.onMessage(obj, session); break;
            case Dato.COMPONENT: Dato.onMessage(obj, session); break;
            case RolDato.COMPONENT: RolDato.onMessage(obj, session); break;
            case UsuarioDato.COMPONENT: UsuarioDato.onMessage(obj, session); break;
            case Reporte.COMPONENT: Reporte.onMessage(obj, session); break;
            case Dhm.COMPONENT: Dhm.onMessage(obj, session); break;
            case DmCategorias.COMPONENT: DmCategorias.onMessage(obj, session); break;
            case DmProductos.COMPONENT: DmProductos.onMessage(obj, session); break;
            case DmPedido.COMPONENT: DmPedido.onMessage(obj, session); break;
            case DmClientes.COMPONENT: DmClientes.onMessage(obj, session); break;
            case DmUsuarios.COMPONENT: DmUsuarios.onMessage(obj, session); break;
            case TbEmp.COMPONENT: TbEmp.onMessage(obj, session); break;
            case TbZon.COMPONENT: TbZon.onMessage(obj, session); break;
            case TbRutaDia.COMPONENT: TbRutaDia.onMessage(obj, session); break;
            case TbEmt.COMPONENT: TbEmt.onMessage(obj, session); break;
            case TbVen.COMPONENT: TbVen.onMessage(obj, session); break;
            case DmCabFac.COMPONENT: DmCabFac.onMessage(obj, session); break;
            case TbPrd.COMPONENT: TbPrd.onMessage(obj, session); break;
            case TbVd.COMPONENT: TbVd.onMessage(obj, session); break;
            case TbSucesos.COMPONENT: TbSucesos.onMessage(obj, session); break;
            case TbVc.COMPONENT: TbVc.onMessage(obj, session); break;
            case TbPrdlin.COMPONENT: TbPrdlin.onMessage(obj, session); break;
            case TbCli.COMPONENT: TbCli.onMessage(obj, session); break;
            case TbCliTipo.COMPONENT: TbCliTipo.onMessage(obj, session); break;
            case background_location.COMPONENT: background_location.onMessage(obj, session); break;
            case VisitaVendedor.COMPONENT: VisitaVendedor.onMessage(obj, session); break;
            case VisitaTransportista.COMPONENT: VisitaTransportista.onMessage(obj, session); break;
            case TbTg.COMPONENT: TbTg.onMessage(obj, session); break;
            case TbAlm.COMPONENT: TbAlm.onMessage(obj, session); break;
            case Enviroments.COMPONENT: new Enviroments(obj, session); break;
            case TbCat.COMPONENT: TbCat.onMessage(obj, session); break;
            case LocationInfo.COMPONENT: LocationInfo.onMessage(obj, session); break;
            case Log.COMPONENT: Log.onMessage(obj, session); break;
            case ZonaEmpleado.COMPONENT: ZonaEmpleado.onMessage(obj, session); break;
        }
    }
}

