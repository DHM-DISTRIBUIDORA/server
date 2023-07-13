import Component.*;
import Servisofts.SConsole;
import org.json.JSONObject;
import Server.SSSAbstract.SSSessionAbstract;

public class Manejador {
    public static void onMessage(JSONObject obj, SSSessionAbstract session) {
        if (session != null) {
            SConsole.log(session.getIdSession(), "\t|\t", obj.getString("component"), obj.getString("type"));
        } else {
            SConsole.log("http-server", "-->", obj.getString("component"), obj.getString("type"));
        }
        if (obj.isNull("component")) {
            return;
        }
        switch (obj.getString("component")) {
            case Dato.COMPONENT:
                Dato.onMessage(obj, session);
                break;
            case RolDato.COMPONENT:
                RolDato.onMessage(obj, session);
                break;
            case UsuarioDato.COMPONENT:
                UsuarioDato.onMessage(obj, session);
                break;
            case Reporte.COMPONENT:
                Reporte.onMessage(obj, session);
                break;
            case Dhm.COMPONENT:
                Dhm.onMessage(obj, session);
                break;
            case DmCategorias.COMPONENT:
                DmCategorias.onMessage(obj, session);
                break;
            case DmProductos.COMPONENT:
                DmProductos.onMessage(obj, session);
                break;
            case DmPedido.COMPONENT:
                DmPedido.onMessage(obj, session);
                break;
            case DmClientes.COMPONENT:
                DmClientes.onMessage(obj, session);
                break;
            case DmUsuarios.COMPONENT:
                DmUsuarios.onMessage(obj, session);
                break;
            case TbEmp.COMPONENT:
                TbEmp.onMessage(obj, session);
                break;
            case TbZon.COMPONENT:
                TbZon.onMessage(obj, session);
                break;
            case TbRutaDia.COMPONENT:
                TbRutaDia.onMessage(obj, session);
                break;
        }
    }
}
