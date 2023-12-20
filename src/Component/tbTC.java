package Component;

import org.json.JSONArray;


public class tbTC {


    public static double getTipoCambioDolares() {
        try {
            JSONArray data = Dhm.query("select tbTC.*\n" + //
                    "from tbTC\n" + //
                    "where tbTC.Fecha_TC = (select max(Fecha_TC) from tbTC)");
            return data.getJSONObject(0).getDouble("TC");
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }
    public static double getTipoCambioUfv(){
        try {
            JSONArray data = Dhm.query("select tbTC.*\n" + //
                    "from tbTC\n" + //
                    "where tbTC.Fecha_TC = (select max(Fecha_TC) from tbTC)");
            return data.getJSONObject(0).getDouble("tcufv");
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }
}
