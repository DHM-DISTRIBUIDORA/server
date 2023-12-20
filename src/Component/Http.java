package Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.CookieHandler;
import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.HttpURLConnection;
import java.net.URL;
import org.json.JSONArray;
import org.json.JSONObject;

public class Http {

    public static String send(String urls, JSONObject data) throws Exception {
        return send(urls, data, null);
    }

    public static String send(String urls, JSONObject data, String X_API_Token) throws Exception{
        URL url = new URL(urls);
        CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        if (X_API_Token != null) {
            con.setRequestProperty("X-Api-Key", X_API_Token);
        }
        try (OutputStream os = con.getOutputStream()) {
            byte[] input = data.toString().getBytes("utf-8");
            os.write(input, 0, input.length);
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
        String resp = "";
        String inputLine;
        while ((inputLine = br.readLine()) != null) {
            resp += inputLine;
        }
        br.close();
        
        return resp;
    }

    public static JSONObject send_(String urls, JSONObject data, String X_API_Token) {
        try {
            URL url = new URL(urls);
            CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", "application/json");
            if (X_API_Token != null) {
                con.setRequestProperty("Authorization", "Bearer "+X_API_Token);
            }
            try (OutputStream os = con.getOutputStream()) {
                byte[] input = data.toString().getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));
            String resp = "";
            String inputLine;
            while ((inputLine = br.readLine()) != null) {
                resp += inputLine;
            }
            br.close();
            return new JSONObject(resp);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getLocalizedMessage());
        }
        return null;
    }

    public static JSONArray send_(String urls, String data, String X_API_Token) throws Exception {
        String resp = "";
        try {
            URL url = new URL(urls);
            CookieHandler.setDefault(new CookieManager(null, CookiePolicy.ACCEPT_ALL));
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", "application/json");
            if (X_API_Token != null) {
                con.setRequestProperty("Authorization", X_API_Token);
            }
            try (OutputStream os = con.getOutputStream()) {
                byte[] input = data.toString().getBytes("UTF-8");
                os.write(input, 0, input.length);
            }

            int statusCode = con.getResponseCode();
            
            String inputLine;

            BufferedReader br;
            if (200 == statusCode) {
                br = new BufferedReader(new InputStreamReader(con.getInputStream()));
                
                while ((inputLine = br.readLine()) != null) {
                    resp += inputLine;
                }
                br.close();
                return new JSONArray(resp);
            }
            
            br = new BufferedReader(new InputStreamReader(con.getErrorStream()));
            while ((inputLine = br.readLine()) != null) {
                resp += inputLine;
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getLocalizedMessage());
        }
        throw new Exception(resp);
    }

}