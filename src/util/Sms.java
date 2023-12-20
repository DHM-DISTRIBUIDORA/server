package util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;

import org.json.JSONArray;
import org.json.JSONObject;

import Servisofts.SConfig;

public class Sms {

    public static String sendCode(String phone) throws Exception {
        JSONArray recipients = new JSONArray();
        recipients.put(phone);
        JSONObject sms = new JSONObject();
        sms.put("type", "sms");
        sms.put("originator", "Tapeke");
        sms.put("datacoding", "plain");
        sms.put("mclass", 1);
        sms.put("recipients", recipients);
        String code = getCode();
        sms.put("body", "Tapeke App verification code " + code);
        send(sms);
        return code;
    }

    public static String getRandomNumber() {
        return ((int) (Math.random() * 9)) + "";
    }

    public static String getCode() {
        String code = getRandomNumber() + getRandomNumber() + getRandomNumber() + getRandomNumber() + getRandomNumber();
        return code;
    }

    public static boolean send(JSONObject message) {

        System.out.println("[SendMsn] ");
        try {

            String accesKey = SConfig.getJSON("sms").getString("api_key");
            URL url = new URL("https://rest.messagebird.com/messages");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Length", Integer.toString(message.toString().getBytes().length));
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Authorization", "AccessKey " + accesKey);

            con.setUseCaches(false);
            con.setDoOutput(true);

            DataOutputStream out = new DataOutputStream(con.getOutputStream());
            out.writeBytes(message.toString());
            out.close();

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-16"));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
            con.disconnect();
            return true;
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(getCode());
        sendCode("59175395848");
    }

}
