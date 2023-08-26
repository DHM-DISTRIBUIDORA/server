package util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import Servisofts.SConfig;


public class GPX {

    public static void saveGPX(String key_usuario, double lat, double lon, double deegre) {
        new GPX(key_usuario).addWPT(lat, lon, deegre);
    }

    public static void saveGPXGlup(String key_glup, String key_usuario, double lat, double lon, double deegre) {
        new GPX(key_usuario, key_glup).addWPT(lat, lon, deegre);
    }

    public static JSONObject getFirstPost(String key_glup, String key_usuario) {
        return new GPX(key_usuario, key_glup).getWPT(0);
    }

    private String name;
    private String url;
    private DocumentBuilderFactory dFactory;

    public GPX(String key_usuario) {
        // Root Element
        String rootPath = SConfig.getJSON("files").getString("url_gpx");
        Calendar cal = Calendar.getInstance();
        String year = String.valueOf(cal.get(Calendar.YEAR));
        String month = String.valueOf(cal.get(Calendar.MONTH) + 1);
        String day = String.valueOf(cal.get(Calendar.DAY_OF_MONTH));
        String hora = String.valueOf(cal.get(Calendar.HOUR_OF_DAY));
        this.url = rootPath + key_usuario + "/" + year + "/" + month + "/" + day + "/";
        if (hora.length() == 1) {
            hora = "0" + hora;
        }
        this.name = hora;
        dFactory = DocumentBuilderFactory.newInstance();
        // load();
    }

    public GPX(String key_usuario, String key_glup) {
        // Root Element
        String rootPath = SConfig.getJSON("files").getString("url_glup");
        this.url = rootPath + key_glup + "/gpx/";
        this.name = key_usuario;
        dFactory = DocumentBuilderFactory.newInstance();
        // load();
    }

    public void init() {
        try {
            DocumentBuilder docBuilder = dFactory.newDocumentBuilder();
            Document doc = docBuilder.newDocument();
            Element gpx = doc.createElement("gpx");
            doc.appendChild(gpx);
            save(doc);
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
    }

    public void save(Document doc) {
        try {
            String arr[] = url.split("/");
            String urlTemp = "";
            for (int i = 0; i < arr.length; i++) {
                urlTemp += "/" + arr[i];
                File d = new File(urlTemp);
                if (!d.exists()) {
                    d.mkdirs();
                }
            }
            File itm = new File(this.url + name + ".gpx");
            // INFO  falta importar, para utilizar 
            // FileWriter fw = new FileWriter(this.url + name + ".gpx");
            
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            StreamResult result = new StreamResult(itm);
            transformer.transform(source, result);
            // System.out.println("File saved!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Document load() {
        try {
            // File f = new File("./" + name + ".gpx");
            Path fileName = Path.of(this.url + name + ".gpx");
            DocumentBuilder docBuilder = dFactory.newDocumentBuilder();
            StringBuilder xmlStringBuilder = new StringBuilder();
            xmlStringBuilder.append(Files.readString(fileName));
            ByteArrayInputStream input = new ByteArrayInputStream(xmlStringBuilder.toString().getBytes("UTF-8"));
            Document doc = docBuilder.parse(input);
            return doc;
        } catch (ParserConfigurationException | SAXException | IOException e) {
            return null;
        }
    }

    public JSONObject getWPT(int pos) {
        if (load() == null) {
            init();
        }

        Document doc = load();
        Element root = doc.getDocumentElement();
        Element element = (Element) root.getElementsByTagName("wpt").item(pos);
        String lat = element.getAttribute("lat");
        String lon = element.getAttribute("lon");
        JSONObject obj = new JSONObject();
        obj.put("latitude", lat);
        obj.put("longitude", lon);
        return obj;
    }

    public void addWPT(double lat, double lon, double deegre) {
        if (load() == null) {
            init();
        }

        Document doc = load();
        Element root = doc.getDocumentElement();
        Element wpt = doc.createElement("wpt");
        wpt.setAttribute("lat", lat + "");
        wpt.setAttribute("lon", lon + "");
        // wpt.setAttribute("time", fecha.getTime() + "");
        wpt.setAttribute("deegre", deegre + "");
        root.appendChild(wpt);
        Element time = doc.createElement("time");
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        time.setTextContent(format.format(new Date()));
        wpt.appendChild(time);
        save(doc);
    }
}
