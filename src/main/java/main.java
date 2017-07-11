import org.apache.log4j.Logger;
import org.apache.velocity.app.Velocity;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.websocket.server.WebSocketHandler;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.glassfish.tyrus.client.ClientManager;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.FileReader;
import java.net.URI;
import java.util.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class main {

    final static Logger infoLogger = Logger.getLogger("infoLogger");
    final static Logger errorLogger = Logger.getLogger("errorLogger");
    public static Map<String,Terminal> terminals = new ConcurrentHashMap<String,Terminal>();
    public static Map<String,JSONObject> requests = new HashMap<String,JSONObject>();
    public static ws_client client = new ws_client("Pass thru");
    public static JSONObject gerMarkets = new JSONObject();
    public static JSONObject marketGroup = new JSONObject();
    public static int retryes = 600;
    public static CountDownLatch latch = new CountDownLatch(13);
    private static JSONObject mailCred = new JSONObject();

    public static LiveUpdater football;
    public static Totalizer footballTotals = new Totalizer(latch, "Soccer");
    public static LiveUpdater tennis;
    public static Totalizer tennisTotals = new Totalizer(latch, "Tennis");
    public static LiveUpdater hockey_basket;
    public static Totalizer hockey_basketTotals = new Totalizer(latch, "Hockey and Basketball");
    public static LiveUpdater other;
    public static Totalizer otherTotals = new Totalizer(latch, "Other sports");

    public static FliveUpdater flw = new FliveUpdater(latch);
    public static Totalizer fliveTotals = new Totalizer(latch, "Flive");
    public static MenuUpdater mw = new MenuUpdater(latch);
    public static FavoriteUpdater favw = new FavoriteUpdater(latch);
    public static Totalizer favTotals = new Totalizer(latch, "Favorite");

    public static final Map<String, String> sportPartsT;
    public static final Map<String, String> sportPartsGer;
    static{
        sportPartsT = new HashMap<String,String>();
        sportPartsT.put("858", "Round");
        sportPartsT.put("848" , "Set");
        sportPartsT.put("1308102967" , "Half");
        sportPartsT.put("846" , "Period");
        sportPartsT.put("850" , "Quarter");
        sportPartsT.put("852" , "Set");
        sportPartsT.put("854" , "Half");
        sportPartsT.put("856" , "Inning");
        sportPartsT.put("862" , "Set");
        sportPartsT.put("864" , "Period");
        sportPartsT.put("866" , "Half");
        sportPartsT.put("36116468" , "Half");
        sportPartsT.put("868" , "Frame");
        sportPartsT.put("870" , "Quarter");
        sportPartsT.put("886" , "Quarter");
        sportPartsT.put("872" , "Period");
        sportPartsT.put("874" , "Half");
        sportPartsT.put("878" , "Period");
        sportPartsT.put("884" , "Set");
        sportPartsT.put("900" , "Game");
        sportPartsT.put("197402321" , "Game");
        sportPartsT.put("108949150" , "Quarter");
        sportPartsGer = new HashMap<String,String>();
        sportPartsGer.put("858", "Runden");
        sportPartsGer.put("848" , "Satz");
        sportPartsGer.put("1308102967" , "Hälfte");
        sportPartsGer.put("846" , "Periode");
        sportPartsGer.put("850" , "Viertel");
        sportPartsGer.put("852" , "Satz");
        sportPartsGer.put("854" , "Hälfte");
        sportPartsGer.put("856" , "Inning");
        sportPartsGer.put("862" , "Satz");
        sportPartsGer.put("864" , "Periode");
        sportPartsGer.put("866" , "Hälfte");
        sportPartsGer.put("36116468" , "Hälfte");
        sportPartsGer.put("868" , "Frame");
        sportPartsGer.put("870" , "Viertel");
        sportPartsGer.put("886" , "Viertel");
        sportPartsGer.put("872" , "Period");
        sportPartsGer.put("874" , "Hälfte");
        sportPartsGer.put("878" , "Periode");
        sportPartsGer.put("884" , "Satz");
        sportPartsGer.put("900" , "Spiel");
        sportPartsGer.put("197402321" , "Spiel");
        sportPartsGer.put("108949150" , "Viertel");
    }

    public static void main(String[] args) throws Exception {

        JSONParser parser = new JSONParser();
        try {
            Properties p = new Properties();
            p.setProperty("file.resource.loader.path", "./src/main/templates");
            Velocity.init(p);
            Object obj = parser.parse(new FileReader("./src/main/resources/markets.json"));
            gerMarkets = (JSONObject) obj;
            obj = parser.parse(new FileReader("./src/main/resources/market_group.json"));
            marketGroup = (JSONObject) obj;
            mailCred = (JSONObject) parser.parse(new FileReader("./src/main/resources/mail.json"));
        }
        catch(Exception e){
            e.printStackTrace();
        }

        WebSocketHandler wsHandler = new WebSocketHandler() {
            @Override
            public void configure(WebSocketServletFactory factory) {
                //factory.getPolicy().setIdleTimeout(5000);
                factory.getPolicy().setAsyncWriteTimeout(10000);
                factory.register(ws_server.class);
            }
        };

        try {
            List<Integer> scr = new ArrayList<Integer>();
            scr.add(844);
            List<Integer> hbskt = new ArrayList<Integer>();
            hbskt.add(850);
            hbskt.add(846);
            List<Integer> tns = new ArrayList<Integer>();
            tns.add(848);
            List<Integer> othr = new ArrayList<Integer>();
            othr.addAll(scr);
            othr.addAll(hbskt);
            othr.addAll(tns);

            football = new LiveUpdater(scr,"8081","@in",latch, footballTotals);
            tennis = new LiveUpdater(tns, "8082", "@in",latch, tennisTotals);
            hockey_basket = new LiveUpdater(hbskt,"8083", "@in",latch, hockey_basketTotals);
            other = new LiveUpdater(othr, "8084", "@nin",latch, otherTotals);

            footballTotals.start();
            tennisTotals.start();
            hockey_basketTotals.start();
            otherTotals.start();
            fliveTotals.start();
            favTotals.start();

            football.start();
            tennis.start();
            hockey_basket.start();
            other.start();
            flw.start();
            mw.start();
            favw.start();

            client.addMessageHandler(new ws_client.MessageHandler() {
                public void handleMessage(String message) {
                    JSONParser parser = new JSONParser();
                    JSONObject rcvd;
                    try
                    {
                        rcvd = (JSONObject) parser.parse(message);
                        if (rcvd.containsKey("rid"))
                        {
                            JSONObject data = (JSONObject) rcvd.get("data");
                            if (data.containsKey("data"))
                            {
                                String rid = rcvd.get("rid").toString();
                                JSONObject data2 = (JSONObject) data.get("data");
                                //System.out.println(rid);
                                String tid = requests.get(rid).get("tid").toString();
                                if (terminals.containsKey(requests.get(rid).get("tid").toString())) {
                                    Terminal terminal = terminals.get(requests.get(rid).get("tid").toString());
                                    JSONObject qObj = new JSONObject();
                                    switch ((String)requests.get(rid).get("command"))
                                    {
                                        case "get_day":
                                        {
                                            qObj.put("type","day");
                                            qObj.put("data",data2);
                                            terminal.getQueue().put(qObj);
                                            requests.remove(rid);
                                        }; break;
                                        case "get_comp":
                                        {
                                            qObj.put("type","comp");
                                            qObj.put("data",data2);
                                            terminal.getQueue().put(qObj);
                                            requests.remove(rid);
                                        }; break;
                                        case "get_region":
                                        {
                                            qObj.put("type","region");
                                            qObj.put("data",data2);
                                            terminal.getQueue().put(qObj);
                                            requests.remove(rid);
                                        }; break;
                                        case "search":
                                        {
                                            qObj.put("type","search");
                                            qObj.put("data",data2);
                                            terminal.getQueue().put(qObj);
                                            requests.remove(rid);
                                        }; break;
                                        case "get_coeff":
                                        {
                                            qObj.put("type","event");
                                            qObj.put("data",data2);
                                            terminal.getQueue().put(qObj);
                                            requests.remove(rid);
                                        }; break;
                                        case "betvars":
                                        {
                                            qObj.put("type","vars2");
                                            qObj.put("data",data2);
                                            terminal.setCount(1);
                                            terminal.getQueue().put(qObj);
                                            requests.remove(rid);
                                        }; break;
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        // TODO Auto-generated catch block
                        main.errorLogger.error("Error happened", e);
                        e.printStackTrace();
                    }
                }
            });

            ClientManager cm = new ClientManager();
            cm.getProperties().put("org.glassfish.tyrus.incomingBufferSize", 104857600);
            cm.connectToServer(client, new URI("ws://swarm.solidarbet.com:8092"));
            long counter = latch.getCount();

            System.out.println("Starting count from "+counter);

            JSONObject start = new JSONObject ();
            start.put("type", "start");
            football.getQueue().put(start);
            tennis.getQueue().put(start);
            hockey_basket.getQueue().put(start);
            other.getQueue().put(start);
            flw.getQueue().put(start);
            mw.getQueue().put(start);
            favw.getQueue().put(start);

            latch.await();

            System.out.println("ALL DATA EXISTS!");

            Server server = new Server(5025);
            server.setHandler(wsHandler);
            server.start();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static synchronized void sendNotification(String subj, String msg) {
        final String username = mailCred.get("login").toString();
        final String password = mailCred.get("pass").toString();

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");

        javax.mail.Session session = javax.mail.Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password);
                    }
                });

        try {

            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(username));
            message.setRecipients(Message.RecipientType.TO,
                    InternetAddress.parse("alexf@almog.com.ua,ohmygoodua@gmail.com"));
            message.setSubject(subj);
            message.setText("Administrator,\n\n"+msg+"\n\n Check the logs");

            Transport.send(message);

            System.out.println("Done");

        } catch (MessagingException e) {
            throw new RuntimeException(e);
        }
    }


}


