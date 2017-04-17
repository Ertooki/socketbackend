import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Administrator on 17.04.2017.
 */

@WebSocket
public class TotalServer {

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        try {
            System.out.println("Close " + session.getRemoteAddress() + " : statusCode=" + statusCode + ", reason=" + reason + "  - " + new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date()));
            ConcurrentHashMap<String, JSONObject> terminals = (ConcurrentHashMap<String, JSONObject>) main.totalSessions;
            String rm_tid = "";
            for (String tid : terminals.keySet()) {
                Session rcpt = (Session) terminals.get(tid).get("session");
                if (rcpt == session) {
                    rm_tid = tid;
                    break;
                }
            }
            if (rm_tid != "") main.totalSessions.remove(rm_tid);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        session.close();
    }

    @OnWebSocketError
    public void onError(Throwable t) {
        System.out.println("Error: " + t.getMessage());
    }

    @OnWebSocketConnect
    public void onConnect(Session session) {
        System.out.println(new Date() + " Connect: " + session.getRemoteAddress().getAddress());
        main.infoLogger.info("Connect: " + session.getRemoteAddress().getAddress());
    }

    @OnWebSocketMessage
    public void onMessage(Session s, String message) {
        JSONParser parser = new JSONParser();
        try {
            JSONObject rcvd = (JSONObject) parser.parse(message);
            String comm = (String) rcvd.get("command");

            switch (comm) {
                case "build": {
                    JSONObject terminal = new JSONObject();
                    terminal.put("gid", null);
                    terminal.put("session", s);
                    terminal.put("betslip", new ArrayList<String>());
                    if (rcvd.containsKey("multiplier")) terminal.put("multiplier", rcvd.get("multiplier").toString());
                    else terminal.put("multiplier", "1");
                    main.totalSessions.put(rcvd.get("id").toString(), terminal);
                }
                ; break;
                case "get_total":
                {
                    if (rcvd.get("type")!=null)
                    {
                        main.get_total((String)rcvd.get("sid"),(String)rcvd.get("rid"),(String)rcvd.get("cid"),(String)rcvd.get("gid"),(String)rcvd.get("type"), s);
                    }

                }; break;
            }
        }
        catch (Exception e) {
            main.errorLogger.error("Error happened", e);
        }
    }

}
