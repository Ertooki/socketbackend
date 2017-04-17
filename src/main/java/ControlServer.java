import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Administrator on 17.04.2017.
 */
@WebSocket
public class ControlServer {

    @OnWebSocketClose
    public void onClose(Session session, int statusCode, String reason) {
        try {
            System.out.println("Close " + session.getRemoteAddress()+ " : statusCode=" + statusCode + ", reason=" + reason + "  - " +  new java.text.SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(new Date()));
            ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.cntrlSessions;
            String rm_tid = "";
            for(String tid : terminals.keySet())
            {
                Session rcpt = (Session) terminals.get(tid).get("session");
                if (rcpt == session)
                {
                    rm_tid = tid;
                    break;
                }
            }
            if (rm_tid != "") main.cntrlSessions.remove(rm_tid);
        }
        catch (Exception e) {
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
            String comm = (String)rcvd.get("command");

            switch (comm) {
                case "build": {
                    main.build_live(s);
                    main.build_flive(s);
                    main.build_menu(s);
                    main.build_favorite(s);
                    JSONObject terminal = new JSONObject();
                    terminal.put("gid", null);
                    terminal.put("session", s);
                    terminal.put("betslip", new ArrayList<String>());
                    if (rcvd.containsKey("multiplier")) terminal.put("multiplier", rcvd.get("multiplier").toString());
                    else terminal.put("multiplier", "1");
                    main.cntrlSessions.put(rcvd.get("id").toString(), terminal);
                };
                break;
                case "betvars":
                {
                    //System.out.println(rcvd);
                    main.get_vars((String)rcvd.get("sid"),(String)rcvd.get("rid"),(String)rcvd.get("cid"),(String)rcvd.get("gid"),(String)rcvd.get("type"), rcvd.get("tid").toString(), s);
                    main.updtSessions.get(rcvd.get("tid").toString()).put("gid", rcvd.get("gid").toString());
                }; break;
                case "get_day":
                {
                    Date when = new Date();
                    String rid = UUID.randomUUID().toString();
                    JSONObject request = new JSONObject();
                    request.put("tid", rcvd.get("tid"));
                    request.put("command", "get_day");
                    request.put("when", when);
                    //System.out.println(rcvd.get("tid")+" get_day "+rid);
                    main.requests.put(rid, request);

                    Long from = new Long (rcvd.get("from").toString());
                    Long to = new Long(rcvd.get("to").toString());
                    Date begin = new Date(from);
                    int offset = new Integer(rcvd.get("timeZone").toString())-1;
                    begin.setHours(begin.getHours()+(new Integer(rcvd.get("timeZone").toString()) - offset));
                    Date finish = new Date(to);
                    finish.setHours(begin.getHours()+(new Integer(rcvd.get("timeZone").toString()) - offset));
                    JSONObject get_info = new JSONObject();

                    get_info.put("command","get");
                    get_info.put("rid", rid);

                    JSONObject params = new JSONObject();
                    params.put("source", "betting");

                    JSONObject what = new JSONObject();
                    JSONArray empty = new JSONArray();
                    what.put("sport", empty);
                    what.put("region", empty);
                    what.put("competition", empty);
                    what.put("game", empty);
                    what.put("market", empty);
                    what.put("event", empty);

                    params.put("what", what);

                    JSONObject where = new JSONObject();
                    JSONObject game = new JSONObject();
                    JSONArray and = new JSONArray();
                    JSONObject type = new JSONObject();
                    type.put("type", new Integer(0));
                    JSONObject start = new JSONObject();
                    JSONObject start_value = new JSONObject();
                    start_value.put("@gte", (int)(begin.getTime()/1000));
                    start.put("start_ts", start_value);
                    JSONObject end = new JSONObject();
                    JSONObject end_value = new JSONObject();
                    end_value.put("@lte", (int)(finish.getTime()/1000));
                    end.put("start_ts", end_value);
                    and.add(type);
                    and.add(start);
                    and.add(end);
                    game.put("@and", and);
                    where.put("game", game);
                    params.put("where",where);
                    params.put("subscribe", false);
                    get_info.put("params", params);
                    main.client.sendMessage(get_info.toString());
                }; break;
                case "get_comp":
                {
                    Date when = new Date();
                    String rid = UUID.randomUUID().toString();
                    JSONObject request = new JSONObject();
                    request.put("session", s);
                    request.put("command", "get_comp");
                    request.put("tid", rcvd.get("tid"));
                    request.put("when", when);
                    //System.out.println(rcvd.get("tid")+" get_comp "+rid);
                    main.requests.put(rid, request);

                    JSONObject get_info = new JSONObject();
                    get_info.put("command","get");
                    get_info.put("rid", rid);

                    JSONObject params = new JSONObject();
                    params.put("source", "betting");

                    JSONObject what = new JSONObject();
                    JSONArray empty = new JSONArray();
                    what.put("sport", empty);
                    what.put("region", empty);
                    what.put("competition", empty);
                    what.put("game", empty);
                    what.put("market", empty);
                    what.put("event", empty);

                    params.put("what", what);

                    Date today = new Date();
                    today.setHours(today.getHours()-1);
                    Date end_day = new Date();
                    end_day.setHours(23);
                    end_day.setMinutes(59);
                    end_day.setSeconds(59);
                    Date tomorrow = new Date();
                    tomorrow.setDate(today.getDate()+1);
                    tomorrow.setHours(0);
                    tomorrow.setMinutes(0);
                    tomorrow.setSeconds(0);
                    Date end_date = new Date();
                    end_date.setDate(today.getDate()+15);
                    end_date.setHours(0);
                    end_date.setMinutes(0);
                    end_date.setSeconds(0);

                    JSONObject where = new JSONObject();
                    JSONObject game = new JSONObject();
                    JSONArray ands = new JSONArray();
                    JSONObject and1 = new JSONObject();
                    JSONArray and1_cond = new JSONArray();
                    JSONObject t = new JSONObject();
                    t.put("type", new Integer(0));
                    JSONObject now = new JSONObject();
                    JSONObject now_value = new JSONObject();
                    now_value.put("@gte",(int)(today.getTime()/1000));
                    now.put("start_ts", now_value);
                    JSONObject end = new JSONObject();
                    JSONObject end_value = new JSONObject();
                    end_value.put("@lte",(int)(end_day.getTime()/1000));
                    end.put("start_ts", end_value);
                    JSONObject thc = new JSONObject();
                    JSONObject fl = new JSONObject();
                    fl.put("flive",false);
                    thc.put("descr",fl);
                    and1_cond.add(t);
                    and1_cond.add(now);
                    and1_cond.add(end);
                    and1_cond.add(thc);
                    and1.put("@and", and1_cond);
                    ands.add(and1);
                    JSONObject and2 = new JSONObject();
                    JSONArray and2_cond = new JSONArray();
                    JSONObject tm = new JSONObject();
                    JSONObject tm_value = new JSONObject();
                    tm_value.put("@gte",(int)(tomorrow.getTime()/1000));
                    tm.put("start_ts", tm_value);
                    JSONObject aw = new JSONObject();
                    JSONObject aw_value = new JSONObject();
                    aw_value.put("@lte",(int)(end_date.getTime()/1000));
                    aw.put("start_ts", aw_value);
                    and2_cond.add(t);
                    and2_cond.add(tm);
                    and2_cond.add(aw);
                    and2.put("@and", and2_cond);
                    ands.add(and2);
                    JSONObject and3 = new JSONObject();
                    JSONArray and3_cond = new JSONArray();
                    and3_cond.add(t);
                    and3_cond.add(now);
                    and3_cond.add(end);
                    and3.put("@and", and3_cond);
                    ands.add(and3);
                    game.put("@or", ands);
                    JSONObject id = new JSONObject();
                    id.put("id", Long.parseLong(rcvd.get("cid").toString()));
                    where.put("game", game);
                    where.put("competition", id);

                    params.put("where",where);
                    params.put("subscribe", false);

                    get_info.put("params", params);

                    main.client.sendMessage(get_info.toString());
                }; break;
                case "get_region":
                {
                    //System.out.println(rcvd + " " + new Date());
                    Date when = new Date();
                    String rid = UUID.randomUUID().toString();
                    JSONObject request = new JSONObject();
                    request.put("session", s);
                    request.put("command", "get_region");
                    request.put("tid", rcvd.get("tid"));
                    request.put("when", when);
                    //System.out.println(rcvd.get("tid")+" get_region "+rid);
                    main.requests.put(rid, request);

                    JSONObject get_info = new JSONObject();
                    get_info.put("command","get");
                    get_info.put("rid", rid);

                    JSONObject params = new JSONObject();
                    params.put("source", "betting");

                    JSONObject what = new JSONObject();
                    JSONArray empty = new JSONArray();
                    what.put("sport", empty);
                    what.put("region", empty);
                    what.put("competition", empty);
                    what.put("game", empty);
                    what.put("market", empty);
                    what.put("event", empty);

                    params.put("what", what);

                    Date today = new Date();
                    today.setHours(today.getHours()-1);
                    Date end_day = new Date();
                    end_day.setHours(23);
                    end_day.setMinutes(59);
                    end_day.setSeconds(59);
                    Date tomorrow = new Date();
                    tomorrow.setDate(today.getDate()+1);
                    tomorrow.setHours(0);
                    tomorrow.setMinutes(0);
                    tomorrow.setSeconds(0);
                    Date end_date = new Date();
                    end_date.setDate(today.getDate()+15);
                    end_date.setHours(0);
                    end_date.setMinutes(0);
                    end_date.setSeconds(0);

                    JSONObject where = new JSONObject();
                    JSONObject game = new JSONObject();
                    JSONArray ands = new JSONArray();
                    JSONObject and1 = new JSONObject();
                    JSONArray and1_cond = new JSONArray();
                    JSONObject t = new JSONObject();
                    t.put("type", new Integer(0));
                    JSONObject now = new JSONObject();
                    JSONObject now_value = new JSONObject();
                    now_value.put("@gte",(int)(today.getTime()/1000));
                    now.put("start_ts", now_value);
                    JSONObject end = new JSONObject();
                    JSONObject end_value = new JSONObject();
                    end_value.put("@lte",(int)(end_day.getTime()/1000));
                    end.put("start_ts", end_value);
                    JSONObject thc = new JSONObject();
                    JSONObject fl = new JSONObject();
                    fl.put("flive",false);
                    thc.put("descr",fl);
                    and1_cond.add(t);
                    and1_cond.add(now);
                    and1_cond.add(end);
                    and1_cond.add(thc);
                    and1.put("@and", and1_cond);
                    ands.add(and1);
                    JSONObject and2 = new JSONObject();
                    JSONArray and2_cond = new JSONArray();
                    JSONObject tm = new JSONObject();
                    JSONObject tm_value = new JSONObject();
                    tm_value.put("@gte",(int)(tomorrow.getTime()/1000));
                    tm.put("start_ts", tm_value);
                    JSONObject aw = new JSONObject();
                    JSONObject aw_value = new JSONObject();
                    aw_value.put("@lte",(int)(end_date.getTime()/1000));
                    aw.put("start_ts", aw_value);
                    and2_cond.add(t);
                    and2_cond.add(tm);
                    and2_cond.add(aw);
                    and2.put("@and", and2_cond);
                    ands.add(and2);
                    JSONObject and3 = new JSONObject();
                    JSONArray and3_cond = new JSONArray();
                    and3_cond.add(t);
                    and3_cond.add(now);
                    and3_cond.add(end);
                    and3.put("@and", and3_cond);
                    ands.add(and3);
                    game.put("@or", ands);
                    JSONObject sid = new JSONObject();
                    sid.put("id", Long.parseLong(rcvd.get("sid").toString()));
                    JSONObject id = new JSONObject();
                    id.put("id", Long.parseLong(rcvd.get("rid").toString()));
                    where.put("game", game);
                    where.put("region", id);
                    where.put("sport", sid);

                    params.put("where",where);
                    params.put("subscribe", false);

                    get_info.put("params", params);

                    main.client.sendMessage(get_info.toString());
                }; break;
                case "search":
                {
                    Date when = new Date();
                    String rid = UUID.randomUUID().toString();
                    JSONObject request = new JSONObject();
                    request.put("session", s);
                    request.put("command", "search");
                    request.put("tid", rcvd.get("tid"));
                    request.put("when", when);
                    //System.out.println(rcvd.get("tid")+" search "+rid);
                    main.requests.put(rid, request);

                    JSONObject get_info = new JSONObject();
                    get_info.put("command","get");
                    get_info.put("rid", rid);
                    JSONObject params = new JSONObject();
                    params.put("source", "betting");
                    JSONObject what = new JSONObject();
                    JSONArray empty = new JSONArray();
                    what.put("sport", empty);
                    what.put("region", empty);
                    what.put("competition", empty);
                    what.put("game", empty);
                    what.put("market", empty);
                    what.put("event", empty);
                    params.put("what", what);
                    JSONObject where = new JSONObject();
                    JSONObject game = new JSONObject();
                    game.put("game_number",Integer.parseInt(rcvd.get("alias").toString()));
                    where.put("game", game);
                    params.put("where",where);
                    params.put("subscribe", false);
                    get_info.put("params", params);
                    main.client.sendMessage(get_info.toString());
                }; break;
                case "close_vars":
                {
                    //System.out.println(rcvd);
                    main.updtSessions.get(rcvd.get("tid").toString()).put("gid", "");
                }; break;
                case "add_event":
                {
                    //System.out.println(rcvd);
                    if (rcvd.get("type").toString().equals("1") || rcvd.get("type").toString().equals("0"))
                    {
                        ((ArrayList<String>)main.updtSessions.get(rcvd.get("tid").toString()).get("betslip")).add(rcvd.get("id").toString());
                    }
                }; break;
                case "remove_event":
                {
                    //System.out.println(rcvd);
                    if (rcvd.get("type").toString().equals("1") || rcvd.get("type").toString().equals("0"))
                    {
                        ((ArrayList<String>)main.updtSessions.get(rcvd.get("tid").toString()).get("betslip")).remove(rcvd.get("id").toString());
                    }
                }; break;
                case "get_coeff":
                {
                    Date when = new Date();
                    String rid = UUID.randomUUID().toString();
                    JSONObject request = new JSONObject();
                    request.put("session", s);
                    request.put("command", "get_coeff");
                    request.put("tid", rcvd.get("tid"));
                    request.put("when", when);
                    //System.out.println(rcvd.get("tid")+" get_coeff "+rid);
                    main.requests.put(rid, request);

                    JSONObject get_info = new JSONObject();
                    get_info.put("command","get");
                    get_info.put("rid", rid);
                    JSONObject params = new JSONObject();
                    params.put("source", "betting");
                    JSONObject what = new JSONObject();
                    JSONArray empty = new JSONArray();
                    what.put("event", empty);
                    params.put("what", what);
                    JSONObject where = new JSONObject();
                    JSONObject event = new JSONObject();
                    event.put("id",new Long(rcvd.get("id").toString()));
                    where.put("event", event);
                    params.put("where",where);
                    params.put("subscribe", false);
                    get_info.put("params", params);
                    main.client.sendMessage(get_info.toString());
                }; break;
            }
        } catch (Exception e) {

        }
    }
}
