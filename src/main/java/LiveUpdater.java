import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.eclipse.jetty.websocket.api.Session;
import org.glassfish.tyrus.client.ClientManager;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.StringWriter;
import java.math.RoundingMode;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LiveUpdater extends Thread {

    public List<Integer> sport_ids;
    public Map<String, JSONObject> data= new ConcurrentHashMap<String,JSONObject>();
    public String port = "";
    public boolean isStarted = false;
    ws_client client = new ws_client("Live updater");
    JSONParser parser = new JSONParser();
    boolean update = false;
    String opt;
    CountDownLatch latch = null;

    private static final Map<String, String> sportPartsT;
    private static final Map<String, String> sportPartsGer;
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

    LiveUpdater (List<Integer> sports, String p, String opt, CountDownLatch l) {

        this.latch = l;
        sport_ids = new ArrayList<Integer>();
        sport_ids.addAll(sports);
        port = p;
        this.opt = opt;

        client.addMessageHandler(new ws_client.MessageHandler() {
            public void handleMessage(String message) {
                JSONParser parser = new JSONParser();
                JSONObject rcvd;
                try
                {
                    rcvd = (JSONObject) parser.parse(message);
                    if (rcvd.containsKey("rid")) {
                        if (!rcvd.get("rid").toString().isEmpty()) {
                            JSONObject data = (JSONObject) rcvd.get("data");
                            if (data.containsKey("data")) {
                                JSONObject data2 = (JSONObject) data.get("data");
                                JSONObject tdata = new JSONObject();
                                tdata.put("type", "data");
                                tdata.put("id",rcvd.get("rid"));
                                tdata.put("data", data2);
                                getQueue().put(tdata);
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
    }

    BlockingQueue<JSONObject> queue = new LinkedBlockingQueue<JSONObject>();

    BlockingQueue<JSONObject> getQueue(){
        return queue;
    }

    public void run() {
        try {
            isStarted = true;
            while (!interrupted()) {
                if (!queue.isEmpty()) {
                    JSONObject qd = queue.take();
                    switch ((String) qd.get("type")) {
                        case "start": {
                            ClientManager cm = ClientManager.createClient();
                            cm.connectToServer(client, new URI("ws://swarm.solidarbet.com:"+port));

                            JSONObject get_info = new JSONObject();
                            get_info.put("command","get");
                            get_info.put("rid", new Integer(1));
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
                            game.put("type",new Integer(1));
                            where.put("game", game);
						    JSONObject sport = new JSONObject();
						    JSONObject sids = new JSONObject();
						    sids.put(opt, sport_ids);
						    sport.put("id",sids);
						    where.put("sport", sport);
                            params.put("where",where);
                            params.put("subscribe", false);
                            get_info.put("params", params);
                            //System.out.println("-----------"+port+"------"+get_info);
                            client.sendMessage(get_info.toString());
                        }; break;
                        case "data": {
                            JSONObject narr_data = (JSONObject)qd.get("data");
                            JSONObject sport = (JSONObject)narr_data.get("sport");
                            if (!update) build_data(sport.keySet(),sport);
                            else form_data_update(sport.keySet(), sport);
                        }; break;
                    }
                }
            }
        }
        catch (Exception e) {
            main.errorLogger.error("Error happened", e);
        }
        finally {
            isStarted = false;
        }
    }

    public void build_data(Set<String> sids_u, JSONObject sport)
    {
        try
        {
            for (String sid : sids_u)
            {
                JSONObject sport_node = new JSONObject();
                JSONObject region_node = new JSONObject();
                sport_node = (JSONObject)sport.get(sid);
                JSONObject region = new JSONObject();
                region = (JSONObject) sport_node.get("region");
                Set<String> region_ids = region.keySet();
                List<JSONObject> comps_arr = new ArrayList<JSONObject>();
                List<JSONObject> games_arr = new ArrayList<JSONObject>();
                Map<String,JSONObject> regions = new HashMap<String,JSONObject>();
                for (String rid : region_ids)
                {
                    region_node = (JSONObject)region.get(rid);
                    JSONObject comp = new JSONObject();
                    comp = (JSONObject) region_node.get("competition");
                    Set<String> comp_ids = comp.keySet();
                    List<JSONObject> cl = new ArrayList<JSONObject>();
                    Map<String,JSONObject> cmps = new HashMap<String,JSONObject>();
                    for(String cid : comp_ids)
                    {
                        JSONObject comp_node = new JSONObject();
                        comp_node = (JSONObject)comp.get(cid);
                        JSONObject game = new JSONObject();
                        game = (JSONObject) comp_node.get("game");
                        Set<String> game_ids = game.keySet();
                        int c_l = 0;
                        Map<String,JSONObject> gms = new HashMap<String,JSONObject>();
                        for (String gid : game_ids)
                        {
                            JSONObject game_node = new JSONObject();
                            game_node = (JSONObject)game.get(gid);
                            JSONObject add = new JSONObject();
                            add.put("_id", gid);
                            add.put("cid", cid);
                            add.put("sid", sid);
                            if (game_node.containsKey("start_ts")) add.put("start", game_node.get("start_ts").toString());
                            else {
                                Date date = new Date();
                                add.put("start", new Integer((int)(date.getTime()/1000)).toString());
                            }
                            add.put("type", new Integer(Integer.parseInt(game_node.get("type").toString())));
                            if (game_node.containsKey("game_number")) add.put("alias", new Integer(Integer.parseInt(game_node.get("game_number").toString())));
                            else add.put("alias",gid);
                            if (game_node.containsKey("team2_name")){
                                add.put("team1",game_node.get("team1_name"));
                                add.put("team2",game_node.get("team2_name"));
                            }
                            else add.put("team1",game_node.get("team1_name"));
                            if (game_node.containsKey("markets_count")) add.put("mc", new Integer(Integer.parseInt(game_node.get("markets_count").toString())));
                            if (game_node.containsKey("game_external_id")) add.put("external-id", game_node.get("game_external_id").toString());
                            String st = "";
                            String tm = "";

                            if (game_node.containsKey("info"))
                            {
                                JSONObject info = new JSONObject();
                                info = (JSONObject)game_node.get("info");
                                if (sid.equals("844"))
                                {
                                    add.put("short1_color", info.get("short1_color"));
                                    add.put("shirt1_color", info.get("shirt1_color"));
                                    add.put("short2_color", info.get("short2_color"));
                                    add.put("shirt2_color", info.get("shirt2_color"));
                                }
                                if (info.containsKey("current_game_state"))
                                {
                                    String state = (String)info.get("current_game_state");
                                    add.put("state", (String)info.get("current_game_state"));
                                    if (state.toLowerCase().contains("set") && sid.equals("844")) add.put("true_state", (state.substring(3)+". Hälfte"));
                                    else if (state.toLowerCase().contains("set")){
                                        if (sportPartsGer.containsKey(sid)) add.put("true_state", (state.substring(3)+". "+sportPartsGer.get(sid)));
                                        else add.put("true_state", (state.substring(3)+". Satz"));
                                    }
                                    else if (state.toLowerCase().equals("halftime")) add.put("true_state", "HZ");
                                    else if (state.toLowerCase().equals("timeout")) add.put("true_state", "HZ");
                                    else add.put("true_state", "1. Satz");
                                    st = add.get("true_state").toString();
                                }
                                else if (sid.equals("844"))
                                {
                                    String state = "set1";
                                    add.put("state", state);
                                    if (state.toLowerCase().contains("set") && sid.equals("844")) add.put("true_state", (state.substring(3)+". Hälfte"));
                                    else if (state.toLowerCase().contains("set")) add.put("true_state", (state.substring(3)+". Satz"));
                                    else if (state.toLowerCase().contains("game")) add.put("true_state", (state.substring(3)+". Game"));
                                    else if (state.toLowerCase().equals("halftime")) add.put("true_state", "HZ");
                                    else if (state.toLowerCase().equals("timeout")) add.put("true_state", "HZ");
                                    else add.put("true_state", "1. Satz");
                                    st = add.get("true_state").toString();
                                }
                                if (info.containsKey("current_game_time"))
                                {
                                    String time = (String)info.get("current_game_time");
                                    if (!sid.equals("844"))
                                    {
                                        if (time.contains("set"))
                                        {
                                            st = time.substring(4,time.length());
                                            //st = time.substring(4,4)+" .Satz";
                                            add.put("time", time);
                                            add.put("true_time", st);
                                        }
                                    }
                                    else
                                    {
                                        if (time.contains("-")) add.put("time", "");
                                        else add.put("time", time);
                                    }
                                }
                                else
                                {
                                    add.put("true_time", tm);
                                    if (sid.equals("844")) add.put("time", "0'");
                                }
                                if (info.containsKey("score1")) add.put("score1",Integer.parseInt((String)info.get("score1")));
                                if (info.containsKey("score2")) add.put("score2",Integer.parseInt((String)info.get("score2")));
                            }
                            else if (sid.equals("844"))
                            {
                                String state = "set1";
                                add.put("state", state);
                                if (state.toLowerCase().contains("set") && sid.equals("844")) add.put("true_state", (state.substring(3)+". Hälfte"));
                                else if (state.toLowerCase().contains("set")) add.put("true_state", (state.substring(3)+". Satz"));
                                else if (state.toLowerCase().contains("game")) add.put("true_state", (state.substring(3)+". Game"));
                                else if (state.toLowerCase().equals("halftime")) add.put("true_state", "HZ");
                                else if (state.toLowerCase().equals("timeout")) add.put("true_state", "HZ");
                                st = add.get("true_state").toString();
                                add.put("time", "0'");
                                add.put("true_time", "0'");
                            }
                            if (sid.equals("844"))
                            {
                                if (game_node.containsKey("last_event"))add.put("last_event", game_node.get("last_event"));
                                else add.put("last_event", new JSONObject());
                            }
                            if (sid.equals("848") || sid.equals("884") || sid.equals("850") || sid.equals("852") || sid.equals("856") || sid.equals("846"))
                            {
                                JSONObject stats = (JSONObject)game_node.get("stats");
                                List<JSONObject> scoreboard = new ArrayList<JSONObject>();
                                int sets = 1;
                                int max = 0;
                                switch (sid){
                                    case "848": max = 6; break;
                                    case "884": max = 9; break;
                                    case "850": max = 4; break;
                                    case "852": max = 5; break;
                                    case "856": max = 9; break;
                                    case "846": max = 5; break;
                                }
                                if (stats != null) {
                                    while (sets<max+1){
                                        if (stats.containsKey("score_set"+sets)){
                                            JSONObject set_score = (JSONObject)stats.get("score_set"+sets);
                                            JSONObject score = new JSONObject();
                                            score.put("team1", set_score.get("team1_value").toString());
                                            score.put("team2", set_score.get("team2_value").toString());
                                            scoreboard.add(score);
                                        }
                                        sets++;
                                    }
                                }
                                add.put("scoreboard", scoreboard);
                            }
                            if (game_node.containsKey("live_events"))
                            {
                                JSONArray live_events = (JSONArray)game_node.get("live_events");
                                List<JSONObject> les = new ArrayList<JSONObject>();
                                int rt1 = 0, rt2 = 0, yt1 = 0, yt2 = 0;
                                for (int i = 0; i<live_events.size(); i++)
                                {
                                    JSONObject le = (JSONObject)live_events.get(i);
                                    JSONObject nle = new JSONObject();
                                    if (le.containsKey("event_type")) nle.put("type", le.get("event_type"));
                                    if (le.containsKey("team")) nle.put("team", le.get("team"));
                                    if (le.containsKey("add_info")) nle.put("time", le.get("add_info"));
                                    les.add(nle);
                                    if (le.containsKey("event_type"))
                                    {
                                        if (((String)le.get("event_type")).equals("yellow_card"))
                                        {
                                            switch ((String)le.get("team"))
                                            {
                                                case "team1": yt1++; break;
                                                case "team2": yt2++; break;
                                            }
                                        }
                                        if (((String)le.get("event_type")).equals("red_card"))
                                        {
                                            switch ((String)le.get("team"))
                                            {
                                                case "team1": rt1++; break;
                                                case "team2": rt2++; break;
                                            }
                                        }
                                    }
                                }

                                add.put("red_1", rt1);
                                add.put("red_2", rt2);
                                add.put("yel_1", yt1);
                                add.put("yel_2", yt2);
                                add.put("live_events", les);
                            }
                            else add.put("live_events", new ArrayList());
                            if (game_node.containsKey("exclude_ids")) {
                                if (game_node.get("exclude_ids") != null)
                                {
                                    if (!game_node.get("exclude_ids").getClass().isArray()) add.put("excl_id", game_node.get("exclude_ids").toString());
                                    else add.put("excl_id", game_node.get("exclude_ids"));
                                }
                            }
                            if (sid.equals("844")) {
                                if (game_node.containsKey("text_info")) {
                                    if (game_node.get("text_info") != null) {
                                        String text_info = game_node.get("text_info").toString();
                                        Pattern pattern = Pattern.compile("(HT)(?=[\\s;])|(HT)$");
                                        Matcher matcher = pattern.matcher(text_info);
                                        if (matcher.find()) {
                                            add.put("true_time", "HZ");
                                        }
                                        else {
                                            pattern = Pattern.compile("(\\d+)(\\+)(\\d)(?=[`'\"])");
                                            matcher = pattern.matcher(text_info);
                                            if (matcher.find()) {
                                                add.put("true_time", matcher.group(0));
                                            }
                                            else {
                                                pattern = Pattern.compile("(\\d+)(?=[`'\"])");
                                                matcher = pattern.matcher(text_info);
                                                if (matcher.find()) {
                                                    add.put("true_time", matcher.group());
                                                }
                                            }
                                        }
                                    }
                                    else if (game_node.containsKey("last_event")) {
                                        JSONObject le = (JSONObject) game_node.get("last_event");
                                        if (le.containsKey("info")) {
                                            String text_info = le.get("info").toString();
                                            Pattern pattern = Pattern.compile("(HT)(?=[\\s;])|(HT)$");
                                            Matcher matcher = pattern.matcher(text_info);
                                            if (matcher.find()) {
                                                add.put("true_time", "HZ");
                                            }
                                            else {
                                                pattern = Pattern.compile("(\\d+)(\\+)(\\d)(?=[`'\"])");
                                                matcher = pattern.matcher(text_info);
                                                if (matcher.find()) {
                                                    add.put("true_time", matcher.group(0));
                                                }
                                                else {
                                                    pattern = Pattern.compile("(\\d+)(?=[`'\"])");
                                                    matcher = pattern.matcher(text_info);
                                                    if (matcher.find()) {
                                                        add.put("true_time", matcher.group());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                else if (game_node.containsKey("last_event")) {
                                    JSONObject le = (JSONObject) game_node.get("last_event");
                                    if (le.containsKey("info")) {
                                        String text_info = le.get("info").toString();
                                        Pattern pattern = Pattern.compile("(HT)(?=[\\s;])|(HT)$");
                                        Matcher matcher = pattern.matcher(text_info);
                                        if (matcher.find()) {
                                            add.put("true_time", "HZ");
                                        }
                                        else {
                                            pattern = Pattern.compile("(\\d+)(\\+)(\\d)(?=[`'\"])");
                                            matcher = pattern.matcher(text_info);
                                            if (matcher.find()) {
                                                add.put("true_time", matcher.group(0));
                                            }
                                            else {
                                                pattern = Pattern.compile("(\\d+)(?=[`'\"])");
                                                matcher = pattern.matcher(text_info);
                                                if (matcher.find()) {
                                                    add.put("true_time", matcher.group());
                                                }
                                            }
                                        }
                                    }
                                }
                            }


                            JSONObject market = new JSONObject();
                            market = (JSONObject)game_node.get("market");
                            Set<String> market_ids = market.keySet();
                            //List<Document> mgr = new ArrayList<Document>();
                            List<JSONObject> gr1 = new ArrayList<JSONObject>();
                            List<JSONObject> gr2 = new ArrayList<JSONObject>();
                            List<JSONObject> gr3 = new ArrayList<JSONObject>();
                            List<JSONObject> gr4 = new ArrayList<JSONObject>();
                            List<JSONObject> total = new ArrayList<JSONObject>();
                            List<JSONObject> total2 = new ArrayList<JSONObject>();
                            Map<String, JSONObject> mkts = new HashMap<String, JSONObject>();
                            for (String mid : market_ids)
                            {
                                JSONObject market_node= new JSONObject ();
                                market_node = (JSONObject) market.get(mid);
                                JSONObject nme = new JSONObject();
                                nme.put("_id",mid);
                                nme.put("gid",gid);
                                if (market_node.containsKey("express_id")) nme.put("exp_id", market_node.get("express_id").toString());

                                if (market_node.containsKey("name")) {
                                    nme.put("name", market_node.get("name").toString());
                                }
                                else {
                                    nme.put("name", "No name");
                                }

                                if (market_node.containsKey("type")) {
                                    nme.put("type", market_node.get("type").toString());
                                }
                                else {
                                    nme.put("type", "No type");
                                }

                                if (market_node.containsKey("base"))
                                {
                                    DecimalFormat df = new DecimalFormat("#.##");
                                    df.setRoundingMode(RoundingMode.CEILING);
                                    nme.put("base",df.format(Double.parseDouble(market_node.get("base").toString())).replaceAll(",", "."));
                                }
                                else {
                                    nme.put("base", "@");
                                }

                                String mName = nme.get("name").toString();
                                String [] mNameArr = mName.split("( +)");
                                mName = String.join(" ", mNameArr);
                                String mType = nme.get("type").toString();
                                String [] mTypeArr = mType.split("( +)");
                                mType = String.join(" ", mTypeArr);
                                String mBase = nme.get("base").toString();
                                JSONObject gerMarket = new JSONObject();

                                if (main.gerMarkets.containsKey(mType)) {
                                    gerMarket = (JSONObject) main.gerMarkets.get(mType);
                                }	else if (main.gerMarkets.containsKey(mType + " " + mBase)){
                                    gerMarket = (JSONObject) main.gerMarkets.get(mType + " " + mBase);
                                }
                                else if (main.gerMarkets.containsKey(mName)){
                                    gerMarket = (JSONObject) main.gerMarkets.get(mName);
                                }
                                else if (main.gerMarkets.containsKey(mName + " " + mBase)){
                                    gerMarket = (JSONObject) main.gerMarkets.get(mName + " " + mBase);
                                }

                                if (gerMarket.containsKey("name")) {
                                    if (!mBase.equals("@")) nme.put("name", gerMarket.get("name") + " " + mBase);
                                    else nme.put("name", gerMarket.get("name"));
                                }
                                else if (!mBase.equals("@"))nme.put("name", mName + " " + mBase);
                                else nme.put("name", mName);

                                if (market_node.containsKey("order")) nme.put("order",market_node.get("order").toString());
                                else nme.put("order", "999");

                                if (gerMarket.containsKey("bases")) {
                                    JSONObject bases = (JSONObject) gerMarket.get("bases");
                                    if (bases.containsKey(mBase)) {
                                        nme.put("order", bases.get(mBase));
                                    }
                                    else if (gerMarket.containsKey("order")) nme.put("order", gerMarket.get("order").toString());
                                }
                                else if (gerMarket.containsKey("order")) nme.put("order", gerMarket.get("order").toString());

                                if (market_node.containsKey("show_type_DISABLE")){
                                    JSONObject group = new JSONObject();
                                    group.put("type", market_node.get("show_type").toString());
                                    group.put("alias", market_node.get("show_type").toString());
                                    group.put("order", 1);
                                    nme.put("group", group);
                                }
                                else {
                                    if (main.marketGroup.containsKey(nme.get("type").toString()))
                                    {
                                        nme.put("group", main.marketGroup.get(nme.get("type").toString()));
                                    }
                                    else
                                    {
                                        if (main.marketGroup.containsKey(nme.get("name").toString()))
                                        {
                                            nme.put("group", main.marketGroup.get(nme.get("name").toString()));
                                        }
                                        else
                                        {
                                            JSONObject group = new JSONObject();
                                            group.put("type", "NO GROUP");
                                            group.put("alias", "NO GROUP");
                                            group.put("order", "999");
                                            nme.put("group", group);
                                        }
                                    }
                                }
                                JSONObject event = new JSONObject();
                                event = (JSONObject)market_node.get("event");
                                Set<String> event_ids = event.keySet();
                                List<JSONObject> evnts = new ArrayList<JSONObject> ();
                                Map<String, JSONObject> evnt = new HashMap<String, JSONObject>();
                                for (String eid : event_ids)
                                {
                                    JSONObject event_node = new JSONObject();
                                    event_node = (JSONObject) event.get(eid);
                                    JSONObject ne = new JSONObject();
                                    ne.put("_id", eid);
                                    ne.put("gid", gid);
                                    ne.put("mid", mid);
                                    if (event_node.containsKey("type"))
                                        ne.put("type", event_node.get("type"));
                                    else
                                        ne.put("type", "No type");
                                    if (event_node.containsKey("name"))
                                        ne.put("name", event_node.get("name"));
                                    else
                                        ne.put("name", "No name");

                                    String eType = ne.get("type").toString();
                                    String [] eTypeArr = eType.split("( +)");
                                    eType = String.join(" ", eTypeArr);
                                    String eName = ne.get("name").toString();
                                    String [] eNameArr = eName.split("( +)");
                                    eName = String.join(" ", eNameArr);

                                    if (gerMarket.containsKey(eType)) {
                                        ne.put("name", gerMarket.get(eType));
                                    }
                                    else if (gerMarket.containsKey(eName)) {
                                        ne.put("name", gerMarket.get(eName));
                                    }

                                    if (mType.equals("NextGoal")) {
                                        if (eType.toLowerCase().contains("firstteam")) ne.put("name", "1");
                                        else if (eType.toLowerCase().contains("goal") &&
                                                !eType.toLowerCase().contains("firstteam") &&
                                                !eType.toLowerCase().contains("secondteam")) ne.put("name", "X");
                                        else if (eType.toLowerCase().contains("secondteam")) ne.put("name", "2");
                                    }

                                    if (event_node.containsKey("order")) ne.put("order", new Integer(Integer.parseInt(event_node.get("order").toString())));
                                    else ne.put("order", 999);
                                    if (market_node.containsKey("express_id")) ne.put("exp_id", market_node.get("express_id").toString());
                                    if (event_node.containsKey("price"))
                                    {
                                        DecimalFormat df = new DecimalFormat("#.##");
                                        df.setRoundingMode(RoundingMode.CEILING);
                                        ne.put("price", df.format(Double.parseDouble(event_node.get("price").toString())).toString().replaceAll(",", "."));
                                    }
                                    else {
                                        ne.put("price", "1.01");
                                    }
                                    evnts.add(ne);
                                }
                                Collections.sort(evnts, new Comparator<JSONObject>() {
                                    @Override
                                    public int compare(JSONObject o1, JSONObject o2) {
                                        return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                                    }
                                });
                                List<String> eIds = new ArrayList<String>();
                                for (JSONObject ne : evnts)
                                {
                                    evnt.put(ne.get("_id").toString(),ne);
                                    eIds.add(ne.get("_id").toString());
                                }
                                VelocityContext mv = new VelocityContext();
                                StringWriter mvr = new StringWriter();
                                mv.put("market", nme);
                                mv.put("events", evnt);
                                mv.put("eIds", eIds);
                                Velocity.mergeTemplate("market.vm", StandardCharsets.UTF_8.name(), mv, mvr);
                                nme.put("html", mvr.toString());
                                nme.put("events", evnt);
                                if(nme.containsKey("name"))
                                {
                                    if (!nme.get("name").toString().toLowerCase().contains("asian"))mkts.put(mid, nme);
                                    else add.put("mc", new Integer(Integer.parseInt(add.get("mc").toString()))-1);
                                }
                                else add.put("mc", new Integer(Integer.parseInt(add.get("mc").toString()))-1);

                            }
                            games_arr.add(add);
                            add.put("markets",mkts);
                            gms.put(gid, add);
                        }
                        JSONObject cmp = new JSONObject();
                        cmp.put("_id", cid);
                        cmp.put("rsid", sid+rid);
                        cmp.put("sid", sid);
                        cmp.put("name", comp_node.get("name"));
                        cmp.put("games", gms);
                        cmps.put(cid,cmp);
                    }
                    JSONObject rgn = new JSONObject();

                    rgn.put("comps", cmps);
                    if (region_node.containsKey("alias"))
                    {
                        rgn.put("alias", region_node.get("alias").toString());
                    }
                    else {
                        if (region_node.containsKey("name")) {
                            rgn.put("alias", region_node.get("name").toString());
                        }
                        else {
                            rgn.put("alias", "No alias");
                        }
                    }
                    regions.put(rid, rgn);

                }
                JSONObject sdata = new JSONObject();
                sdata.put("_id", sid);
                sdata.put("alias", sport_node.get("alias"));
                sdata.put("name", sport_node.get("name"));
                switch (sdata.get("alias").toString()) {
                    case "Soccer":
                        sdata.put("order", 1);
                        break;
                    case "IceHockey":
                        sdata.put("order", 2);
                        break;
                    case "Volleyball":
                        sdata.put("order", 3);
                        break;
                    case "Basketball":
                        sdata.put("order", 4);
                        break;
                    case "Tennis":
                        sdata.put("order", 5);
                        break;
                    case "TableTennis":
                        sdata.put("order", 6);
                        break;
                    case "Badminton":
                        sdata.put("order", 7);
                        break;
                    default:
                        sdata.put("order", 999);
                        break;
                }
                sdata.put("regions", regions);
                this.data.put(sid, sdata);
            }
            update = true;
            JSONObject get_info = new JSONObject();
            get_info.put("command","get");
            get_info.put("rid", new Integer(1));
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
            game.put("type",new Integer(1));
            where.put("game", game);
            JSONObject sp_where = new JSONObject();
            JSONObject sids = new JSONObject();
            sids.put(opt, sport_ids);
            sp_where.put("id",sids);
            where.put("sport", sp_where);
            params.put("where",where);
            params.put("subscribe", false);
            get_info.put("params", params);
            client.sendMessage(get_info.toString());

            this.latch.countDown();
            System.out.println("Live with sports:" +sport_ids+ " and opt " +opt+ " has data. Counter:"+this.latch.getCount());
        }
        catch (Exception e)
        {
            main.errorLogger.error("Error happened "+sport_ids, e);
            e.printStackTrace();
        }
    }

    public void form_data_update(Set<String> sids_u, JSONObject sport)
    {
        Map<String, JSONObject> udata = new ConcurrentHashMap<String, JSONObject>();
        try
        {
            for (String sid : sids_u)
            {
                JSONObject sport_node = new JSONObject();
                JSONObject region_node = new JSONObject();
                sport_node = (JSONObject)sport.get(sid);
                JSONObject region = new JSONObject();
                region = (JSONObject) sport_node.get("region");
                Set<String> region_ids = region.keySet();
                List<JSONObject> comps_arr = new ArrayList<JSONObject>();
                List<JSONObject> games_arr = new ArrayList<JSONObject>();
                Map<String,JSONObject> regions = new HashMap<String,JSONObject>();
                for (String rid : region_ids)
                {
                    region_node = (JSONObject)region.get(rid);
                    JSONObject comp = new JSONObject();
                    comp = (JSONObject) region_node.get("competition");
                    Set<String> comp_ids = comp.keySet();
                    List<JSONObject> cl = new ArrayList<JSONObject>();
                    Map<String,JSONObject> cmps = new HashMap<String,JSONObject>();
                    for(String cid : comp_ids)
                    {
                        JSONObject comp_node = new JSONObject();
                        comp_node = (JSONObject)comp.get(cid);
                        JSONObject game = new JSONObject();
                        game = (JSONObject) comp_node.get("game");
                        Set<String> game_ids = game.keySet();
                        int c_l = 0;
                        Map<String,JSONObject> gms = new HashMap<String,JSONObject>();
                        for (String gid : game_ids)
                        {
                            JSONObject game_node = new JSONObject();
                            game_node = (JSONObject)game.get(gid);
                            JSONObject add = new JSONObject();
                            add.put("_id", gid);
                            add.put("cid", cid);
                            add.put("sid", sid);
                            add.put("start", game_node.get("start_ts").toString());
                            add.put("type", new Integer(Integer.parseInt(game_node.get("type").toString())));
                            add.put("alias", new Integer(Integer.parseInt(game_node.get("game_number").toString())));
                            if (game_node.containsKey("team2_name")){
                                add.put("team1",game_node.get("team1_name"));
                                add.put("team2",game_node.get("team2_name"));
                            }
                            else add.put("team1",game_node.get("team1_name"));
                            if (game_node.containsKey("markets_count")) add.put("mc", new Integer(Integer.parseInt(game_node.get("markets_count").toString())));
                            if (game_node.containsKey("game_external_id")) add.put("external-id", game_node.get("game_external_id").toString());
                            String st = "";
                            String tm = "";

                            if (game_node.containsKey("info"))
                            {
                                JSONObject info = new JSONObject();
                                info = (JSONObject)game_node.get("info");
                                if (sid.equals("844"))
                                {
                                    add.put("short1_color", info.get("short1_color"));
                                    add.put("shirt1_color", info.get("shirt1_color"));
                                    add.put("short2_color", info.get("short2_color"));
                                    add.put("shirt2_color", info.get("shirt2_color"));
                                }
                                if (info.containsKey("current_game_state"))
                                {
                                    String state = (String)info.get("current_game_state");
                                    add.put("state", (String)info.get("current_game_state"));
                                    if (state.toLowerCase().contains("set") && sid.equals("844")) add.put("true_state", (state.substring(3)+". Hälfte"));
                                    else if (state.toLowerCase().contains("set")){
                                        if (sportPartsGer.containsKey(sid)) add.put("true_state", (state.substring(3)+". "+sportPartsGer.get(sid)));
                                        else add.put("true_state", (state.substring(3)+". Satz"));
                                    }
                                    else if (state.toLowerCase().equals("halftime")) add.put("true_state", "HZ");
                                    else if (state.toLowerCase().equals("timeout")) add.put("true_state", "HZ");
                                    else add.put("true_state", "1. Satz");
                                    st = add.get("true_state").toString();
                                }
                                else if (sid.equals("844"))
                                {
                                    String state = "set1";
                                    add.put("state", state);
                                    if (state.toLowerCase().contains("set") && sid.equals("844")) add.put("true_state", (state.substring(3)+". Hälfte"));
                                    else if (state.toLowerCase().contains("set")) add.put("true_state", (state.substring(3)+". Satz"));
                                    else if (state.toLowerCase().contains("game")) add.put("true_state", (state.substring(3)+". Game"));
                                    else if (state.toLowerCase().equals("halftime")) add.put("true_state", "HZ");
                                    else if (state.toLowerCase().equals("timeout")) add.put("true_state", "HZ");
                                    st = add.get("true_state").toString();
                                }
                                if (info.containsKey("current_game_time"))
                                {
                                    String time = (String)info.get("current_game_time");
                                    if (!sid.equals("844"))
                                    {
                                        if (time.contains("set"))
                                        {
                                            st = time.substring(4,time.length());
                                            //st = time.substring(4,4)+" .Satz";
                                            add.put("time", time);
                                            add.put("true_time", st);
                                        }
                                    }
                                    else
                                    {
                                        if (time.contains("-")) add.put("time", "");
                                        else add.put("time", time);
                                    }
                                }
                                else
                                {
                                    add.put("true_time", tm);
                                    if (sid.equals("844")) add.put("time", "0'");
                                }
                                if (info.containsKey("score1")) add.put("score1",Integer.parseInt((String)info.get("score1")));
                                if (info.containsKey("score2")) add.put("score2",Integer.parseInt((String)info.get("score2")));
                            }
                            else if (sid.equals("844"))
                            {
                                String state = "set1";
                                add.put("state", state);
                                if (state.toLowerCase().contains("set") && sid.equals("844")) add.put("true_state", (state.substring(3)+". Hälfte"));
                                else if (state.toLowerCase().contains("set")) add.put("true_state", (state.substring(3)+". Satz"));
                                else if (state.toLowerCase().contains("game")) add.put("true_state", (state.substring(3)+". Game"));
                                else if (state.toLowerCase().equals("halftime")) add.put("true_state", "HZ");
                                else if (state.toLowerCase().equals("timeout")) add.put("true_state", "HZ");
                                st = add.get("true_state").toString();
                                add.put("time", "0'");
                                add.put("true_time", "0'");
                            }
                            if (sid.equals("844"))
                            {
                                if (game_node.containsKey("last_event"))add.put("last_event", game_node.get("last_event"));
                                else add.put("last_event", new JSONObject());
                            }
                            if (sid.equals("848") || sid.equals("884") || sid.equals("850") || sid.equals("852") || sid.equals("856") || sid.equals("846"))
                            {
                                JSONObject stats = (JSONObject)game_node.get("stats");
                                List<JSONObject> scoreboard = new ArrayList<JSONObject>();
                                int sets = 1;
                                int max = 0;
                                switch (sid){
                                    case "848": max = 6; break;
                                    case "884": max = 9; break;
                                    case "850": max = 4; break;
                                    case "852": max = 5; break;
                                    case "856": max = 9; break;
                                    case "846": max = 5; break;
                                }
                                if (stats != null) {
                                    while (sets<max+1){
                                        if (stats.containsKey("score_set"+sets)){
                                            JSONObject set_score = (JSONObject)stats.get("score_set"+sets);
                                            JSONObject score = new JSONObject();
                                            score.put("team1", set_score.get("team1_value").toString());
                                            score.put("team2", set_score.get("team2_value").toString());
                                            scoreboard.add(score);
                                        }
                                        sets++;
                                    }
                                }
                                add.put("scoreboard", scoreboard);
                            }
                            if (game_node.containsKey("live_events"))
                            {
                                JSONArray live_events = (JSONArray)game_node.get("live_events");
                                List<JSONObject> les = new ArrayList<JSONObject>();
                                int rt1 = 0, rt2 = 0, yt1 = 0, yt2 = 0;
                                for (int i = 0; i<live_events.size(); i++)
                                {
                                    JSONObject le = (JSONObject)live_events.get(i);
                                    JSONObject nle = new JSONObject();
                                    if (le.containsKey("event_type")) nle.put("type", le.get("event_type"));
                                    if (le.containsKey("team")) nle.put("team", le.get("team"));
                                    if (le.containsKey("add_info")) nle.put("time", le.get("add_info"));
                                    les.add(nle);
                                    if (le.containsKey("event_type"))
                                    {
                                        if (((String)le.get("event_type")).equals("yellow_card"))
                                        {
                                            switch ((String)le.get("team"))
                                            {
                                                case "team1": yt1++; break;
                                                case "team2": yt2++; break;
                                            }
                                        }
                                        if (((String)le.get("event_type")).equals("red_card"))
                                        {
                                            switch ((String)le.get("team"))
                                            {
                                                case "team1": rt1++; break;
                                                case "team2": rt2++; break;
                                            }
                                        }
                                    }
                                }

                                add.put("red_1", rt1);
                                add.put("red_2", rt2);
                                add.put("yel_1", yt1);
                                add.put("yel_2", yt2);
                                add.put("live_events", les);
                            }
                            else add.put("live_events", new ArrayList());
                            if (game_node.containsKey("exclude_ids")) {
                                if (game_node.get("exclude_ids") != null)
                                {
                                    if (!game_node.get("exclude_ids").getClass().isArray()) add.put("excl_id", game_node.get("exclude_ids").toString());
                                    else add.put("excl_id", game_node.get("exclude_ids"));
                                }
                            }
                            if (sid.equals("844")) {
                                if (game_node.containsKey("text_info")) {
                                    if (game_node.get("text_info") != null) {
                                        String text_info = game_node.get("text_info").toString();
                                        Pattern pattern = Pattern.compile("(HT)(?=[\\s;])|(HT)$");
                                        Matcher matcher = pattern.matcher(text_info);
                                        if (matcher.find()) {
                                            add.put("true_time", "HZ");
                                        }
                                        else {
                                            pattern = Pattern.compile("(\\d+)(\\+)(\\d)(?=[`'\"])");
                                            matcher = pattern.matcher(text_info);
                                            if (matcher.find()) {
                                                add.put("true_time", matcher.group(0));
                                            }
                                            else {
                                                pattern = Pattern.compile("(\\d+)(?=[`'\"])");
                                                matcher = pattern.matcher(text_info);
                                                if (matcher.find()) {
                                                    add.put("true_time", matcher.group());
                                                }
                                            }
                                        }
                                    }
                                    else if (game_node.containsKey("last_event")) {
                                        JSONObject le = (JSONObject) game_node.get("last_event");
                                        if (le.containsKey("info")) {
                                            String text_info = le.get("info").toString();
                                            Pattern pattern = Pattern.compile("(HT)(?=[\\s;])|(HT)$");
                                            Matcher matcher = pattern.matcher(text_info);
                                            if (matcher.find()) {
                                                add.put("true_time", "HZ");
                                            }
                                            else {
                                                pattern = Pattern.compile("(\\d+)(\\+)(\\d)(?=[`'\"])");
                                                matcher = pattern.matcher(text_info);
                                                if (matcher.find()) {
                                                    add.put("true_time", matcher.group(0));
                                                }
                                                else {
                                                    pattern = Pattern.compile("(\\d+)(?=[`'\"])");
                                                    matcher = pattern.matcher(text_info);
                                                    if (matcher.find()) {
                                                        add.put("true_time", matcher.group());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                else if (game_node.containsKey("last_event")) {
                                    JSONObject le = (JSONObject) game_node.get("last_event");
                                    if (le.containsKey("info")) {
                                        String text_info = le.get("info").toString();
                                        Pattern pattern = Pattern.compile("(HT)(?=[\\s;])|(HT)$");
                                        Matcher matcher = pattern.matcher(text_info);
                                        if (matcher.find()) {
                                            add.put("true_time", "HZ");
                                        }
                                        else {
                                            pattern = Pattern.compile("(\\d+)(\\+)(\\d)(?=[`'\"])");
                                            matcher = pattern.matcher(text_info);
                                            if (matcher.find()) {
                                                add.put("true_time", matcher.group(0));
                                            }
                                            else {
                                                pattern = Pattern.compile("(\\d+)(?=[`'\"])");
                                                matcher = pattern.matcher(text_info);
                                                if (matcher.find()) {
                                                    add.put("true_time", matcher.group());
                                                }
                                            }
                                        }
                                    }
                                }
                            }


                            JSONObject market = new JSONObject();
                            market = (JSONObject)game_node.get("market");
                            Set<String> market_ids = market.keySet();
                            //List<Document> mgr = new ArrayList<Document>();
                            List<JSONObject> gr1 = new ArrayList<JSONObject>();
                            List<JSONObject> gr2 = new ArrayList<JSONObject>();
                            List<JSONObject> gr3 = new ArrayList<JSONObject>();
                            List<JSONObject> gr4 = new ArrayList<JSONObject>();
                            List<JSONObject> total = new ArrayList<JSONObject>();
                            List<JSONObject> total2 = new ArrayList<JSONObject>();
                            Map<String, JSONObject> mkts = new HashMap<String, JSONObject>();
                            for (String mid : market_ids)
                            {
                                JSONObject market_node= new JSONObject ();
                                market_node = (JSONObject) market.get(mid);
                                JSONObject nme = new JSONObject();
                                nme.put("_id",mid);
                                nme.put("gid",gid);
                                if (market_node.containsKey("express_id")) nme.put("exp_id", market_node.get("express_id").toString());

                                if (market_node.containsKey("name")) {
                                    nme.put("name", market_node.get("name").toString());
                                }
                                else {
                                    nme.put("name", "No name");
                                }

                                if (market_node.containsKey("type")) {
                                    nme.put("type", market_node.get("type").toString());
                                }
                                else {
                                    nme.put("type", "No type");
                                }

                                if (market_node.containsKey("base"))
                                {
                                    DecimalFormat df = new DecimalFormat("#.##");
                                    df.setRoundingMode(RoundingMode.CEILING);
                                    nme.put("base",df.format(Double.parseDouble(market_node.get("base").toString())).replaceAll(",", "."));
                                }
                                else {
                                    nme.put("base", "@");
                                }

                                String mName = nme.get("name").toString();
                                String [] mNameArr = mName.split("( +)");
                                mName = String.join(" ", mNameArr);
                                String mType = nme.get("type").toString();
                                String [] mTypeArr = mType.split("( +)");
                                mType = String.join(" ", mTypeArr);
                                String mBase = nme.get("base").toString();
                                JSONObject gerMarket = new JSONObject();

                                if (main.gerMarkets.containsKey(mType)) {
                                    gerMarket = (JSONObject) main.gerMarkets.get(mType);
                                }	else if (main.gerMarkets.containsKey(mType + " " + mBase)){
                                    gerMarket = (JSONObject) main.gerMarkets.get(mType + " " + mBase);
                                }
                                else if (main.gerMarkets.containsKey(mName)){
                                    gerMarket = (JSONObject) main.gerMarkets.get(mName);
                                }
                                else if (main.gerMarkets.containsKey(mName + " " + mBase)){
                                    gerMarket = (JSONObject) main.gerMarkets.get(mName + " " + mBase);
                                }

                                if (gerMarket.containsKey("name")) {
                                    if (!mBase.equals("@")) nme.put("name", gerMarket.get("name") + " " + mBase);
                                    else nme.put("name", gerMarket.get("name"));
                                }
                                else if (!mBase.equals("@"))nme.put("name", mName + " " + mBase);
                                else nme.put("name", mName);

                                if (market_node.containsKey("order")) nme.put("order",market_node.get("order").toString());
                                else nme.put("order", "999");

                                if (gerMarket.containsKey("bases")) {
                                    JSONObject bases = (JSONObject) gerMarket.get("bases");
                                    if (bases.containsKey(mBase)) {
                                        nme.put("order", bases.get(mBase));
                                    }
                                    else if (gerMarket.containsKey("order")) nme.put("order", gerMarket.get("order").toString());
                                }
                                else if (gerMarket.containsKey("order")) nme.put("order", gerMarket.get("order").toString());

                                if (market_node.containsKey("show_type_DISABLE")){
                                    JSONObject group = new JSONObject();
                                    group.put("type", market_node.get("show_type").toString());
                                    group.put("alias", market_node.get("show_type").toString());
                                    group.put("order", 1);
                                    nme.put("group", group);
                                }
                                else {
                                    if (main.marketGroup.containsKey(nme.get("type").toString()))
                                    {
                                        nme.put("group", main.marketGroup.get(nme.get("type").toString()));
                                    }
                                    else
                                    {
                                        if (main.marketGroup.containsKey(nme.get("name").toString()))
                                        {
                                            nme.put("group", main.marketGroup.get(nme.get("name").toString()));
                                        }
                                        else
                                        {
                                            JSONObject group = new JSONObject();
                                            group.put("type", "NO GROUP");
                                            group.put("alias", "NO GROUP");
                                            group.put("order", "999");
                                            nme.put("group", group);
                                        }
                                    }
                                }
                                JSONObject event = new JSONObject();
                                event = (JSONObject)market_node.get("event");
                                Set<String> event_ids = event.keySet();
                                List<JSONObject> evnts = new ArrayList<JSONObject> ();
                                Map<String, JSONObject> evnt = new HashMap<String, JSONObject>();
                                for (String eid : event_ids)
                                {
                                    JSONObject event_node = new JSONObject();
                                    event_node = (JSONObject) event.get(eid);
                                    JSONObject ne = new JSONObject();
                                    ne.put("_id", eid);
                                    ne.put("gid", gid);
                                    ne.put("mid", mid);
                                    if (event_node.containsKey("type"))
                                        ne.put("type", event_node.get("type"));
                                    else
                                        ne.put("type", "No type");
                                    if (event_node.containsKey("name"))
                                        ne.put("name", event_node.get("name"));
                                    else
                                        ne.put("name", "No name");

                                    String eType = ne.get("type").toString();
                                    String [] eTypeArr = eType.split("( +)");
                                    eType = String.join(" ", eTypeArr);
                                    String eName = ne.get("name").toString();
                                    String [] eNameArr = eName.split("( +)");
                                    eName = String.join(" ", eNameArr);

                                    if (gerMarket.containsKey(eType)) {
                                        ne.put("name", gerMarket.get(eType));
                                    }
                                    else if (gerMarket.containsKey(eName)) {
                                        ne.put("name", gerMarket.get(eName));
                                    }

                                    if (mType.equals("NextGoal")) {
                                        if (eType.toLowerCase().contains("firstteam")) ne.put("name", "1");
                                        else if (eType.toLowerCase().contains("goal") &&
                                                !eType.toLowerCase().contains("firstteam") &&
                                                !eType.toLowerCase().contains("secondteam")) ne.put("name", "X");
                                        else if (eType.toLowerCase().contains("secondteam")) ne.put("name", "2");
                                    }

                                    if (event_node.containsKey("order")) ne.put("order", new Integer(Integer.parseInt(event_node.get("order").toString())));
                                    else ne.put("order", 999);
                                    if (market_node.containsKey("express_id")) ne.put("exp_id", market_node.get("express_id").toString());
                                    if (event_node.containsKey("price"))
                                    {
                                        DecimalFormat df = new DecimalFormat("#.##");
                                        df.setRoundingMode(RoundingMode.CEILING);
                                        ne.put("price", df.format(Double.parseDouble(event_node.get("price").toString())).toString().replaceAll(",", "."));
                                    }
                                    else {
                                        ne.put("price", "1.01");
                                    }
                                    evnts.add(ne);
                                }
                                Collections.sort(evnts, new Comparator<JSONObject>() {
                                    @Override
                                    public int compare(JSONObject o1, JSONObject o2) {
                                        return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                                    }
                                });
                                List<String> eIds = new ArrayList<String>();
                                for (JSONObject ne : evnts)
                                {
                                    evnt.put(ne.get("_id").toString(),ne);
                                    eIds.add(ne.get("_id").toString());
                                }
                                VelocityContext mv = new VelocityContext();
                                StringWriter mvr = new StringWriter();
                                mv.put("market", nme);
                                mv.put("events", evnt);
                                mv.put("eIds", eIds);
                                Velocity.mergeTemplate("market.vm", StandardCharsets.UTF_8.name(), mv, mvr);
                                nme.put("html", mvr.toString());
                                nme.put("events", evnt);
                                if(nme.containsKey("name"))
                                {
                                    if (!nme.get("name").toString().toLowerCase().contains("asian"))mkts.put(mid, nme);
                                    else add.put("mc", new Integer(Integer.parseInt(add.get("mc").toString()))-1);
                                }
                                else add.put("mc", new Integer(Integer.parseInt(add.get("mc").toString()))-1);

                            }
                            games_arr.add(add);
                            add.put("markets",mkts);
                            gms.put(gid, add);
                        }
                        JSONObject cmp = new JSONObject();
                        cmp.put("_id", cid);
                        cmp.put("rsid", sid+rid);
                        cmp.put("sid", sid);
                        cmp.put("name", comp_node.get("name"));
                        cmp.put("games", gms);
                        cmps.put(cid,cmp);
                    }
                    JSONObject rgn = new JSONObject();

                    rgn.put("comps", cmps);
                    if (region_node.containsKey("alias"))
                    {
                        rgn.put("alias", region_node.get("alias").toString());
                    }
                    else {
                        if (region_node.containsKey("name")) {
                            rgn.put("alias", region_node.get("name").toString());
                        }
                        else {
                            rgn.put("alias", "No alias");
                        }
                    }
                    regions.put(rid, rgn);

                }
                JSONObject sdata = new JSONObject();
                sdata.put("_id", sid);
                sdata.put("alias", sport_node.get("alias"));
                sdata.put("name", sport_node.get("name"));
                switch ((String)sport_node.get("alias")) {
                    case "Soccer":
                        sdata.put("order", 1);
                        break;
                    case "IceHockey":
                        sdata.put("order", 2);
                        break;
                    case "Volleyball":
                        sdata.put("order", 3);
                        break;
                    case "Basketball":
                        sdata.put("order", 4);
                        break;
                    case "Tennis":
                        sdata.put("order", 5);
                        break;
                    case "TableTennis":
                        sdata.put("order", 6);
                        break;
                    case "Badminton":
                        sdata.put("order", 7);
                        break;
                    default:
                        sdata.put("order", 999);
                        break;
                }
                sdata.put("regions", regions);
                udata.put(sid, sdata);
            }
            if(update) update_data(udata);
        }
        catch (Exception e)
        {
            main.errorLogger.error("Error happened", e);
            e.printStackTrace();
        }
    }

    public void update_data(Map<String,JSONObject> udata)
    {
        Set<String> keys1;
        Set<String> keys2;
        try
        {
            //del_data("sport",udata,this.data);
            keys1 = new HashSet<String>(data.keySet());
            keys2 = new HashSet<String>(udata.keySet());
            keys1.removeAll(keys2);
            if (keys1.size()>0)
            {
                JSONObject obj = new JSONObject();
                obj.put("command", "delete");
                obj.put("what", "sport");
                obj.put("ids", keys1);
                obj.put("type", "live");
                update(obj);
            }
            for (String sid : udata.keySet())
            {
                if (this.data.containsKey(sid))
                {
                    Map<String, JSONObject> rgns_u = (Map<String, JSONObject>) udata.get(sid).get("regions");
                    Map<String, JSONObject> rgns = (Map<String, JSONObject>) this.data.get(sid).get("regions");
                    check_changes("sport", udata.get(sid),this.data.get(sid));
                    keys1 = new HashSet<String>(rgns.keySet());
                    keys2 = new HashSet<String>(rgns_u.keySet());
                    keys1.removeAll(keys2);
                    if (keys1.size()>0)
                    {
                        for (String key : keys1)
                        {
                            JSONObject region = rgns.get(key);
                            Map<String, JSONObject> comps = (Map<String, JSONObject>)region.get("comps");
                            JSONObject obj = new JSONObject();
                            obj.put("command", "delete");
                            obj.put("what", "comp");
                            obj.put("ids", comps.keySet());
                            obj.put("type", "live");
                            update(obj);
                            for (String cid : comps.keySet())
                            {
                                JSONObject comp = comps.get(cid);
                                Map<String, JSONObject> games = (Map<String, JSONObject>)comp.get("games");
                                obj = new JSONObject();
                                obj.put("command", "delete");
                                obj.put("what", "game");
                                obj.put("ids", games.keySet());
                                obj.put("type", "live");
                                update(obj);
                            }
                        }
                    }
                    for (String rid: rgns_u.keySet())
                    {
                        if (rgns.containsKey(rid))
                        {
                            Map<String, JSONObject> comps_u = (Map<String, JSONObject>) rgns_u.get(rid).get("comps");
                            Map<String, JSONObject> comps = (Map<String, JSONObject>) rgns.get(rid).get("comps");
                            //del_data("comp",comps_u,comps);
                            keys1 = new HashSet<String>(comps.keySet());
                            keys2 = new HashSet<String>(comps_u.keySet());
                            keys1.removeAll(keys2);
                            if (keys1.size()>0)
                            {
                                JSONObject obj = new JSONObject();
                                obj.put("command", "delete");
                                obj.put("what", "comp");
                                obj.put("ids", keys1);
                                obj.put("type", "live");
                                update(obj);
                                for (String key : keys1)
                                {
                                    JSONObject comp = comps.get(key);
                                    Map<String, JSONObject> games = (Map<String, JSONObject>)comp.get("games");
                                    obj = new JSONObject();
                                    obj.put("command", "delete");
                                    obj.put("what", "game");
                                    obj.put("ids", games.keySet());
                                    obj.put("type", "live");
                                    update(obj);
                                }
                            }
                            for(String cid : comps_u.keySet())
                            {
                                if (comps.containsKey(cid))
                                {
                                    Map<String, JSONObject> gms_u = (Map<String, JSONObject>) comps_u.get(cid).get("games");
                                    Map<String, JSONObject> gms = (Map<String, JSONObject>) comps.get(cid).get("games");
                                    check_changes("comp", comps_u.get(cid),comps.get(cid));
                                    //del_data("game",gms_u,gms);
                                    keys1 = new HashSet<String>(gms.keySet());
                                    keys2 = new HashSet<String>(gms_u.keySet());
                                    keys1.removeAll(keys2);
                                    if (keys1.size()>0)
                                    {
                                        JSONObject obj = new JSONObject();
                                        obj.put("command", "delete");
                                        obj.put("what", "game");
                                        obj.put("ids", keys1);
                                        obj.put("type", "live");
                                        update(obj);
                                    }
                                    for (String gid : gms_u.keySet())
                                    {
                                        if (gms.containsKey(gid))
                                        {
                                            //check_changes("game",gms_u.get(gid),gms.get(gid));
                                            if (sid.equals("844")) check_game_param("true_time", gms.get(gid), gms_u.get(gid), gid);
                                            else check_game_param("time", gms.get(gid), gms_u.get(gid), gid);
                                            check_game_param("state", gms.get(gid), gms_u.get(gid), gid);
                                            check_game_param("score1", gms.get(gid), gms_u.get(gid), gid);
                                            check_game_param("score2", gms.get(gid), gms_u.get(gid), gid);
                                            check_game_param("yel_1", gms.get(gid), gms_u.get(gid), gid);
                                            check_game_param("yel_2", gms.get(gid), gms_u.get(gid), gid);
                                            check_game_param("red_1", gms.get(gid), gms_u.get(gid), gid);
                                            check_game_param("red_2", gms.get(gid), gms_u.get(gid), gid);
                                            check_game_param("mc", gms.get(gid), gms_u.get(gid), gid);
                                            if(sid.equals("844"))
                                            {
                                                compare_last_event((JSONObject)gms.get(gid).get("last_event"), (JSONObject)gms_u.get(gid).get("last_event"), gid);
                                                compare_live_events((List<JSONObject>)gms.get(gid).get("live_events"),(List<JSONObject>)gms_u.get(gid).get("live_events"), gid);
                                            }
                                            if (sid.equals("848") || sid.equals("884") || sid.equals("850") || sid.equals("852")) {
                                                if (!gms.get(gid).containsKey("scoreboard") && gms_u.get(gid).containsKey("scoreboard")){
                                                    JSONObject obj = new JSONObject();
                                                    List<JSONObject> scrbrd_u = (List<JSONObject>)gms_u.get(gid).get("scoreboard");
                                                    List<JSONObject> scrbrd = new ArrayList<JSONObject>();
                                                    for (JSONObject score : scrbrd_u){
                                                        obj.put("command", "update");
                                                        obj.put("what", "game");
                                                        obj.put("type", "live");
                                                        obj.put("id", gid);
                                                        JSONObject scr = new JSONObject();
                                                        scr.put("state", "set"+scrbrd_u.indexOf(score)+1);
                                                        scr.put("score1", score.get("team1"));
                                                        scr.put("score2", score.get("team2"));
                                                        scrbrd.add(scr);
                                                    }
                                                    obj.put("param", "scoreboard");
                                                    obj.put("value",scrbrd);
                                                    update(obj);
                                                }
                                                else if (gms.get(gid).containsKey("scoreboard") && gms_u.get(gid).containsKey("scoreboard")){
                                                    List<JSONObject> scrbrd = (List<JSONObject>)gms.get(gid).get("scoreboard");
                                                    List<JSONObject> scrbrd_u = (List<JSONObject>)gms_u.get(gid).get("scoreboard");
                                                    if (scrbrd_u.size()>scrbrd.size()){
                                                        JSONObject obj = new JSONObject();
                                                        List<JSONObject> scrbrd_c = new ArrayList<JSONObject>();
                                                        for (int i = scrbrd.size(); i<scrbrd_u.size(); i++){
                                                            obj.put("command", "update");
                                                            obj.put("what", "game");
                                                            obj.put("type", "live");
                                                            obj.put("id", gid);
                                                            JSONObject score = scrbrd_u.get(i);
                                                            JSONObject scr = new JSONObject();
                                                            scr.put("state", "set"+(i+1));
                                                            scr.put("score1", score.get("team1"));
                                                            scr.put("score2", score.get("team2"));
                                                            scrbrd_c.add(scr);
                                                        }
                                                        obj.put("param", "scoreboard");
                                                        obj.put("value",scrbrd_c);
                                                        update(obj);
                                                    }
                                                    else if (scrbrd_u.size() == scrbrd.size()){
                                                        JSONObject obj = new JSONObject();
                                                        List<JSONObject> scrbrd_c = new ArrayList<JSONObject>();
                                                        for (int i = 0; i<scrbrd.size(); i++){
                                                            obj.put("command", "update");
                                                            obj.put("what", "game");
                                                            obj.put("type", "live");
                                                            obj.put("id", gid);
                                                            JSONObject score = scrbrd.get(i);
                                                            JSONObject score_u = scrbrd_u.get(i);
                                                            if (!score.get("team1").equals(score_u.get("team1")) || !score.get("team2").equals(score_u.get("team2"))){
                                                                JSONObject scr = new JSONObject();
                                                                scr.put("state", "set"+(i+1));
                                                                scr.put("score1", score_u.get("team1"));
                                                                scr.put("score2", score_u.get("team2"));
                                                                scrbrd_c.add(scr);
                                                            }
                                                        }
                                                        if(scrbrd_c.size()>0){
                                                            obj.put("param", "scoreboard");
                                                            obj.put("value",scrbrd_c);
                                                            update(obj);
                                                        }

                                                    }
                                                }
                                            }
                                            Map<String, JSONObject> mkts_u = (Map<String, JSONObject>) gms_u.get(gid).get("markets");
                                            Map<String, JSONObject> mkts = (Map<String, JSONObject>) gms.get(gid).get("markets");
                                            //del_data("market",mkts_u,mkts);
                                            keys1 = new HashSet<String>(mkts.keySet());
                                            keys2 = new HashSet<String>(mkts_u.keySet());
                                            keys1.removeAll(keys2);
                                            if (keys1.size()>0)
                                            {
                                                JSONObject obj = new JSONObject();
                                                obj.put("command", "delete");
                                                obj.put("what", "market");
                                                obj.put("ids", keys1);
                                                obj.put("type", "live");
                                                update(obj);
                                            }
                                            for (String mid : mkts_u.keySet())
                                            {
                                                if (mkts.containsKey(mid))
                                                {
                                                    check_changes("market",mkts_u.get(mid),mkts.get(mid));
                                                    Map<String, JSONObject> evnts_u = (Map<String, JSONObject>) mkts_u.get(mid).get("events");
                                                    Map<String, JSONObject> evnts = (Map<String, JSONObject>) mkts.get(mid).get("events");
                                                    //del_data("event",evnts_u,evnts);
                                                    keys1 = new HashSet<String>(evnts.keySet());
                                                    keys2 = new HashSet<String>(evnts_u.keySet());
                                                    keys1.removeAll(keys2);
                                                    if (keys1.size()>0)
                                                    {
                                                        JSONObject obj = new JSONObject();
                                                        obj.put("command", "delete");
                                                        obj.put("what", "event");
                                                        obj.put("ids", keys1);
                                                        obj.put("type", "live");
                                                        update(obj);
                                                    }
                                                    for (String eid : evnts_u.keySet())
                                                    {
                                                        if (evnts.containsKey(eid))
                                                        {
                                                            //check_changes("event",evnts_u.get(eid),evnts.get(eid));
                                                            if (!evnts_u.get(eid).get("price").equals(evnts.get(eid).get("price")))
                                                            {
                                                                JSONObject up = new JSONObject();
                                                                if (mkts_u.get(mid).containsKey("type")) up.put("market_type", mkts_u.get(mid).get("type"));
                                                                up.put("command","update");
                                                                up.put("what", "event");
                                                                up.put("id", evnts_u.get(eid).get("_id"));
                                                                up.put("gid", gid);
                                                                up.put("sid", sid);
                                                                up.put("type", "live");
                                                                up.put("param", "price");
                                                                up.put("value", evnts_u.get(eid).get("price"));
                                                                updateME(up);
                                                            }
                                                        }
                                                        else
                                                        {
                                                            JSONObject eve = (JSONObject)evnts_u.get(eid);
                                                            JSONObject ne = new JSONObject();
                                                            ne.put("command", "new");
                                                            ne.put("what", "event");
                                                            ne.put("type", "live");
                                                            ne.put("data", eve);
                                                            if (mkts_u.get(mid).containsKey("type")) ne.put("market_type", mkts_u.get(mid).get("type"));
                                                            ne.put("gid", gid);
                                                            ne.put("sid", sid);
                                                            updateME(ne);
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    JSONObject market = (JSONObject)mkts_u.get(mid);
                                                    JSONObject nm = new JSONObject();
                                                    nm.put("command", "new");
                                                    nm.put("what", "market");
                                                    nm.put("type", "live");
                                                    JSONObject m = new JSONObject();
                                                    JSONArray eids = new JSONArray();
                                                    m.put("id", mid);
                                                    m.put("order", market.get("order").toString());
                                                    m.put("html", market.get("html"));
                                                    m.put("eIds", ((Map<String, JSONObject>)market.get("events")).keySet());
                                                    nm.put("data_vars", m);
                                                    nm.put("data", market);
                                                    if (market.containsKey("type")) nm.put("market_type", market.get("type"));
                                                    nm.put("gid", gid);
                                                    nm.put("sid", sid);
                                                    updateME(nm);
                                                    Map<String, JSONObject> evnts_u = (Map<String, JSONObject>) mkts_u.get(mid).get("events");
                                                    for (String eid : evnts_u.keySet())
                                                    {
                                                        JSONObject eve = (JSONObject)evnts_u.get(eid);
                                                        JSONObject ne = new JSONObject();
                                                        ne.put("command", "new");
                                                        ne.put("what", "event");
                                                        ne.put("type", "live");
                                                        ne.put("data", eve);
                                                        if (mkts_u.get(mid).containsKey("type")) ne.put("market_type", mkts_u.get(mid).get("type"));
                                                        ne.put("gid", gid);
                                                        ne.put("sid", sid);
                                                        updateME(ne);
                                                    }
                                                }
                                            }
                                        }
                                        else
                                        {
                                            List<JSONObject> gr1 = new ArrayList<JSONObject>();
                                            List<JSONObject> gr2 = new ArrayList<JSONObject>();
                                            List<JSONObject> gr3 = new ArrayList<JSONObject>();
                                            List<JSONObject> gr4 = new ArrayList<JSONObject>();
                                            List<JSONObject> total = new ArrayList<JSONObject>();
                                            List<JSONObject> total2 = new ArrayList<JSONObject>();
                                            Map<String, JSONObject> mkts_u = (Map<String, JSONObject>) gms_u.get(gid).get("markets");
                                            for (String mid : mkts_u.keySet())
                                            {
                                                JSONObject market = (JSONObject)mkts_u.get(mid);
                                                JSONObject nm = new JSONObject();
                                                nm.put("command", "new");
                                                nm.put("what", "market");
                                                nm.put("type", "live");
                                                JSONObject m = new JSONObject();
                                                JSONArray eids = new JSONArray();
                                                m.put("id", mid);
                                                m.put("order", market.get("order").toString());
                                                m.put("html", market.get("html"));
                                                m.put("eIds", ((Map<String, JSONObject>)market.get("events")).keySet());
                                                nm.put("data_vars", m);
                                                nm.put("data", market);
                                                if (market.containsKey("type")) nm.put("market_type", market.get("type"));
                                                nm.put("gid", gid);
                                                nm.put("sid", sid);
                                                updateME(nm);
                                                Map<String, JSONObject> evnts_u = (Map<String, JSONObject>) mkts_u.get(mid).get("events");
                                                for (String eid : evnts_u.keySet())
                                                {
                                                    JSONObject eve = (JSONObject)evnts_u.get(eid);
                                                    JSONObject ne = new JSONObject();
                                                    ne.put("command", "new");
                                                    ne.put("what", "event");
                                                    ne.put("type", "live");
                                                    ne.put("data", eve);
                                                    if (mkts_u.get(mid).containsKey("type")) ne.put("market_type", mkts_u.get(mid).get("type"));
                                                    ne.put("gid", gid);
                                                    ne.put("sid", sid);
                                                    updateME(ne);
                                                }
                                                if (market.containsKey("type"))
                                                {
                                                    if (((String)market.get("type")).equals("P1XP2") || ((String)market.get("type")).equals("P1P2")) gr1.add(market);
                                                    if (((String)market.get("type")).equals("1X12X2") && sid.equals("844")) gr2.add(market);
                                                    else if (sportPartsT.containsKey(sid)){
                                                        if (((String)market.get("type")).toLowerCase().contains(sportPartsT.get(sid).toLowerCase()+"p1p2") ||
                                                                ((String)market.get("type")).toLowerCase().contains(sportPartsT.get(sid).toLowerCase()+"p1xp2")){

                                                            gr2.add(market);
                                                        }
                                                    }
                                                    if (((String)market.get("type")).equals("NextGoal")) gr3.add(market);
                                                    if (sid == "844")
                                                    {
                                                        if (((String)market.get("type")).equals("1HalfP1XP2")) gr1.add(market);
                                                        if (((String)market.get("type")).equals("1Half1X12X2")) gr2.add(market);
                                                        if (((String)market.get("type")).equals("1HalfNextGoal")) gr3.add(market);
                                                        if (((String)market.get("type")).equals("FirstHalfTotal")) total2.add(market);
                                                    }
                                                    if(((String)market.get("type")).equals("Total")
                                                            //|| ((String)market.get("type")).equals("SetTotal")
                                                            //|| ((String)market.get("type")).equals("PeriodTotal")
                                                            || ((String)market.get("type")).equals("Gametotalpoints")
                                                            ) total.add(market);
                                                }
                                            }
                                            Collections.sort(total, new Comparator<JSONObject>() {
                                                @Override
                                                public int compare(JSONObject o1, JSONObject o2) {
                                                    return new Double(String.valueOf(o1.get("base"))).compareTo(new Double(String.valueOf(o2.get("base"))));
                                                }
                                            });
                                            Collections.sort(total2, new Comparator<JSONObject>() {
                                                @Override
                                                public int compare(JSONObject o1, JSONObject o2) {
                                                    return new Double(String.valueOf(o1.get("base"))).compareTo(new Double(String.valueOf(o2.get("base"))));
                                                }
                                            });
                                            if (sid.equals("844")){
                                                if (total.size()>0){
                                                    JSONObject tf1 = new JSONObject();
                                                    for (JSONObject tot1 : total){
                                                        if (Double.parseDouble(tot1.get("base").toString()) == 2.5){
                                                            tf1 = tot1; break;
                                                        }
                                                    }
                                                    if (!tf1.isEmpty()) gr4.add(tf1); else gr4.add(total.get(0));
                                                }
                                                if (total2.size()>0){
                                                    JSONObject tf2 = new JSONObject();
                                                    for (JSONObject tot2 : total2){
                                                        if (Double.parseDouble(tot2.get("base").toString()) == 2.5){
                                                            tf2 = tot2; break;
                                                        }
                                                    }
                                                    if (!tf2.isEmpty()) gr4.add(tf2); else gr4.add(total2.get(0));
                                                }
                                            }
                                            else {
                                                if (total.size()>0)
                                                {
                                                    gr4.add(total.get(0));
                                                }
                                                if (total2.size()>0)
                                                {
                                                    gr4.add(total2.get(0));
                                                }
                                            }
                                            JSONObject sport = udata.get(sid);
                                            JSONObject game = gms_u.get(gid);
                                            JSONObject comp = comps_u.get(cid);
                                            VelocityContext game_row = new VelocityContext();
                                            StringWriter row = new StringWriter();
                                            Date gs = new Date();
                                            gs.setTime(Long.parseLong(game.get("start").toString())*1000);
                                            SimpleDateFormat dt = new SimpleDateFormat("dd.MM hh:mm");
                                            game_row.put("game", game);
                                            game_row.put("talias", "1");
                                            game_row.put("start", dt.format(gs).toString());
                                            JSONObject sp_row = new JSONObject();
                                            sp_row.put("id",sid);
                                            sp_row.put("name", sport.get("name"));
                                            sp_row.put("alias",sport.get("alias"));
                                            game_row.put("sport", sp_row);
                                            JSONObject rcid = new JSONObject();
                                            rcid.put("rid", rid);
                                            rcid.put("cid",cid);
                                            game_row.put("rc", rcid);
                                            game_row.put("comp_name", (String)comp.get("name"));
                                            game_row.put("region_alias", rgns_u.get(rid).get("alias"));
                                            game_row.put("gr1", gr1);
                                            game_row.put("gr2", gr2);
                                            game_row.put("gr3", gr3);
                                            game_row.put("gr4", gr4);
                                            if (!sid.equals("844") && sportPartsT.containsKey(sid)) game_row.put("part", sportPartsT.get(sid).toLowerCase());
                                            Velocity.mergeTemplate("game_row.vm", StandardCharsets.UTF_8.name(), game_row, row);
                                            game.put("view", row.toString());
                                            JSONObject ng = new JSONObject();
                                            ng.put("command","new");
                                            ng.put("what", "game");
                                            ng.put("type", "live");
                                            ng.put("comp_name", comps_u.get(cid).get("name"));
                                            ng.put("sport_name", udata.get(sid).get("name"));
                                            ng.put("sport_alias", udata.get(sid).get("alias"));
                                            ng.put("data", game);
                                            update(ng);
                                        }
                                    }
                                }
                                else
                                {
                                    JSONObject nc = new JSONObject();
                                    nc.put("command","new");
                                    nc.put("what", "comp");
                                    nc.put("type", "live");
                                    nc.put("data", comps_u.get(cid));
                                    update(nc);
                                    Map<String,JSONObject> gms = (Map<String,JSONObject>)comps_u.get(cid).get("games");
                                    for(String gid : gms.keySet())
                                    {
                                        List<JSONObject> gr1 = new ArrayList<JSONObject>();
                                        List<JSONObject> gr2 = new ArrayList<JSONObject>();
                                        List<JSONObject> gr3 = new ArrayList<JSONObject>();
                                        List<JSONObject> gr4 = new ArrayList<JSONObject>();
                                        List<JSONObject> total = new ArrayList<JSONObject>();
                                        List<JSONObject> total2 = new ArrayList<JSONObject>();
                                        Map<String, JSONObject> mkts_u = (Map<String, JSONObject>) gms.get(gid).get("markets");
                                        for (String mid : mkts_u.keySet())
                                        {
                                            JSONObject market = (JSONObject)mkts_u.get(mid);
                                            JSONObject nm = new JSONObject();
                                            nm.put("command", "new");
                                            nm.put("what", "market");
                                            nm.put("type", "live");
                                            JSONObject m = new JSONObject();
                                            JSONArray eids = new JSONArray();
                                            m.put("id", mid);
                                            m.put("order", market.get("order").toString());
                                            m.put("html", market.get("html"));
                                            m.put("eIds", ((Map<String, JSONObject>)market.get("events")).keySet());
                                            nm.put("data_vars", m);
                                            nm.put("data", market);
                                            if (market.containsKey("type")) nm.put("market_type", market.get("type"));
                                            nm.put("gid", gid);
                                            nm.put("sid", sid);
                                            updateME(nm);
                                            Map<String, JSONObject> evnts_u = (Map<String, JSONObject>) mkts_u.get(mid).get("events");
                                            for (String eid : evnts_u.keySet())
                                            {
                                                JSONObject eve = (JSONObject)evnts_u.get(eid);
                                                JSONObject ne = new JSONObject();
                                                ne.put("command", "new");
                                                ne.put("what", "event");
                                                ne.put("type", "live");
                                                ne.put("data", eve);
                                                if (mkts_u.get(mid).containsKey("type")) ne.put("market_type", mkts_u.get(mid).get("type"));
                                                ne.put("gid", gid);
                                                ne.put("sid", sid);
                                                updateME(ne);
                                            }
                                            if (market.containsKey("type"))
                                            {
                                                if (((String)market.get("type")).equals("P1XP2") || ((String)market.get("type")).equals("P1P2")) gr1.add(market);
                                                if (((String)market.get("type")).equals("1X12X2") && sid.equals("844")) gr2.add(market);
                                                else if (sportPartsT.containsKey(sid)){
                                                    if (((String)market.get("type")).toLowerCase().contains(sportPartsT.get(sid).toLowerCase()+"p1p2") ||
                                                            ((String)market.get("type")).toLowerCase().contains(sportPartsT.get(sid).toLowerCase()+"p1xp2")){

                                                        gr2.add(market);
                                                    }
                                                }
                                                if (((String)market.get("type")).equals("NextGoal")) gr3.add(market);
                                                if (sid == "844")
                                                {
                                                    if (((String)market.get("type")).equals("1HalfP1XP2")) gr1.add(market);
                                                    if (((String)market.get("type")).equals("1Half1X12X2")) gr2.add(market);
                                                    if (((String)market.get("type")).equals("1HalfNextGoal")) gr3.add(market);
                                                    if (((String)market.get("type")).equals("FirstHalfTotal")) total2.add(market);
                                                }
                                                if(((String)market.get("type")).equals("Total")
                                                        //|| ((String)market.get("type")).equals("SetTotal")
                                                        //|| ((String)market.get("type")).equals("PeriodTotal")
                                                        || ((String)market.get("type")).equals("Gametotalpoints")
                                                        ) total.add(market);
                                            }
                                        }
                                        Collections.sort(total, new Comparator<JSONObject>() {
                                            @Override
                                            public int compare(JSONObject o1, JSONObject o2) {
                                                return new Double(String.valueOf(o1.get("base"))).compareTo(new Double(String.valueOf(o2.get("base"))));
                                            }
                                        });
                                        Collections.sort(total2, new Comparator<JSONObject>() {
                                            @Override
                                            public int compare(JSONObject o1, JSONObject o2) {
                                                return new Double(String.valueOf(o1.get("base"))).compareTo(new Double(String.valueOf(o2.get("base"))));
                                            }
                                        });
                                        if (sid.equals("844")){
                                            if (total.size()>0){
                                                JSONObject tf1 = new JSONObject();
                                                for (JSONObject tot1 : total){
                                                    if (Double.parseDouble(tot1.get("base").toString()) == 2.5){
                                                        tf1 = tot1; break;
                                                    }
                                                }
                                                if (!tf1.isEmpty()) gr4.add(tf1); else gr4.add(total.get(0));
                                            }
                                            if (total2.size()>0){
                                                JSONObject tf2 = new JSONObject();
                                                for (JSONObject tot2 : total2){
                                                    if (Double.parseDouble(tot2.get("base").toString()) == 2.5){
                                                        tf2 = tot2; break;
                                                    }
                                                }
                                                if (!tf2.isEmpty()) gr4.add(tf2); else gr4.add(total2.get(0));
                                            }
                                        }
                                        else {
                                            if (total.size()>0)
                                            {
                                                gr4.add(total.get(0));
                                            }
                                            if (total2.size()>0)
                                            {
                                                gr4.add(total2.get(0));
                                            }
                                        }
                                        JSONObject sport = udata.get(sid);
                                        JSONObject game = gms.get(gid);
                                        JSONObject comp = comps_u.get(cid);
                                        VelocityContext game_row = new VelocityContext();
                                        StringWriter row = new StringWriter();
                                        Date gs = new Date();
                                        gs.setTime(Long.parseLong(game.get("start").toString())*1000);
                                        SimpleDateFormat dt = new SimpleDateFormat("dd.MM hh:mm");
                                        game_row.put("game", game);
                                        game_row.put("talias", "1");
                                        game_row.put("start", dt.format(gs).toString());
                                        JSONObject sp_row = new JSONObject();
                                        sp_row.put("id",sid);
                                        sp_row.put("name", sport.get("name"));
                                        sp_row.put("alias",sport.get("alias"));
                                        game_row.put("sport", sp_row);
                                        JSONObject rcid = new JSONObject();
                                        rcid.put("rid", rid);
                                        rcid.put("cid",cid);
                                        game_row.put("rc", rcid);
                                        game_row.put("comp_name", (String)comp.get("name"));
                                        game_row.put("region_alias", rgns_u.get(rid).get("alias"));
                                        game_row.put("gr1", gr1);
                                        game_row.put("gr2", gr2);
                                        game_row.put("gr3", gr3);
                                        game_row.put("gr4", gr4);
                                        if (!sid.equals("844") && sportPartsT.containsKey(sid)) game_row.put("part", sportPartsT.get(sid).toLowerCase());
                                        Velocity.mergeTemplate("game_row.vm", StandardCharsets.UTF_8.name(), game_row, row);
                                        game.put("view", row.toString());
                                        JSONObject ng = new JSONObject();
                                        ng.put("command","new");
                                        ng.put("what", "game");
                                        ng.put("type", "live");
                                        ng.put("comp_name", comps_u.get(cid).get("name"));
                                        ng.put("sport_name", udata.get(sid).get("name"));
                                        ng.put("sport_alias", udata.get(sid).get("alias"));
                                        ng.put("data", game);
                                        update(ng);
                                    }
                                }
                            }
                        }
                        else
                        {
                            Map<String,JSONObject> comps = (Map<String,JSONObject>)rgns_u.get(rid).get("comps");
                            for(String cid : comps.keySet())
                            {
                                JSONObject nc = new JSONObject();
                                nc.put("command","new");
                                nc.put("what", "comp");
                                nc.put("type", "live");
                                nc.put("data", comps.get(cid));
                                update(nc);
                                Map<String,JSONObject> gms = (Map<String,JSONObject>)comps.get(cid).get("games");
                                for(String gid : gms.keySet())
                                {
                                    List<JSONObject> gr1 = new ArrayList<JSONObject>();
                                    List<JSONObject> gr2 = new ArrayList<JSONObject>();
                                    List<JSONObject> gr3 = new ArrayList<JSONObject>();
                                    List<JSONObject> gr4 = new ArrayList<JSONObject>();
                                    List<JSONObject> total = new ArrayList<JSONObject>();
                                    List<JSONObject> total2 = new ArrayList<JSONObject>();
                                    Map<String, JSONObject> mkts_u = (Map<String, JSONObject>) gms.get(gid).get("markets");
                                    for (String mid : mkts_u.keySet())
                                    {
                                        JSONObject market = (JSONObject)mkts_u.get(mid);
                                        JSONObject nm = new JSONObject();
                                        nm.put("command", "new");
                                        nm.put("what", "market");
                                        nm.put("type", "live");
                                        JSONObject m = new JSONObject();
                                        JSONArray eids = new JSONArray();
                                        m.put("id", mid);
                                        m.put("order", market.get("order").toString());
                                        m.put("html", market.get("html"));
                                        m.put("eIds", ((Map<String, JSONObject>)market.get("events")).keySet());
                                        nm.put("data_vars", m);
                                        nm.put("data", market);
                                        if (market.containsKey("type")) nm.put("market_type", market.get("type"));
                                        nm.put("gid", gid);
                                        nm.put("sid", sid);
                                        updateME(nm);
                                        Map<String, JSONObject> evnts_u = (Map<String, JSONObject>) mkts_u.get(mid).get("events");
                                        for (String eid : evnts_u.keySet())
                                        {
                                            JSONObject eve = (JSONObject)evnts_u.get(eid);
                                            JSONObject ne = new JSONObject();
                                            ne.put("command", "new");
                                            ne.put("what", "event");
                                            ne.put("type", "live");
                                            ne.put("data", eve);
                                            if (mkts_u.get(mid).containsKey("type")) ne.put("market_type", mkts_u.get(mid).get("type"));
                                            ne.put("gid", gid);
                                            ne.put("sid", sid);
                                            updateME(ne);
                                        }
                                        if (market.containsKey("type"))
                                        {
                                            if (((String)market.get("type")).equals("P1XP2") || ((String)market.get("type")).equals("P1P2")) gr1.add(market);
                                            if (((String)market.get("type")).equals("1X12X2") && sid.equals("844")) gr2.add(market);
                                            else if (sportPartsT.containsKey(sid)){
                                                if (((String)market.get("type")).toLowerCase().contains(sportPartsT.get(sid).toLowerCase()+"p1p2") ||
                                                        ((String)market.get("type")).toLowerCase().contains(sportPartsT.get(sid).toLowerCase()+"p1xp2")){

                                                    gr2.add(market);
                                                }
                                            }
                                            if (((String)market.get("type")).equals("NextGoal")) gr3.add(market);
                                            if (sid == "844")
                                            {
                                                if (((String)market.get("type")).equals("1HalfP1XP2")) gr1.add(market);
                                                if (((String)market.get("type")).equals("1Half1X12X2")) gr2.add(market);
                                                if (((String)market.get("type")).equals("1HalfNextGoal")) gr3.add(market);
                                                if (((String)market.get("type")).equals("FirstHalfTotal")) total2.add(market);
                                            }
                                            if(((String)market.get("type")).equals("Total")
                                                    //|| ((String)market.get("type")).equals("SetTotal")
                                                    //|| ((String)market.get("type")).equals("PeriodTotal")
                                                    || ((String)market.get("type")).equals("Gametotalpoints")
                                                    ) total.add(market);
                                        }
                                    }
                                    Collections.sort(total, new Comparator<JSONObject>() {
                                        @Override
                                        public int compare(JSONObject o1, JSONObject o2) {
                                            return new Double(String.valueOf(o1.get("base"))).compareTo(new Double(String.valueOf(o2.get("base"))));
                                        }
                                    });
                                    Collections.sort(total2, new Comparator<JSONObject>() {
                                        @Override
                                        public int compare(JSONObject o1, JSONObject o2) {
                                            return new Double(String.valueOf(o1.get("base"))).compareTo(new Double(String.valueOf(o2.get("base"))));
                                        }
                                    });
                                    if (sid.equals("844")){
                                        if (total.size()>0){
                                            JSONObject tf1 = new JSONObject();
                                            for (JSONObject tot1 : total){
                                                if (Double.parseDouble(tot1.get("base").toString()) == 2.5){
                                                    tf1 = tot1; break;
                                                }
                                            }
                                            if (!tf1.isEmpty()) gr4.add(tf1); else gr4.add(total.get(0));
                                        }
                                        if (total2.size()>0){
                                            JSONObject tf2 = new JSONObject();
                                            for (JSONObject tot2 : total2){
                                                if (Double.parseDouble(tot2.get("base").toString()) == 2.5){
                                                    tf2 = tot2; break;
                                                }
                                            }
                                            if (!tf2.isEmpty()) gr4.add(tf2); else gr4.add(total2.get(0));
                                        }
                                    }
                                    else {
                                        if (total.size()>0)
                                        {
                                            gr4.add(total.get(0));
                                        }
                                        if (total2.size()>0)
                                        {
                                            gr4.add(total2.get(0));
                                        }
                                    }
                                    JSONObject sport = udata.get(sid);
                                    JSONObject game = gms.get(gid);
                                    JSONObject comp = comps.get(cid);
                                    VelocityContext game_row = new VelocityContext();
                                    StringWriter row = new StringWriter();
                                    Date gs = new Date();
                                    gs.setTime(Long.parseLong(game.get("start").toString())*1000);
                                    SimpleDateFormat dt = new SimpleDateFormat("dd.MM hh:mm");
                                    game_row.put("game", game);
                                    game_row.put("talias", "1");
                                    game_row.put("start", dt.format(gs).toString());
                                    JSONObject sp_row = new JSONObject();
                                    sp_row.put("id",sid);
                                    sp_row.put("name", sport.get("name"));
                                    sp_row.put("alias",sport.get("alias"));
                                    game_row.put("sport", sp_row);
                                    JSONObject rcid = new JSONObject();
                                    rcid.put("rid", rid);
                                    rcid.put("cid",cid);
                                    game_row.put("rc", rcid);
                                    game_row.put("comp_name", (String)comp.get("name"));
                                    game_row.put("region_alias", rgns_u.get(rid).get("alias"));
                                    game_row.put("gr1", gr1);
                                    game_row.put("gr2", gr2);
                                    game_row.put("gr3", gr3);
                                    game_row.put("gr4", gr4);
                                    if (!sid.equals("844") && sportPartsT.containsKey(sid)) game_row.put("part", sportPartsT.get(sid).toLowerCase());
                                    Velocity.mergeTemplate("game_row.vm", StandardCharsets.UTF_8.name(), game_row, row);
                                    game.put("view", row.toString());
                                    JSONObject ng = new JSONObject();
                                    ng.put("command","new");
                                    ng.put("what", "game");
                                    ng.put("type", "live");
                                    ng.put("comp_name", comps.get(cid).get("name"));
                                    ng.put("sport_name", udata.get(sid).get("name"));
                                    ng.put("sport_alias", udata.get(sid).get("alias"));
                                    ng.put("data", game);
                                    update(ng);
                                }
                            }
                        }
                    }
                }
                else
                {
                    JSONObject ns = new JSONObject();
                    main.infoLogger.info("Added sport(LIVE): " + sid);
                    ns.put("command","new");
                    ns.put("what", "sport");
                    ns.put("type", "live");
                    ns.put("data", udata.get(sid));
                    update(ns);
                    Map<String, JSONObject> rgns_u = (Map<String, JSONObject>) udata.get(sid).get("regions");
                    for (String rid : rgns_u.keySet())
                    {
                        Map<String,JSONObject> comps = (Map<String,JSONObject>)rgns_u.get(rid).get("comps");
                        for(String cid : comps.keySet())
                        {
                            JSONObject nc = new JSONObject();
                            nc.put("command","new");
                            nc.put("what", "comp");
                            nc.put("type", "live");
                            nc.put("data", comps.get(cid));
                            update(nc);
                            Map<String,JSONObject> gms = (Map<String,JSONObject>)comps.get(cid).get("games");
                            for(String gid : gms.keySet())
                            {
                                List<JSONObject> gr1 = new ArrayList<JSONObject>();
                                List<JSONObject> gr2 = new ArrayList<JSONObject>();
                                List<JSONObject> gr3 = new ArrayList<JSONObject>();
                                List<JSONObject> gr4 = new ArrayList<JSONObject>();
                                List<JSONObject> total = new ArrayList<JSONObject>();
                                List<JSONObject> total2 = new ArrayList<JSONObject>();
                                Map<String, JSONObject> mkts_u = (Map<String, JSONObject>) gms.get(gid).get("markets");
                                for (String mid : mkts_u.keySet())
                                {
                                    JSONObject market = (JSONObject)mkts_u.get(mid);
                                    JSONObject nm = new JSONObject();
                                    nm.put("command", "new");
                                    nm.put("what", "market");
                                    nm.put("type", "live");
                                    JSONObject m = new JSONObject();
                                    JSONArray eids = new JSONArray();
                                    m.put("id", mid);
                                    m.put("order", market.get("order").toString());
                                    m.put("html", market.get("html"));
                                    m.put("eIds", ((Map<String, JSONObject>)market.get("events")).keySet());
                                    nm.put("data_vars", m);
                                    nm.put("data", market);
                                    if (market.containsKey("type")) nm.put("market_type", market.get("type"));
                                    nm.put("gid", gid);
                                    nm.put("sid", sid);
                                    updateME(nm);
                                    Map<String, JSONObject> evnts_u = (Map<String, JSONObject>) mkts_u.get(mid).get("events");
                                    for (String eid : evnts_u.keySet())
                                    {
                                        JSONObject eve = (JSONObject)evnts_u.get(eid);
                                        JSONObject ne = new JSONObject();
                                        ne.put("command", "new");
                                        ne.put("what", "event");
                                        ne.put("type", "live");
                                        ne.put("data", eve);
                                        if (mkts_u.get(mid).containsKey("type")) ne.put("market_type", mkts_u.get(mid).get("type"));
                                        ne.put("gid", gid);
                                        ne.put("sid", sid);
                                        updateME(ne);
                                    }
                                    if (market.containsKey("type"))
                                    {
                                        if (((String)market.get("type")).equals("P1XP2") || ((String)market.get("type")).equals("P1P2")) gr1.add(market);
                                        if (((String)market.get("type")).equals("1X12X2") && sid.equals("844")) gr2.add(market);
                                        else if (sportPartsT.containsKey(sid)){
                                            if (((String)market.get("type")).toLowerCase().contains(sportPartsT.get(sid).toLowerCase()+"p1p2") ||
                                                    ((String)market.get("type")).toLowerCase().contains(sportPartsT.get(sid).toLowerCase()+"p1xp2")){

                                                gr2.add(market);
                                            }
                                        }
                                        if (((String)market.get("type")).equals("NextGoal")) gr3.add(market);
                                        if (sid == "844")
                                        {
                                            if (((String)market.get("type")).equals("1HalfP1XP2")) gr1.add(market);
                                            if (((String)market.get("type")).equals("1Half1X12X2")) gr2.add(market);
                                            if (((String)market.get("type")).equals("1HalfNextGoal")) gr3.add(market);
                                            if (((String)market.get("type")).equals("FirstHalfTotal")) total2.add(market);
                                        }
                                        if(((String)market.get("type")).equals("Total")
                                                //|| ((String)market.get("type")).equals("SetTotal")
                                                //|| ((String)market.get("type")).equals("PeriodTotal")
                                                || ((String)market.get("type")).equals("Gametotalpoints")
                                                ) total.add(market);
                                    }
                                }
                                Collections.sort(total, new Comparator<JSONObject>() {
                                    @Override
                                    public int compare(JSONObject o1, JSONObject o2) {
                                        return new Double(String.valueOf(o1.get("base"))).compareTo(new Double(String.valueOf(o2.get("base"))));
                                    }
                                });
                                Collections.sort(total2, new Comparator<JSONObject>() {
                                    @Override
                                    public int compare(JSONObject o1, JSONObject o2) {
                                        return new Double(String.valueOf(o1.get("base"))).compareTo(new Double(String.valueOf(o2.get("base"))));
                                    }
                                });
                                if (sid.equals("844")){
                                    if (total.size()>0){
                                        JSONObject tf1 = new JSONObject();
                                        for (JSONObject tot1 : total){
                                            if (Double.parseDouble(tot1.get("base").toString()) == 2.5){
                                                tf1 = tot1; break;
                                            }
                                        }
                                        if (!tf1.isEmpty()) gr4.add(tf1); else gr4.add(total.get(0));
                                    }
                                    if (total2.size()>0){
                                        JSONObject tf2 = new JSONObject();
                                        for (JSONObject tot2 : total2){
                                            if (Double.parseDouble(tot2.get("base").toString()) == 2.5){
                                                tf2 = tot2; break;
                                            }
                                        }
                                        if (!tf2.isEmpty()) gr4.add(tf2); else gr4.add(total2.get(0));
                                    }
                                }
                                else {
                                    if (total.size()>0)
                                    {
                                        gr4.add(total.get(0));
                                    }
                                    if (total2.size()>0)
                                    {
                                        gr4.add(total2.get(0));
                                    }
                                }
                                JSONObject sport = udata.get(sid);
                                JSONObject game = gms.get(gid);
                                JSONObject comp = comps.get(cid);
                                VelocityContext game_row = new VelocityContext();
                                StringWriter row = new StringWriter();
                                Date gs = new Date();
                                gs.setTime(Long.parseLong(game.get("start").toString())*1000);
                                SimpleDateFormat dt = new SimpleDateFormat("dd.MM hh:mm");
                                game_row.put("game", game);
                                game_row.put("talias", "1");
                                game_row.put("start", dt.format(gs).toString());
                                JSONObject sp_row = new JSONObject();
                                sp_row.put("id",sid);
                                sp_row.put("name", sport.get("name"));
                                sp_row.put("alias",sport.get("alias"));
                                game_row.put("sport", sp_row);
                                JSONObject rcid = new JSONObject();
                                rcid.put("rid", rid);
                                rcid.put("cid",cid);
                                game_row.put("rc", rcid);
                                game_row.put("comp_name", (String)comp.get("name"));
                                game_row.put("region_alias", rgns_u.get(rid).get("alias"));
                                game_row.put("gr1", gr1);
                                game_row.put("gr2", gr2);
                                game_row.put("gr3", gr3);
                                game_row.put("gr4", gr4);
                                if (!sid.equals("844") && sportPartsT.containsKey(sid)) game_row.put("part", sportPartsT.get(sid).toLowerCase());
                                Velocity.mergeTemplate("game_row.vm", StandardCharsets.UTF_8.name(), game_row, row);
                                game.put("view", row.toString());
                                JSONObject ng = new JSONObject();
                                ng.put("command","new");
                                ng.put("what", "game");
                                ng.put("type", "live");
                                ng.put("comp_name", comps.get(cid).get("name"));
                                ng.put("sport_name", udata.get(sid).get("name"));
                                ng.put("sport_alias", udata.get(sid).get("alias"));
                                ng.put("data", game);
                                update(ng);
                            }
                        }
                    }
                }
            }
            data = new ConcurrentHashMap<String, JSONObject>(udata);
            if (update){
                JSONObject get_info = new JSONObject();
                get_info.put("command","get");
                get_info.put("rid", new Integer(1));
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
                game.put("type",new Integer(1));
                where.put("game", game);
                JSONObject sp_where = new JSONObject();
                JSONObject sids = new JSONObject();
                sids.put(opt, sport_ids);
                sp_where.put("id",sids);
                where.put("sport", sp_where);
                params.put("where",where);
                params.put("subscribe", false);
                get_info.put("params", params);
                client.sendMessage(get_info.toString());
            }
            //System.out.println(new Date() + "_" + "_sent update message_" + worker);
        }
        catch(Exception e)
        {
            main.errorLogger.error("Error happened", e);
            e.printStackTrace();
        }
    }

    void update(JSONObject update)
    {
        ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
        for(String tid : terminals.keySet())
        {
            if (terminals.get(tid) != null) {
                Session rcpt = (Session) terminals.get(tid).get("session");
                if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                    if (rcpt.isOpen()) main.sendIt(update, rcpt);
                }
                else {
                    ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                }
            }
        }
    }

    void updateME(JSONObject update) {
        if (update.containsKey("market_type")) {
            if ((update.get("market_type").equals("P1XP2") ||
                    update.get("market_type").equals("1HalfP1XP2") ||
                    update.get("market_type").equals("NextGoal") ||
                    update.get("market_type").equals("1HalfNextGoal") ||
                    update.get("market_type").equals("1X12X2")  ||
                    update.get("market_type").equals("1Half1X12X2"))
                    && update.get("sid").toString().equals("844"))
            {
                if (update.get("command").toString().equals("new") && update.get("what").toString().equals("market"))
                {
                    ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                    for(String tid : terminals.keySet())
                    {
                        HashMap<String,JSONObject> events = (HashMap<String,JSONObject>)((JSONObject)update.get("data")).get("events");
                        JSONObject sinfo = terminals.get(tid);
                        if (sinfo != null) {
                            for (String eid : events.keySet()) {
                                JSONObject event = events.get(eid);
                                String price = event.get("price").toString();
                                Double currCoef = Double.parseDouble(price);
                                String multi = sinfo.get("multiplier").toString();
                                Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
                                if (currCoef < 2 && currCoef > 1) {
                                    Double tmpCurrentKoeff = currCoef;
                                    Double koef = Double.parseDouble(multi) - 1.0;
                                    currCoef -= 1.0;
                                    currCoef *= koef;
                                    multiPrice = tmpCurrentKoeff + currCoef;
                                }
                                DecimalFormat df = new DecimalFormat("#.##");
                                df.setRoundingMode(RoundingMode.CEILING);
                                event.put("price", df.format(multiPrice).toString().replaceAll(",", "."));
                            }
                            ((JSONObject) update.get("data")).put("events", events);
                            Session rcpt = (Session) terminals.get(tid).get("session");
                            if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                if (rcpt.isOpen()) main.sendIt(update, rcpt);
                            }
                            else {
                                ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                            }
                        }
                    }
                }
                else if (update.get("what").toString().equals("event"))
                {
                    if (update.get("command").toString().equals("update"))
                    {
                        String price = update.get("value").toString();
                        Double currCoef = Double.parseDouble(price);
                        ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                        for(String tid : terminals.keySet())
                        {
                            JSONObject sinfo = terminals.get(tid);
                            if (sinfo != null) {
                                String multi = sinfo.get("multiplier").toString();
                                Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
                                if (currCoef < 2 && currCoef > 1) {
                                    Double tmpCurrentKoeff = currCoef;
                                    Double koef = Double.parseDouble(multi) - 1.0;
                                    currCoef -= 1.0;
                                    currCoef *= koef;
                                    multiPrice = tmpCurrentKoeff + currCoef;
                                }
                                DecimalFormat df = new DecimalFormat("#.##");
                                df.setRoundingMode(RoundingMode.CEILING);
                                update.put("value", df.format(multiPrice).toString().replaceAll(",", "."));
                                Session rcpt = (Session) terminals.get(tid).get("session");
                                if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                    if (rcpt.isOpen()) main.sendIt(update, rcpt);
                                }
                                else {
                                    ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                                }
                            }
                        }
                    }
                    else if (update.get("command").toString().equals("new"))
                    {
                        String price = ((JSONObject)update.get("data")).get("price").toString();
                        Double currCoef = Double.parseDouble(price);
                        ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                        for(String tid : terminals.keySet())
                        {
                            JSONObject sinfo = terminals.get(tid);
                            if (sinfo != null) {
                                String multi = sinfo.get("multiplier").toString();
                                Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
                                if (currCoef < 2 && currCoef > 1) {
                                    Double tmpCurrentKoeff = currCoef;
                                    Double koef = Double.parseDouble(multi) - 1.0;
                                    currCoef -= 1.0;
                                    currCoef *= koef;
                                    multiPrice = tmpCurrentKoeff + currCoef;
                                }
                                DecimalFormat df = new DecimalFormat("#.##");
                                df.setRoundingMode(RoundingMode.CEILING);
                                ((JSONObject) update.get("data")).put("price",
                                        df.format(multiPrice).toString().replaceAll(",", "."));
                                Session rcpt = (Session) terminals.get(tid).get("session");
                                if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                    if (rcpt.isOpen()) main.sendIt(update, rcpt);
                                }
                                else {
                                    ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                                }
                            }
                        }
                    }
                }
                else {
                    update(update);
                }

            }
            else if (update.get("market_type").equals("P1XP2") ||
                    update.get("market_type").equals("P1P2") ||
                    update.get("market_type").equals("Total") ||
                    //update.get("market_type").equals("SetTotal") ||
                    //update.get("market_type").equals("PeriodTotal") ||
                    update.get("market_type").equals("Gametotalpoints") ||
                    update.get("market_type").equals("FirstHalfTotal"))
            {
                if (update.get("command").toString().equals("new") && update.get("what").toString().equals("market"))
                {
                    ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                    for(String tid : terminals.keySet())
                    {
                        JSONObject modUp = update;
                        HashMap<String,JSONObject> events = (HashMap<String,JSONObject>)((JSONObject)modUp.get("data")).get("events");
                        JSONObject sinfo = terminals.get(tid);
                        if (sinfo != null) {
                            for (String eid : events.keySet()) {
                                JSONObject event = events.get(eid);
                                String price = event.get("price").toString();
                                Double currCoef = Double.parseDouble(price);
                                String multi = sinfo.get("multiplier").toString();
                                Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
                                if (currCoef < 2 && currCoef > 1) {
                                    Double tmpCurrentKoeff = currCoef;
                                    Double koef = Double.parseDouble(multi) - 1.0;
                                    currCoef -= 1.0;
                                    currCoef *= koef;
                                    multiPrice = tmpCurrentKoeff + currCoef;
                                }
                                DecimalFormat df = new DecimalFormat("#.##");
                                df.setRoundingMode(RoundingMode.CEILING);
                                event.put("price", df.format(multiPrice).toString().replaceAll(",", "."));
                            }
                            ((JSONObject) modUp.get("data")).put("events", events);
                            Session rcpt = (Session) terminals.get(tid).get("session");
                            if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                if (rcpt.isOpen()) main.sendIt(update, rcpt);
                            }
                            else {
                                ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                            }
                        }
                    }
                }
                else if (update.get("what").toString().equals("event"))
                {
                    if (update.get("command").toString().equals("update"))
                    {
                        String price = update.get("value").toString();

                        ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                        for(String tid : terminals.keySet())
                        {
                            JSONObject sinfo = terminals.get(tid);
                            if (sinfo != null)
                            {
                                String multi = sinfo.get("multiplier").toString();
                                Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
                                Double currCoef = Double.parseDouble(price);
                                if (currCoef < 2 && currCoef > 1)
                                {
                                    Double tmpCurrentKoeff = currCoef;
                                    Double koef = Double.parseDouble(multi) - 1.0;
                                    currCoef -= 1.0;
                                    currCoef *= koef;
                                    multiPrice = tmpCurrentKoeff + currCoef;
                                }
                                DecimalFormat df = new DecimalFormat("#.##");
                                df.setRoundingMode(RoundingMode.CEILING);
                                update.put("value", df.format(multiPrice).toString().replaceAll(",", "."));
                                Session rcpt = (Session) terminals.get(tid).get("session");
                                if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                    if (rcpt.isOpen()) main.sendIt(update, rcpt);
                                }
                                else {
                                    ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                                }
                            }
                        }
                    }
                    else if (update.get("command").toString().equals("new"))
                    {
                        String price = ((JSONObject)update.get("data")).get("price").toString();
                        ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                        for(String tid : terminals.keySet())
                        {
                            JSONObject sinfo = terminals.get(tid);
                            if (sinfo != null)
                            {
                                String multi = sinfo.get("multiplier").toString();
                                Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
                                Double currCoef = Double.parseDouble(price);
                                if (currCoef < 2 && currCoef > 1)
                                {
                                    Double tmpCurrentKoeff = currCoef;
                                    Double koef = Double.parseDouble(multi) - 1.0;
                                    currCoef -= 1.0;
                                    currCoef *= koef;
                                    multiPrice = tmpCurrentKoeff + currCoef;
                                }
                                DecimalFormat df = new DecimalFormat("#.##");
                                df.setRoundingMode(RoundingMode.CEILING);
                                ((JSONObject)update.get("data")).put("price", df.format(multiPrice).toString().replaceAll(",", "."));
                                Session rcpt = (Session) terminals.get(tid).get("session");
                                if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                    if (rcpt.isOpen()) main.sendIt(update, rcpt);
                                }
                                else {
                                    ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                                }
                            }
                        }
                    }
                }
                else {
                    update(update);
                }
            }

            else if (sportPartsT.containsKey(update.get("sid").toString()))
            {
                if (update.get("market_type").toString().toLowerCase().contains(sportPartsT.get(update.get("sid").toString()).toLowerCase()+"p1p2") ||
                        update.get("market_type").toString().toLowerCase().contains(sportPartsT.get(update.get("sid").toString()).toLowerCase()+"p1xp2")){
                    if (update.get("command").toString().equals("new") && update.get("what").toString().equals("market"))
                    {
                        ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                        for(String tid : terminals.keySet())
                        {
                            JSONObject modUp = update;
                            HashMap<String,JSONObject> events = (HashMap<String,JSONObject>)((JSONObject)modUp.get("data")).get("events");
                            JSONObject sinfo = terminals.get(tid);
                            if (sinfo != null) {
                                for (String eid : events.keySet()) {
                                    JSONObject event = events.get(eid);
                                    String price = event.get("price").toString();
                                    Double currCoef = Double.parseDouble(price);
                                    String multi = sinfo.get("multiplier").toString();
                                    Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
                                    if (currCoef < 2 && currCoef > 1) {
                                        Double tmpCurrentKoeff = currCoef;
                                        Double koef = Double.parseDouble(multi) - 1.0;
                                        currCoef -= 1.0;
                                        currCoef *= koef;
                                        multiPrice = tmpCurrentKoeff + currCoef;
                                    }
                                    DecimalFormat df = new DecimalFormat("#.##");
                                    df.setRoundingMode(RoundingMode.CEILING);
                                    event.put("price", df.format(multiPrice).toString().replaceAll(",", "."));
                                }
                                ((JSONObject) modUp.get("data")).put("events", events);
                                Session rcpt = (Session) terminals.get(tid).get("session");
                                if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                    if (rcpt.isOpen()) main.sendIt(update, rcpt);
                                }
                                else {
                                    ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                                }
                            }
                        }
                    }
                    else if (update.get("what").toString().equals("event"))
                    {
                        if (update.get("command").toString().equals("update"))
                        {
                            String price = update.get("value").toString();
                            ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                            for(String tid : terminals.keySet())
                            {
                                JSONObject sinfo = terminals.get(tid);
                                if (sinfo != null) {
                                    String multi = sinfo.get("multiplier").toString();
                                    Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
                                    Double currCoef = Double.parseDouble(price);
                                    if (currCoef < 2 && currCoef > 1) {
                                        Double tmpCurrentKoeff = currCoef;
                                        Double koef = Double.parseDouble(multi) - 1.0;
                                        currCoef -= 1.0;
                                        currCoef *= koef;
                                        multiPrice = tmpCurrentKoeff + currCoef;
                                    }
                                    DecimalFormat df = new DecimalFormat("#.##");
                                    df.setRoundingMode(RoundingMode.CEILING);
                                    update.put("value", df.format(multiPrice).toString().replaceAll(",", "."));
                                    Session rcpt = (Session) terminals.get(tid).get("session");
                                    if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                        if (rcpt.isOpen()) main.sendIt(update, rcpt);
                                    }
                                    else {
                                        ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                                    }
                                }
                            }
                        }
                        else if (update.get("command").toString().equals("new"))
                        {
                            String price = ((JSONObject)update.get("data")).get("price").toString();
                            ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                            for(String tid : terminals.keySet())
                            {
                                JSONObject sinfo = terminals.get(tid);
                                String multi = sinfo.get("multiplier").toString();
                                if (sinfo != null) {
                                    Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
                                    Double currCoef = Double.parseDouble(price);
                                    if (currCoef < 2 && currCoef > 1) {
                                        Double tmpCurrentKoeff = currCoef;
                                        Double koef = Double.parseDouble(multi) - 1.0;
                                        currCoef -= 1.0;
                                        currCoef *= koef;
                                        multiPrice = tmpCurrentKoeff + currCoef;
                                    }
                                    DecimalFormat df = new DecimalFormat("#.##");
                                    df.setRoundingMode(RoundingMode.CEILING);
                                    ((JSONObject) update.get("data")).put("price",
                                            df.format(multiPrice).toString().replaceAll(",", "."));
                                    Session rcpt = (Session) terminals.get(tid).get("session");
                                    if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                        if (rcpt.isOpen()) main.sendIt(update, rcpt);
                                    }
                                    else {
                                        ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                                    }
                                }
                            }
                        }
                    }
                    else {
                        update(update);
                    }
                }
                else
                {
                    if (update.get("command").toString().equals("statistic")) {
                        ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                        //System.out.println(update.get("id"));
                        for (String tid : terminals.keySet())
                        {
                            JSONObject sinfo = terminals.get(tid);
                            if (sinfo != null) {
                                if (sinfo.get("gid") != null) {
                                    if (sinfo.get("gid").equals(update.get("id"))) {
                                        Session rcpt = (Session) terminals.get(tid).get("session");
                                        if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                            if (rcpt.isOpen()) main.sendIt(update, rcpt);
                                        }
                                        else {
                                            ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else if (update.get("what").equals("event")) {
                        ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                        for (String tid : terminals.keySet())
                        {
                            JSONObject sinfo = terminals.get(tid);
                            List<String> eids = (ArrayList<String>) sinfo.get("betslip");
                            Session rcpt = (Session) sinfo.get("session");
                            if (eids.contains(update.get("id"))){
                                if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                    if (rcpt.isOpen()) main.sendIt(update, rcpt);
                                }
                                else {
                                    ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                for (String tid : terminals.keySet())
                {
                    JSONObject sinfo = terminals.get(tid);
                    if (sinfo != null) {
                        if (sinfo.get("gid") != null) {
                            if (sinfo.get("gid").equals(update.get("gid"))) {
                                Session rcpt = (Session) terminals.get(tid).get("session");
                                if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                    if (rcpt.isOpen()) main.sendIt(update, rcpt);
                                }
                                else {
                                    ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                                }
                            }
                        }
                        if (update.get("what").equals("event")) {
                            List<String> eids = (ArrayList<String>) sinfo.get("betslip");
                            Session rcpt = (Session) sinfo.get("session");
                            if (eids.contains(update.get("id"))){
                                if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                    if (rcpt.isOpen()) main.sendIt(update, rcpt);
                                }
                                else {
                                    ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                                }
                            }
                        }
                    }
                }
            }
        }
        else
        {
            ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
            for (String tid : terminals.keySet())
            {
                JSONObject sinfo = terminals.get(tid);
                if (sinfo != null) {
                    Session rcpt = (Session) sinfo.get("session");
                    if (sinfo.get("gid") != null) {
                        if (sinfo.get("gid").equals(update.get("gid"))) {
                            if (update.get("command").toString().equals("statistic")) {
                                update.put("id", update.get("gid"));
                                update.remove("gid");
                            }
                            if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                if (rcpt.isOpen()) main.sendIt(update, rcpt);
                            }
                            else {
                                ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                            }
                        }
                        //else { sessions.remove(s); session_info.remove(s); }
                    }
                    if (update.get("what").equals("event")) {
                        List<String> eids = (ArrayList<String>) sinfo.get("betslip");
                        if (eids.contains(update.get("id"))) {
                            if (((CountDownLatch)terminals.get(tid).get("latch")).getCount() == 0) {
                                if (rcpt.isOpen()) main.sendIt(update, rcpt);
                            }
                            else {
                                ((ArrayList<JSONObject>)terminals.get(tid).get("updates")).add(update);
                            }
                        }
                        //else { sessions.remove(s); session_info.remove(s); }
                    }
                }
            }
        }
    }

    public static Map<String, Object> JSONtoMap (JSONObject obj)
    {
        Map<String, Object> map = new HashMap<String, Object>();
        Set<String> keys = obj.keySet();
        for (String key : keys)
        {
            map.put(key, obj.get(key));
        }
        return map;
    }

    public void check_changes (String what, JSONObject obj1, JSONObject obj2)
    {
        try {
            JSONObject mobj1 = (JSONObject)obj1.clone();
            JSONObject mobj2 = (JSONObject)obj2.clone();
            switch (what)
            {
                case "sport":
                {
                    mobj1.remove("regions");
                    mobj2.remove("regions");
                }; break;
                case "comp":
                {
                    mobj1.remove("games");
                    mobj2.remove("games");
                }; break;
                case "game":
                {
                    mobj1.remove("markets");
                    mobj2.remove("markets");
                }; break;
                case "market":
                {
                    mobj1.remove("events");
                    mobj2.remove("events");
                }; break;
            }
            Map<String, Object> map1 = JSONtoMap(mobj1);
            Map<String, Object> map2 = JSONtoMap(mobj2);
            MapDifference<String, Object> mapDifference = Maps.difference(map1,map2);
            Map<String,MapDifference.ValueDifference<Object>> diff = mapDifference.entriesDiffering();
            Set<String> diff_keys = diff.keySet();
            for (String key : diff_keys)
            {
                MapDifference.ValueDifference<Object> o = diff.get(key);
                JSONObject up = new JSONObject();
                up.put("command","update");
                up.put("what", what);
                up.put("type", "live");
                up.put("id", mobj1.get("_id"));
                if (what.equals("game"))
                {
                    if (!key.equals("view"))
                    {
                        up.put("param", key);
                        up.put("value",o.rightValue());
                    }
                }
                else
                {
                    up.put("param", key);
                    up.put("value",o.rightValue());
                }
                if (up.containsKey("param") && up.containsKey("value"))
                {
                    update(up);
                }
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            main.errorLogger.error("Error happened", e);
            e.printStackTrace();
        }
    }

    public void check_game_param(String param, JSONObject obj, JSONObject obj_u, String id)
    {
        try {
            if (obj_u.containsKey(param))
            {
                if (obj.containsKey(param))
                {
                    switch (obj.get(param).getClass().getName())
                    {
                        case "java.lang.Integer":
                        {
                            Integer v, v_u;
                            v = new Integer(Integer.parseInt(obj.get(param).toString()));
                            v_u = new Integer(Integer.parseInt(obj_u.get(param).toString()));
                            if (!v_u.equals(v))
                            {
                                JSONObject up = new JSONObject();
                                up.put("command","update");
                                up.put("what", "game");
                                up.put("type", "live");
                                up.put("id", id);
                                if (param.equals("true_time")) up.put("param", "time"); else up.put("param", param);
                                up.put("value", v_u);
                                update(up);
                                ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                                for (String tid : terminals.keySet())
                                {
                                    JSONObject sinfo = terminals.get(tid);
                                    Session rcpt = (Session) sinfo.get("session");
                                    if (sinfo.get("gid")!=null)
                                    {
                                        if (sinfo.get("gid").equals(id) && rcpt.isOpen())
                                        {
                                            JSONObject stup = new JSONObject();
                                            stup.put("command","statistic");
                                            stup.put("what", "game");
                                            stup.put("type", "live");
                                            stup.put("gid", id);
                                            stup.put("id", id);
                                            if (param.equals("true_time")){
                                                stup.put("param", "time");
                                                stup.put("current_game_time", obj_u.get("time").toString());
                                            }
                                            else stup.put("param", param);
                                            stup.put("value", v_u);
                                            updateME(stup);
                                        }
                                    }
                                }
                            }
                        }; break;
                        case "java.lang.String":
                        {
                            String v, v_u;
                            v = (String)obj.get(param);
                            v_u = (String)obj_u.get(param);
                            if (!v_u.equals(v))
                            {
                                JSONObject up = new JSONObject();
                                up.put("command","update");
                                up.put("what", "game");
                                up.put("type", "live");
                                up.put("id", id);
                                if (param.equals("true_time")) up.put("param", "time"); else up.put("param", param);
                                if(param.equals("state"))
                                {
                                    up.put("value", obj_u.get("state"));
                                    up.put("true_value", obj_u.get("true_state"));
                                }
                                else up.put("value", v_u);
                                update(up);
                                ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                                for (String tid : terminals.keySet())
                                {
                                    JSONObject sinfo = terminals.get(tid);
                                    Session rcpt = (Session) sinfo.get("session");
                                    if (sinfo.get("gid")!=null)
                                    {
                                        if (sinfo.get("gid").equals(id) && rcpt.isOpen())
                                        {
                                            JSONObject stup = new JSONObject();
                                            stup.put("command","statistic");
                                            stup.put("what", "game");
                                            stup.put("type", "live");
                                            stup.put("gid", id);
                                            stup.put("id", id);
                                            if (param.equals("true_time")){
                                                stup.put("param", "time");
                                                stup.put("current_game_time", obj_u.get("time").toString());
                                            }
                                            else stup.put("param", param);
                                            stup.put("value", v_u);
                                            updateME(stup);
                                        }
                                    }
                                }
                            }
                        }; break;
                    }
                }
                else
                {
                    switch (obj_u.get(param).getClass().getName())
                    {
                        case "java.lang.Integer":
                        {
                            Integer v_u = new Integer(Integer.parseInt(obj_u.get(param).toString()));
                            JSONObject up = new JSONObject();
                            up.put("command","update");
                            up.put("what", "game");
                            up.put("type", "live");
                            up.put("id", id);
                            up.put("param", param);
                            up.put("value", v_u);
                            update(up);
                            ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                            for (String tid : terminals.keySet())
                            {
                                JSONObject sinfo = terminals.get(tid);
                                Session rcpt = (Session) sinfo.get("session");
                                if (sinfo.get("gid")!=null)
                                {
                                    if (sinfo.get("gid").equals(id) && rcpt.isOpen())
                                    {
                                        JSONObject stup = new JSONObject();
                                        stup.put("command","statistic");
                                        stup.put("what", "game");
                                        stup.put("type", "live");
                                        stup.put("gid", id);
                                        stup.put("id", id);
                                        stup.put("param", param);
                                        stup.put("value", v_u);
                                        updateME(stup);
                                    }
                                }
                            }
                        }; break;
                        case "java.lang.String":
                        {
                            String v_u = (String)obj_u.get(param);
                            JSONObject up = new JSONObject();
                            up.put("command","update");
                            up.put("what", "game");
                            up.put("type", "live");
                            up.put("id", id);
                            up.put("param", param);
                            up.put("value", v_u);
                            update(up);
                            ConcurrentHashMap<String,JSONObject> terminals = (ConcurrentHashMap<String,JSONObject>)main.terminals;
                            for (String tid : terminals.keySet())
                            {
                                JSONObject sinfo = terminals.get(tid);
                                Session rcpt = (Session) sinfo.get("session");
                                if (sinfo.get("gid")!=null)
                                {
                                    if (sinfo.get("gid").equals(id) && rcpt.isOpen())
                                    {
                                        JSONObject stup = new JSONObject();
                                        stup.put("command","statistic");
                                        stup.put("what", "game");
                                        stup.put("type", "live");
                                        stup.put("gid", id);
                                        stup.put("id", id);
                                        stup.put("param", param);
                                        stup.put("value", v_u);
                                        updateME(stup);
                                    }
                                }
                            }
                        }; break;
                    }
                }
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            main.errorLogger.error("Error happened", e);
            e.printStackTrace();
        }
    }

    void compare_last_event(JSONObject last, JSONObject last_u, String gid)
    {
        if (!last.equals(last_u))
        {
            JSONObject obj = new JSONObject();
            obj.put("command", "statistic");
            obj.put("what", "last_event");
            obj.put("type", "live");
            obj.put("gid", gid);
            obj.put("data", last_u);
            updateME(obj);
        }
    }

    void compare_live_events(List<JSONObject> list, List<JSONObject> list_u, String gid)
    {
        if (list_u != null )
        {
            for (JSONObject le : list_u)
            {
                if (!list.contains(le))
                {
                    JSONObject obj = new JSONObject();
                    obj.put("command", "statistic");
                    obj.put("what", "live_event");
                    obj.put("type", "live");
                    obj.put("gid", gid);
                    obj.put("data", le);
                    updateME(obj);
                }
            }
        }
    }
}
