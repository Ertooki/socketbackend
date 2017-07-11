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

/**
 * Created by Administrator on 12.04.2017.
 */
public class FliveUpdater extends Thread {

    public Map<String, JSONObject> data = new ConcurrentHashMap<String, JSONObject>();
    ws_client client = new ws_client("Flive updater");
    JSONParser parser = new JSONParser();
    boolean update = false;
    CountDownLatch latch = null;

    private static final Map<String, String> sportPartsT;
    private static final Map<String, String> sportPartsGer;

    static {
        sportPartsT = new HashMap<String, String>();
        sportPartsT.put("858", "Round");
        sportPartsT.put("848", "Set");
        sportPartsT.put("1308102967", "Half");
        sportPartsT.put("846", "Period");
        sportPartsT.put("850", "Quarter");
        sportPartsT.put("852", "Set");
        sportPartsT.put("854", "Half");
        sportPartsT.put("856", "Inning");
        sportPartsT.put("862", "Set");
        sportPartsT.put("864", "Period");
        sportPartsT.put("866", "Half");
        sportPartsT.put("36116468", "Half");
        sportPartsT.put("868", "Frame");
        sportPartsT.put("870", "Quarter");
        sportPartsT.put("886", "Quarter");
        sportPartsT.put("872", "Period");
        sportPartsT.put("874", "Half");
        sportPartsT.put("878", "Period");
        sportPartsT.put("884", "Set");
        sportPartsT.put("900", "Game");
        sportPartsT.put("197402321", "Game");
        sportPartsT.put("108949150", "Quarter");
        sportPartsGer = new HashMap<String, String>();
        sportPartsGer.put("858", "Runden");
        sportPartsGer.put("848", "Satz");
        sportPartsGer.put("1308102967", "Hälfte");
        sportPartsGer.put("846", "Periode");
        sportPartsGer.put("850", "Viertel");
        sportPartsGer.put("852", "Satz");
        sportPartsGer.put("854", "Hälfte");
        sportPartsGer.put("856", "Inning");
        sportPartsGer.put("862", "Satz");
        sportPartsGer.put("864", "Periode");
        sportPartsGer.put("866", "Hälfte");
        sportPartsGer.put("36116468", "Hälfte");
        sportPartsGer.put("868", "Frame");
        sportPartsGer.put("870", "Viertel");
        sportPartsGer.put("886", "Viertel");
        sportPartsGer.put("872", "Period");
        sportPartsGer.put("874", "Hälfte");
        sportPartsGer.put("878", "Periode");
        sportPartsGer.put("884", "Satz");
        sportPartsGer.put("900", "Spiel");
        sportPartsGer.put("197402321", "Spiel");
        sportPartsGer.put("108949150", "Viertel");
    }

    BlockingQueue<JSONObject> queue = new LinkedBlockingQueue<JSONObject>();

    BlockingQueue<JSONObject> getQueue() {
        return queue;
    }

    FliveUpdater(CountDownLatch l) {
        this.latch = l;
        client.addMessageHandler(new ws_client.MessageHandler() {
            public void handleMessage(String message) {
                JSONParser parser = new JSONParser();
                JSONObject rcvd;
                try {
                    rcvd = (JSONObject) parser.parse(message);
                    if (rcvd.containsKey("rid")) {
                        if (!rcvd.get("rid").toString().isEmpty()) {
                            JSONObject data = (JSONObject) rcvd.get("data");
                            if (data.containsKey("data")) {
                                JSONObject data2 = (JSONObject) data.get("data");
                                JSONObject tdata = new JSONObject();
                                tdata.put("type", "data");
                                tdata.put("id", rcvd.get("rid"));
                                tdata.put("data", data2);
                                getQueue().put(tdata);
                            }
                        }
                    }

                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    main.errorLogger.error("Error happened", e);
                    e.printStackTrace();
                }
            }
        });
    }

    public void run() {
        try {
            while (!interrupted()) {
                if (!queue.isEmpty()) {
                    JSONObject qd = queue.take();
                    switch ((String) qd.get("type")) {
                        case "start": {
                            ClientManager cm = ClientManager.createClient();
                            cm.getProperties().put("org.glassfish.tyrus.incomingBufferSize", 104857600);
                            cm.connectToServer(client, new URI("ws://swarm.solidarbet.com:8085"));

                            JSONObject get_info = new JSONObject();

                            get_info.put("command", "get");
                            get_info.put("rid", new Integer(4));

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

                            Date end_day = new Date();
                            end_day.setHours(23);
                            end_day.setMinutes(59);
                            end_day.setSeconds(59);

                            JSONObject where = new JSONObject();
                            JSONObject game = new JSONObject();
                            JSONArray and1_cond = new JSONArray();
                            JSONObject t = new JSONObject();
                            t.put("type", new Integer(0));
                            JSONObject now = new JSONObject();
                            JSONObject now_value = new JSONObject();
                            now_value.put("@gte", (int) (today.getTime() / 1000));
                            now.put("start_ts", now_value);
                            JSONObject end = new JSONObject();
                            JSONObject end_value = new JSONObject();
                            end_value.put("@lte", (int) (end_day.getTime() / 1000));
                            end.put("start_ts", end_value);
                            JSONObject thc = new JSONObject();
                            JSONObject fl = new JSONObject();
                            fl.put("flive", true);
                            thc.put("descr", fl);
                            and1_cond.add(t);
                            and1_cond.add(now);
                            and1_cond.add(end);
                            and1_cond.add(thc);
                            game.put("@and", and1_cond);
                            where.put("game", game);

                            params.put("where", where);
                            params.put("subscribe", false);

                            get_info.put("params", params);

                            client.sendMessage(get_info.toString());
                        }
                        ;
                        break;
                        case "data": {
                            JSONObject narr_data = (JSONObject) qd.get("data");
                            JSONObject sport = (JSONObject) narr_data.get("sport");
                            if (!update) build_data(sport.keySet(), sport);
                            else form_data_update(sport.keySet(), sport);
                        }
                        ;
                        break;
                    }
                }
            }
        } catch (Exception e) {
            main.errorLogger.error("Error happened", e);
        } finally {
            main.sendNotification("Flive updater crash", new Date()+"\n\n  Flive updater stopped");
        }
    }

    public void build_data(Set<String> sids_u, JSONObject sport) {
        try {
            for (String sid : sids_u) {
                JSONObject sport_node = new JSONObject();
                JSONObject region_node = new JSONObject();
                sport_node = (JSONObject) sport.get(sid);
                JSONObject region = new JSONObject();
                region = (JSONObject) sport_node.get("region");
                Set<String> region_ids = region.keySet();
                List<JSONObject> comps_arr = new ArrayList<JSONObject>();
                List<JSONObject> games_arr = new ArrayList<JSONObject>();
                int s_l = 0;
                String comps = "";
                Map<String, JSONObject> regions = new HashMap<String, JSONObject>();
                for (String rid : region_ids) {
                    region_node = (JSONObject) region.get(rid);
                    JSONObject comp = new JSONObject();
                    comp = (JSONObject) region_node.get("competition");
                    Set<String> comp_ids = comp.keySet();
                    List<JSONObject> cl = new ArrayList<JSONObject>();
                    Map<String, JSONObject> cmps = new HashMap<String, JSONObject>();
                    for (String cid : comp_ids) {
                        JSONObject comp_node = new JSONObject();
                        comp_node = (JSONObject) comp.get(cid);
                        JSONObject game = new JSONObject();
                        game = (JSONObject) comp_node.get("game");
                        Set<String> game_ids = game.keySet();
                        List<String> gvl = new ArrayList<String>();
                        int c_l = 0;
                        Map<String, JSONObject> gms = new HashMap<String, JSONObject>();
                        for (String gid : game_ids) {
                            JSONObject game_node = new JSONObject();
                            game_node = (JSONObject) game.get(gid);
                            JSONObject add = new JSONObject();
                            add.put("_id", gid);
                            add.put("cid", cid);
                            add.put("sid", sid);
                            add.put("start", game_node.get("start_ts").toString());
                            add.put("type", new Integer(Integer.parseInt(game_node.get("type").toString())));
                            if (game_node.containsKey("game_external_id"))
                                add.put("external-id", game_node.get("game_external_id").toString());
                            if (game_node.get("game_number").getClass() != java.lang.Boolean.class)
                                add.put("alias", game_node.get("game_number").toString());
                            else add.put("alias", gid);
                            if (game_node.containsKey("team2_name")) {
                                add.put("team1", game_node.get("team1_name"));
                                add.put("team2", game_node.get("team2_name"));
                            } else add.put("team1", game_node.get("team1_name"));
                            if (game_node.containsKey("markets_count"))
                                add.put("mc", new Integer(Integer.parseInt(game_node.get("markets_count").toString())));
                            if (game_node.containsKey("exclude_ids")) {
                                if (game_node.get("exclude_ids") != null) {
                                    if (!game_node.get("exclude_ids").getClass().isArray())
                                        add.put("excl_id", game_node.get("exclude_ids").toString());
                                    else add.put("excl_id", game_node.get("exclude_ids"));
                                }
                            }
                            JSONObject market = new JSONObject();
                            market = (JSONObject) game_node.get("market");
                            Set<String> market_ids = market.keySet();
                            Map<String, JSONObject> mkts = new HashMap<String, JSONObject>();
                            for (String mid : market_ids) {
                                JSONObject market_node = new JSONObject();
                                market_node = (JSONObject) market.get(mid);
                                JSONObject nme = new JSONObject();
                                nme.put("_id", mid);
                                nme.put("gid", gid);
                                if (market_node.containsKey("express_id"))
                                    nme.put("exp_id", market_node.get("express_id").toString());

                                if (market_node.containsKey("name")) {
                                    nme.put("name", market_node.get("name").toString());
                                } else {
                                    nme.put("name", "No name");
                                }

                                if (market_node.containsKey("type")) {
                                    nme.put("type", market_node.get("type").toString());
                                } else {
                                    nme.put("type", "No type");
                                }

                                if (market_node.containsKey("base")) {
                                    DecimalFormat df = new DecimalFormat("#.##");
                                    df.setRoundingMode(RoundingMode.CEILING);
                                    nme.put("base", df.format(Double.parseDouble(market_node.get("base").toString())).replaceAll(",", "."));
                                } else {
                                    nme.put("base", "@");
                                }

                                String mName = nme.get("name").toString();
                                String[] mNameArr = mName.split("( +)");
                                mName = String.join(" ", mNameArr);
                                String mType = nme.get("type").toString();
                                String[] mTypeArr = mType.split("( +)");
                                mType = String.join(" ", mTypeArr);
                                String mBase = nme.get("base").toString();
                                JSONObject gerMarket = new JSONObject();

                                if (main.gerMarkets.containsKey(mType)) {
                                    gerMarket = (JSONObject) main.gerMarkets.get(mType);
                                } else if (main.gerMarkets.containsKey(mType + " " + mBase)) {
                                    gerMarket = (JSONObject) main.gerMarkets.get(mType + " " + mBase);
                                } else if (main.gerMarkets.containsKey(mName)) {
                                    gerMarket = (JSONObject) main.gerMarkets.get(mName);
                                } else if (main.gerMarkets.containsKey(mName + " " + mBase)) {
                                    gerMarket = (JSONObject) main.gerMarkets.get(mName + " " + mBase);
                                }

                                if (gerMarket.containsKey("name")) {
                                    if (!mBase.equals("@")) nme.put("name", gerMarket.get("name") + " " + mBase);
                                    else nme.put("name", gerMarket.get("name"));
                                } else if (!mBase.equals("@")) nme.put("name", mName + " " + mBase);
                                else nme.put("name", mName);

                                if (market_node.containsKey("order"))
                                    nme.put("order", market_node.get("order").toString());
                                else nme.put("order", "999");

                                if (gerMarket.containsKey("bases")) {
                                    JSONObject bases = (JSONObject) gerMarket.get("bases");
                                    if (bases.containsKey(mBase)) {
                                        nme.put("order", bases.get(mBase));
                                    } else if (gerMarket.containsKey("order"))
                                        nme.put("order", gerMarket.get("order").toString());
                                } else if (gerMarket.containsKey("order"))
                                    nme.put("order", gerMarket.get("order").toString());

                                if (market_node.containsKey("show_type_DISABLE")) {
                                    JSONObject group = new JSONObject();
                                    group.put("type", market_node.get("show_type").toString());
                                    group.put("alias", market_node.get("show_type").toString());
                                    group.put("order", 1);
                                    nme.put("group", group);
                                } else {
                                    if (main.marketGroup.containsKey(nme.get("type").toString())) {
                                        nme.put("group", main.marketGroup.get(nme.get("type").toString()));
                                    } else {
                                        if (main.marketGroup.containsKey(nme.get("name").toString())) {
                                            nme.put("group", main.marketGroup.get(nme.get("name").toString()));
                                        } else {
                                            JSONObject group = new JSONObject();
                                            group.put("type", "NO GROUP");
                                            group.put("alias", "NO GROUP");
                                            group.put("order", "999");
                                            nme.put("group", group);
                                        }
                                    }
                                }
                                JSONObject event = new JSONObject();
                                event = (JSONObject) market_node.get("event");
                                Set<String> event_ids = event.keySet();
                                List<JSONObject> evnts = new ArrayList<JSONObject>();
                                Map<String, JSONObject> evnt = new HashMap<String, JSONObject>();
                                for (String eid : event_ids) {
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
                                    String[] eTypeArr = eType.split("( +)");
                                    eType = String.join(" ", eTypeArr);
                                    String eName = ne.get("name").toString();
                                    String[] eNameArr = eName.split("( +)");
                                    eName = String.join(" ", eNameArr);

                                    if (gerMarket.containsKey(eType)) {
                                        ne.put("name", gerMarket.get(eType));
                                    } else if (gerMarket.containsKey(eName)) {
                                        ne.put("name", gerMarket.get(eName));
                                    }

                                    if (mType.equals("NextGoal")) {
                                        if (eType.toLowerCase().contains("firstteam")) ne.put("name", "1");
                                        else if (eType.toLowerCase().contains("goal") &&
                                                !eType.toLowerCase().contains("firstteam") &&
                                                !eType.toLowerCase().contains("secondteam")) ne.put("name", "X");
                                        else if (eType.toLowerCase().contains("secondteam")) ne.put("name", "2");
                                    }

                                    if (event_node.containsKey("order"))
                                        ne.put("order", new Integer(Integer.parseInt(event_node.get("order").toString())));
                                    else ne.put("order", 999);
                                    if (market_node.containsKey("express_id"))
                                        ne.put("exp_id", market_node.get("express_id").toString());
                                    if (event_node.containsKey("price")) {
                                        DecimalFormat df = new DecimalFormat("#.##");
                                        df.setRoundingMode(RoundingMode.CEILING);
                                        ne.put("price", df.format(Double.parseDouble(event_node.get("price").toString())).toString().replaceAll(",", "."));
                                    } else {
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
                                for (JSONObject ne : evnts) {
                                    evnt.put(ne.get("_id").toString(), ne);
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
                                mkts.put(mid, nme);
                            }
                            games_arr.add(add);
                            s_l++;
                            c_l++;
                            add.put("markets", mkts);
                            gms.put(gid, add);
                        }
                        VelocityContext fl = new VelocityContext();
                        StringWriter f1 = new StringWriter();
                        fl.put("ralias", region_node.get("alias"));
                        fl.put("name", comp_node.get("name"));
                        fl.put("id", cid);
                        fl.put("descr", "f-live");
                        fl.put("games", gvl);
                        Velocity.mergeTemplate("comp_filter.vm", StandardCharsets.UTF_8.name(), fl, f1);
                        String competitionName = comp_node.get("name").toString().replaceFirst("-", "<br><hr>");
                        competitionName = competitionName.substring(competitionName.indexOf(".") + 1, competitionName.length()).trim();
                        StringWriter cli = new StringWriter();
                        VelocityContext ctx = new VelocityContext();
                        ctx.put("sport", "f-live");
                        ctx.put("id", sid);
                        ctx.put("cntr", region_node.get("alias"));
                        JSONObject c_row = new JSONObject();
                        c_row.put("_id", cid);
                        c_row.put("name", competitionName);
                        c_row.put("count", c_l);
                        ctx.put("comp", c_row);
                        Velocity.mergeTemplate("comp_li.vm", StandardCharsets.UTF_8.name(), ctx, cli);
                        JSONObject cmp = new JSONObject();
                        cmp.put("_id", cid);
                        cmp.put("rsid", sid + rid);
                        cmp.put("filter", f1.toString());
                        cmp.put("sid", sid);
                        cmp.put("name", comp_node.get("name"));
                        cmp.put("cmp_li", cli.toString());
                        comps_arr.add(cmp);
                        comps += cli.toString();
                        cmp.put("games", gms);
                        cmps.put(cid, cmp);
                    }
                    JSONObject rgn = new JSONObject();
                    rgn.put("_id",rid);
                    rgn.put("comps", cmps);
                    if (region_node.containsKey("alias")) {
                        rgn.put("alias", region_node.get("alias").toString());
                    } else {
                        rgn.put("alias", region_node.get("name").toString());
                    }
                    regions.put(rid, rgn);
                }
                int order;
                switch ((String) sport_node.get("alias")) {
                    case "Soccer":
                        order = 1;
                        break;
                    case "IceHockey":
                        order = 2;
                        break;
                    case "Volleyball":
                        order = 3;
                        break;
                    case "Basketball":
                        order = 4;
                        break;
                    case "Tennis":
                        order = 5;
                        break;
                    case "TableTennis":
                        order = 6;
                        break;
                    case "Badminton":
                        order = 7;
                        break;
                    default:
                        order = 999;
                        break;
                }
                VelocityContext ctx3 = new VelocityContext();
                StringWriter sfl = new StringWriter();
                ctx3.put("id", sid);
                ctx3.put("alias", (String) sport_node.get("alias"));
                ctx3.put("name", sport_node.get("name"));
                ctx3.put("sport", "f-live");
                ctx3.put("order", order);
                ctx3.put("cntr", region_node.get("alias"));
                ctx3.put("comps", comps);
                ctx3.put("fl", s_l);
                Velocity.mergeTemplate("sfl.vm", StandardCharsets.UTF_8.name(), ctx3, sfl);
                JSONObject sb = new JSONObject();
                sb.put("command", "build");
                JSONObject sdata = new JSONObject();
                sdata.put("_id", sid);
                sdata.put("alias", sport_node.get("alias"));
                sdata.put("name", sport_node.get("name"));
                sdata.put("s_l", s_l);
                sdata.put("order", order);
                sdata.put("menu", sfl.toString());
                sb.put("sport", sdata);
                sdata.put("regions", regions);
                data.put(sid, sdata);
            }
            update = true;

            CountDownLatch tL = new CountDownLatch(1);
            JSONObject tQ = new JSONObject();
            tQ.put("data", this.data);
            tQ.put("latch", tL);
            main.fliveTotals.getQueue().put(tQ);
            tL.await();

            JSONObject get_info = new JSONObject();

            get_info.put("command", "get");
            get_info.put("rid", new Integer(4));

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

            Date end_day = new Date();
            end_day.setHours(23);
            end_day.setMinutes(59);
            end_day.setSeconds(59);

            JSONObject where = new JSONObject();
            JSONObject game = new JSONObject();
            JSONArray and1_cond = new JSONArray();
            JSONObject t = new JSONObject();
            t.put("type", new Integer(0));
            JSONObject now = new JSONObject();
            JSONObject now_value = new JSONObject();
            now_value.put("@gte", (int) (today.getTime() / 1000));
            now.put("start_ts", now_value);
            JSONObject end = new JSONObject();
            JSONObject end_value = new JSONObject();
            end_value.put("@lte", (int) (end_day.getTime() / 1000));
            end.put("start_ts", end_value);
            JSONObject thc = new JSONObject();
            JSONObject fl = new JSONObject();
            fl.put("flive", true);
            thc.put("descr", fl);
            and1_cond.add(t);
            and1_cond.add(now);
            and1_cond.add(end);
            and1_cond.add(thc);
            game.put("@and", and1_cond);
            where.put("game", game);

            params.put("where", where);
            params.put("subscribe", false);

            get_info.put("params", params);

            client.sendMessage(get_info.toString());

            this.latch.countDown();
            System.out.println("Flive has data. Counter:" + latch.getCount());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void form_data_update(Set<String> sids_u, JSONObject sport) {
        try {
            Map<String, JSONObject> udata = new ConcurrentHashMap<String, JSONObject>();
            for (String sid : sids_u) {
                JSONObject sport_node = new JSONObject();
                JSONObject region_node = new JSONObject();
                sport_node = (JSONObject) sport.get(sid);
                JSONObject region = new JSONObject();
                region = (JSONObject) sport_node.get("region");
                Set<String> region_ids = region.keySet();
                List<JSONObject> comps_arr = new ArrayList<JSONObject>();
                List<JSONObject> games_arr = new ArrayList<JSONObject>();
                int s_l = 0;
                String comps = "";
                Map<String, JSONObject> regions = new HashMap<String, JSONObject>();
                for (String rid : region_ids) {
                    region_node = (JSONObject) region.get(rid);
                    JSONObject comp = new JSONObject();
                    comp = (JSONObject) region_node.get("competition");
                    Set<String> comp_ids = comp.keySet();
                    List<JSONObject> cl = new ArrayList<JSONObject>();
                    Map<String, JSONObject> cmps = new HashMap<String, JSONObject>();
                    for (String cid : comp_ids) {
                        JSONObject comp_node = new JSONObject();
                        comp_node = (JSONObject) comp.get(cid);
                        JSONObject game = new JSONObject();
                        game = (JSONObject) comp_node.get("game");
                        Set<String> game_ids = game.keySet();
                        List<String> gvl = new ArrayList<String>();
                        int c_l = 0;
                        Map<String, JSONObject> gms = new HashMap<String, JSONObject>();
                        for (String gid : game_ids) {
                            JSONObject game_node = new JSONObject();
                            game_node = (JSONObject) game.get(gid);
                            JSONObject add = new JSONObject();
                            add.put("_id", gid);
                            add.put("cid", cid);
                            add.put("sid", sid);
                            add.put("start", game_node.get("start_ts").toString());
                            add.put("type", new Integer(Integer.parseInt(game_node.get("type").toString())));
                            if (game_node.containsKey("game_external_id"))
                                add.put("external-id", game_node.get("game_external_id").toString());
                            if (game_node.get("game_number").getClass() != java.lang.Boolean.class)
                                add.put("alias", game_node.get("game_number").toString());
                            else add.put("alias", gid);
                            if (game_node.containsKey("team2_name")) {
                                add.put("team1", game_node.get("team1_name"));
                                add.put("team2", game_node.get("team2_name"));
                            } else add.put("team1", game_node.get("team1_name"));
                            if (game_node.containsKey("markets_count"))
                                add.put("mc", new Integer(Integer.parseInt(game_node.get("markets_count").toString())));
                            if (game_node.containsKey("exclude_ids")) {
                                if (game_node.get("exclude_ids") != null) {
                                    if (!game_node.get("exclude_ids").getClass().isArray())
                                        add.put("excl_id", game_node.get("exclude_ids").toString());
                                    else add.put("excl_id", game_node.get("exclude_ids"));
                                }
                            }
                            JSONObject market = new JSONObject();
                            market = (JSONObject) game_node.get("market");
                            Set<String> market_ids = market.keySet();
                            Map<String, JSONObject> mkts = new HashMap<String, JSONObject>();
                            for (String mid : market_ids) {
                                JSONObject market_node = new JSONObject();
                                market_node = (JSONObject) market.get(mid);
                                JSONObject nme = new JSONObject();
                                nme.put("_id", mid);
                                nme.put("gid", gid);
                                if (market_node.containsKey("express_id"))
                                    nme.put("exp_id", market_node.get("express_id").toString());

                                if (market_node.containsKey("name")) {
                                    nme.put("name", market_node.get("name").toString());
                                } else {
                                    nme.put("name", "No name");
                                }

                                if (market_node.containsKey("type")) {
                                    nme.put("type", market_node.get("type").toString());
                                } else {
                                    nme.put("type", "No type");
                                }

                                if (market_node.containsKey("base")) {
                                    DecimalFormat df = new DecimalFormat("#.##");
                                    df.setRoundingMode(RoundingMode.CEILING);
                                    nme.put("base", df.format(Double.parseDouble(market_node.get("base").toString())).replaceAll(",", "."));
                                } else {
                                    nme.put("base", "@");
                                }

                                String mName = nme.get("name").toString();
                                String[] mNameArr = mName.split("( +)");
                                mName = String.join(" ", mNameArr);
                                String mType = nme.get("type").toString();
                                String[] mTypeArr = mType.split("( +)");
                                mType = String.join(" ", mTypeArr);
                                String mBase = nme.get("base").toString();
                                JSONObject gerMarket = new JSONObject();

                                if (main.gerMarkets.containsKey(mType)) {
                                    gerMarket = (JSONObject) main.gerMarkets.get(mType);
                                } else if (main.gerMarkets.containsKey(mType + " " + mBase)) {
                                    gerMarket = (JSONObject) main.gerMarkets.get(mType + " " + mBase);
                                } else if (main.gerMarkets.containsKey(mName)) {
                                    gerMarket = (JSONObject) main.gerMarkets.get(mName);
                                } else if (main.gerMarkets.containsKey(mName + " " + mBase)) {
                                    gerMarket = (JSONObject) main.gerMarkets.get(mName + " " + mBase);
                                }

                                if (gerMarket.containsKey("name")) {
                                    if (!mBase.equals("@")) nme.put("name", gerMarket.get("name") + " " + mBase);
                                    else nme.put("name", gerMarket.get("name"));
                                } else if (!mBase.equals("@")) nme.put("name", mName + " " + mBase);
                                else nme.put("name", mName);

                                if (market_node.containsKey("order"))
                                    nme.put("order", market_node.get("order").toString());
                                else nme.put("order", "999");

                                if (gerMarket.containsKey("bases")) {
                                    JSONObject bases = (JSONObject) gerMarket.get("bases");
                                    if (bases.containsKey(mBase)) {
                                        nme.put("order", bases.get(mBase));
                                    } else if (gerMarket.containsKey("order"))
                                        nme.put("order", gerMarket.get("order").toString());
                                } else if (gerMarket.containsKey("order"))
                                    nme.put("order", gerMarket.get("order").toString());

                                if (market_node.containsKey("show_type_DISABLE")) {
                                    JSONObject group = new JSONObject();
                                    group.put("type", market_node.get("show_type").toString());
                                    group.put("alias", market_node.get("show_type").toString());
                                    group.put("order", 1);
                                    nme.put("group", group);
                                } else {
                                    if (main.marketGroup.containsKey(nme.get("type").toString())) {
                                        nme.put("group", main.marketGroup.get(nme.get("type").toString()));
                                    } else {
                                        if (main.marketGroup.containsKey(nme.get("name").toString())) {
                                            nme.put("group", main.marketGroup.get(nme.get("name").toString()));
                                        } else {
                                            JSONObject group = new JSONObject();
                                            group.put("type", "NO GROUP");
                                            group.put("alias", "NO GROUP");
                                            group.put("order", "999");
                                            nme.put("group", group);
                                        }
                                    }
                                }
                                JSONObject event = new JSONObject();
                                event = (JSONObject) market_node.get("event");
                                Set<String> event_ids = event.keySet();
                                List<JSONObject> evnts = new ArrayList<JSONObject>();
                                Map<String, JSONObject> evnt = new HashMap<String, JSONObject>();
                                for (String eid : event_ids) {
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
                                    String[] eTypeArr = eType.split("( +)");
                                    eType = String.join(" ", eTypeArr);
                                    String eName = ne.get("name").toString();
                                    String[] eNameArr = eName.split("( +)");
                                    eName = String.join(" ", eNameArr);

                                    if (gerMarket.containsKey(eType)) {
                                        ne.put("name", gerMarket.get(eType));
                                    } else if (gerMarket.containsKey(eName)) {
                                        ne.put("name", gerMarket.get(eName));
                                    }

                                    if (mType.equals("NextGoal")) {
                                        if (eType.toLowerCase().contains("firstteam")) ne.put("name", "1");
                                        else if (eType.toLowerCase().contains("goal") &&
                                                !eType.toLowerCase().contains("firstteam") &&
                                                !eType.toLowerCase().contains("secondteam")) ne.put("name", "X");
                                        else if (eType.toLowerCase().contains("secondteam")) ne.put("name", "2");
                                    }

                                    if (event_node.containsKey("order"))
                                        ne.put("order", new Integer(Integer.parseInt(event_node.get("order").toString())));
                                    else ne.put("order", 999);
                                    if (market_node.containsKey("express_id"))
                                        ne.put("exp_id", market_node.get("express_id").toString());
                                    if (event_node.containsKey("price")) {
                                        DecimalFormat df = new DecimalFormat("#.##");
                                        df.setRoundingMode(RoundingMode.CEILING);
                                        ne.put("price", df.format(Double.parseDouble(event_node.get("price").toString())).toString().replaceAll(",", "."));
                                    } else {
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
                                for (JSONObject ne : evnts) {
                                    evnt.put(ne.get("_id").toString(), ne);
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
                                mkts.put(mid, nme);
                            }
                            games_arr.add(add);
                            s_l++;
                            c_l++;
                            add.put("markets", mkts);
                            gms.put(gid, add);
                        }
                        VelocityContext fl = new VelocityContext();
                        StringWriter f1 = new StringWriter();
                        fl.put("ralias", region_node.get("alias"));
                        fl.put("name", comp_node.get("name"));
                        fl.put("id", cid);
                        fl.put("descr", "f-live");
                        fl.put("games", gvl);
                        Velocity.mergeTemplate("comp_filter.vm", StandardCharsets.UTF_8.name(), fl, f1);
                        String competitionName = comp_node.get("name").toString().replaceFirst("-", "<br><hr>");
                        competitionName = competitionName.substring(competitionName.indexOf(".") + 1, competitionName.length()).trim();
                        StringWriter cli = new StringWriter();
                        VelocityContext ctx = new VelocityContext();
                        ctx.put("sport", "f-live");
                        ctx.put("id", sid);
                        ctx.put("cntr", region_node.get("alias"));
                        JSONObject c_row = new JSONObject();
                        c_row.put("_id", cid);
                        c_row.put("name", competitionName);
                        c_row.put("count", c_l);
                        ctx.put("comp", c_row);
                        Velocity.mergeTemplate("comp_li.vm", StandardCharsets.UTF_8.name(), ctx, cli);
                        JSONObject cmp = new JSONObject();
                        cmp.put("_id", cid);
                        cmp.put("rsid", sid + rid);
                        cmp.put("filter", f1.toString());
                        cmp.put("sid", sid);
                        cmp.put("name", comp_node.get("name"));
                        cmp.put("cmp_li", cli.toString());
                        comps_arr.add(cmp);
                        comps += cli.toString();
                        cmp.put("games", gms);
                        cmps.put(cid, cmp);
                    }
                    JSONObject rgn = new JSONObject();
                    rgn.put("_id",rid);
                    rgn.put("comps", cmps);
                    if (region_node.containsKey("alias")) {
                        rgn.put("alias", region_node.get("alias").toString());
                    } else {
                        rgn.put("alias", region_node.get("name").toString());
                    }
                    regions.put(rid, rgn);
                }
                int order;
                switch ((String) sport_node.get("alias")) {
                    case "Soccer":
                        order = 1;
                        break;
                    case "IceHockey":
                        order = 2;
                        break;
                    case "Volleyball":
                        order = 3;
                        break;
                    case "Basketball":
                        order = 4;
                        break;
                    case "Tennis":
                        order = 5;
                        break;
                    case "TableTennis":
                        order = 6;
                        break;
                    case "Badminton":
                        order = 7;
                        break;
                    default:
                        order = 999;
                        break;
                }
                VelocityContext ctx3 = new VelocityContext();
                StringWriter sfl = new StringWriter();
                ctx3.put("id", sid);
                ctx3.put("alias", (String) sport_node.get("alias"));
                ctx3.put("name", sport_node.get("name"));
                ctx3.put("sport", "f-live");
                ctx3.put("order", order);
                ctx3.put("cntr", region_node.get("alias"));
                ctx3.put("comps", comps);
                ctx3.put("fl", s_l);
                Velocity.mergeTemplate("sfl.vm", StandardCharsets.UTF_8.name(), ctx3, sfl);
                JSONObject sb = new JSONObject();
                sb.put("command", "build");
                JSONObject sdata = new JSONObject();
                sdata.put("_id", sid);
                sdata.put("alias", sport_node.get("alias"));
                sdata.put("name", sport_node.get("name"));
                sdata.put("s_l", s_l);
                sdata.put("order", order);
                sdata.put("menu", sfl.toString());
                sb.put("sport", sdata);
                sdata.put("regions", regions);
                udata.put(sid, sdata);
            }
            if (update) update_data(udata);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void update_data(Map<String, JSONObject> udata) {
        Set<String> keys1;
        Set<String> keys2;
        List<JSONObject> meUps = new ArrayList<>();
        try {
            //del_data("sport",udata,this.data);
            keys1 = new HashSet<String>(data.keySet());
            keys2 = new HashSet<String>(udata.keySet());
            keys1.removeAll(keys2);
            if (keys1.size() > 0) {
                JSONObject obj = new JSONObject();
                obj.put("command", "delete");
                obj.put("what", "sport");
                obj.put("ids", keys1);
                obj.put("type", "flive");
                update(obj);
            }
            for (String sid : udata.keySet()) {
                if (this.data.containsKey(sid)) {
                    Map<String, JSONObject> rgns_u = (Map<String, JSONObject>) udata.get(sid).get("regions");
                    Map<String, JSONObject> rgns = (Map<String, JSONObject>) this.data.get(sid).get("regions");
                    check_changes("sport", udata.get(sid), this.data.get(sid));
                    keys1 = new HashSet<String>(rgns.keySet());
                    keys2 = new HashSet<String>(rgns_u.keySet());
                    keys1.removeAll(keys2);
                    if (keys1.size() > 0) {
                        for (String key : keys1) {
                            JSONObject region = rgns.get(key);
                            Map<String, JSONObject> comps = (Map<String, JSONObject>) region.get("comps");
                            JSONObject obj = new JSONObject();
                            obj.put("command", "delete");
                            obj.put("what", "comp");
                            obj.put("ids", comps.keySet());
                            obj.put("type", "flive");
                            update(obj);
                            for (String cid : comps.keySet()) {
                                JSONObject comp = comps.get(cid);
                                Map<String, JSONObject> games = (Map<String, JSONObject>) comp.get("games");
                                obj = new JSONObject();
                                obj.put("command", "delete");
                                obj.put("what", "game");
                                obj.put("ids", games.keySet());
                                obj.put("type", "flive");
                                update(obj);
                            }
                        }
                    }
                    for (String rid : rgns_u.keySet()) {
                        if (rgns.containsKey(rid)) {
                            Map<String, JSONObject> comps_u = (Map<String, JSONObject>) rgns_u.get(rid).get("comps");
                            Map<String, JSONObject> comps = (Map<String, JSONObject>) rgns.get(rid).get("comps");
                            //del_data("comp",comps_u,comps);
                            keys1 = new HashSet<String>(comps.keySet());
                            keys2 = new HashSet<String>(comps_u.keySet());
                            keys1.removeAll(keys2);
                            if (keys1.size() > 0) {
                                JSONObject obj = new JSONObject();
                                obj.put("command", "delete");
                                obj.put("what", "comp");
                                obj.put("ids", keys1);
                                obj.put("type", "flive");
                                update(obj);
                                for (String key : keys1) {
                                    JSONObject comp = comps.get(key);
                                    Map<String, JSONObject> games = (Map<String, JSONObject>) comp.get("games");
                                    obj = new JSONObject();
                                    obj.put("command", "delete");
                                    obj.put("what", "game");
                                    obj.put("ids", games.keySet());
                                    obj.put("type", "flive");
                                    update(obj);
                                }
                            }
                            for (String cid : comps_u.keySet()) {
                                if (comps.containsKey(cid)) {
                                    Map<String, JSONObject> gms_u = (Map<String, JSONObject>) comps_u.get(cid).get("games");
                                    Map<String, JSONObject> gms = (Map<String, JSONObject>) comps.get(cid).get("games");
                                    check_changes("comp", comps_u.get(cid), comps.get(cid));
                                    //del_data("game",gms_u,gms);
                                    keys1 = new HashSet<String>(gms.keySet());
                                    keys2 = new HashSet<String>(gms_u.keySet());
                                    keys1.removeAll(keys2);
                                    if (keys1.size() > 0) {
                                        JSONObject obj = new JSONObject();
                                        obj.put("command", "delete");
                                        obj.put("what", "game");
                                        obj.put("ids", keys1);
                                        obj.put("type", "flive");
                                        update(obj);
                                    }
                                    for (String gid : gms_u.keySet()) {
                                        if (gms.containsKey(gid)) {
                                            Map<String, JSONObject> mkts_u = (Map<String, JSONObject>) gms_u.get(gid).get("markets");
                                            Map<String, JSONObject> mkts = (Map<String, JSONObject>) gms.get(gid).get("markets");
                                            //del_data("market",mkts_u,mkts);
                                            keys1 = new HashSet<String>(mkts.keySet());
                                            keys2 = new HashSet<String>(mkts_u.keySet());
                                            keys1.removeAll(keys2);
                                            if (keys1.size() > 0) {
                                                JSONObject obj = new JSONObject();
                                                obj.put("command", "delete");
                                                obj.put("what", "market");
                                                obj.put("ids", keys1);
                                                obj.put("type", "flive");
                                                meUps.add(obj);
                                            }
                                            for (String mid : mkts_u.keySet()) {
                                                if (mkts.containsKey(mid)) {
                                                    Map<String, JSONObject> evnts_u = (Map<String, JSONObject>) mkts_u.get(mid).get("events");
                                                    Map<String, JSONObject> evnts = (Map<String, JSONObject>) mkts.get(mid).get("events");
                                                    //del_data("event",evnts_u,evnts);
                                                    keys1 = new HashSet<String>(evnts.keySet());
                                                    keys2 = new HashSet<String>(evnts_u.keySet());
                                                    keys1.removeAll(keys2);
                                                    if (keys1.size() > 0) {
                                                        JSONObject obj = new JSONObject();
                                                        obj.put("command", "delete");
                                                        obj.put("what", "event");
                                                        obj.put("ids", keys1);
                                                        obj.put("type", "flive");
                                                        meUps.add(obj);
                                                    }
                                                    for (String eid : evnts_u.keySet()) {
                                                        if (evnts.containsKey(eid)) {
                                                            //check_changes("event",evnts_u.get(eid),evnts.get(eid));
                                                            if (!evnts_u.get(eid).get("price").equals(evnts.get(eid).get("price"))) {
                                                                JSONObject up = new JSONObject();
                                                                if (mkts_u.get(mid).containsKey("type"))
                                                                    up.put("market_type", mkts_u.get(mid).get("type"));
                                                                up.put("command", "update");
                                                                up.put("what", "event");
                                                                up.put("id", evnts_u.get(eid).get("_id"));
                                                                up.put("gid", gid);
                                                                up.put("type", "flive");
                                                                up.put("param", "price");
                                                                up.put("value", evnts_u.get(eid).get("price"));
                                                                meUps.add(up);
                                                            }
                                                        } else {
                                                            JSONObject eve = (JSONObject) evnts_u.get(eid);
                                                            JSONObject ne = new JSONObject();
                                                            ne.put("command", "new");
                                                            ne.put("what", "event");
                                                            ne.put("type", "flive");
                                                            ne.put("data", eve);
                                                            if (mkts_u.get(mid).containsKey("type"))
                                                                ne.put("market_type", mkts_u.get(mid).get("type"));
                                                            ne.put("gid", gid);
                                                            ne.put("id", eid);
                                                            meUps.add(ne);
                                                        }
                                                    }
                                                } else {
                                                    JSONObject market = (JSONObject) mkts_u.get(mid);
                                                    JSONObject nm = new JSONObject();
                                                    nm.put("command", "new");
                                                    nm.put("what", "market");
                                                    nm.put("type", "flive");
                                                    JSONObject m = new JSONObject();
                                                    JSONArray eids = new JSONArray();
                                                    m.put("id", mid);
                                                    if (market.containsKey("order"))
                                                        m.put("order", market.get("order").toString());
                                                    m.put("html", market.get("html"));
                                                    m.put("eIds", ((Map<String, JSONObject>) market.get("events")).keySet());
                                                    nm.put("data_vars", m);
                                                    nm.put("data", market);
                                                    if (market.containsKey("type"))
                                                        nm.put("market_type", market.get("type"));
                                                    nm.put("gid", gid);
                                                    nm.put("sid", sid);
                                                    if(market.containsKey("type")){
                                                        if(sid.equals("848")) {
                                                            if(market.get("type").toString().equals("Gametotalpoints")
                                                                    && !market.get("base").toString().equals("@")
                                                                    && Double.parseDouble(market.get("base").toString())%1 != 0.0){
                                                                Map<String,JSONObject> events = (Map<String,JSONObject>)market.get("events");
                                                                for (String eid : events.keySet()) {
                                                                    JSONObject event = events.get(eid);
                                                                    if(event.get("type").toString().toLowerCase().equals("totalmore") ||
                                                                            event.get("type").toString().toLowerCase().equals("over")||
                                                                            event.get("type").toString().toLowerCase().equals("more")) {
                                                                        if(Double.parseDouble(event.get("price").toString())>=1.4 &&
                                                                                Double.parseDouble(event.get("price").toString())<=2.55) {
                                                                            meUps.add(nm);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            else meUps.add(nm);
                                                        }
                                                        else {
                                                            if(market.get("type").toString().equals("Total")
                                                                    && !market.get("base").toString().equals("@")
                                                                    && Double.parseDouble(market.get("base").toString())%1 != 0.0) {
                                                                Map<String,JSONObject> events = (Map<String,JSONObject>)market.get("events");
                                                                for (String eid : events.keySet()) {
                                                                    JSONObject event = events.get(eid);
                                                                    if(event.get("type").toString().toLowerCase().equals("totalmore") ||
                                                                            event.get("type").toString().toLowerCase().equals("over")||
                                                                            event.get("type").toString().toLowerCase().equals("more")) {
                                                                        if(Double.parseDouble(event.get("price").toString())>=1.4 &&
                                                                                Double.parseDouble(event.get("price").toString())<=2.55) {
                                                                            meUps.add(nm);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            else meUps.add(nm);
                                                        }
                                                    }
                                                    else meUps.add(nm);
                                                }
                                            }
                                        }
                                        else {
                                            addNewGame(udata.get(sid),rgns_u.get(rid),comps_u.get(cid),gms_u.get(gid));
                                        }
                                    }
                                } else {
                                    Map<String, JSONObject> gms = (Map<String, JSONObject>) comps_u.get(cid).get("games");
                                    for (String gid : gms.keySet()) {
                                        addNewGame(udata.get(sid),rgns_u.get(rid),comps_u.get(cid),gms.get(gid));
                                    }
                                }
                            }
                        } else {
                            Map<String, JSONObject> comps = (Map<String, JSONObject>) rgns_u.get(rid).get("comps");
                            for (String cid : comps.keySet()) {
                                Map<String, JSONObject> gms = (Map<String, JSONObject>) comps.get(cid).get("games");
                                for (String gid : gms.keySet()) {
                                    addNewGame(udata.get(sid),rgns_u.get(rid),comps.get(cid),gms.get(gid));
                                }
                            }
                        }
                    }
                } else {
                    Map<String, JSONObject> rgns_u = (Map<String, JSONObject>) udata.get(sid).get("regions");
                    for (String rid : rgns_u.keySet()) {
                        Map<String, JSONObject> comps = (Map<String, JSONObject>) rgns_u.get(rid).get("comps");
                        for (String cid : comps.keySet()) {
                            Map<String, JSONObject> gms = (Map<String, JSONObject>) comps.get(cid).get("games");
                            for (String gid : gms.keySet()) {
                                addNewGame(udata.get(sid),rgns_u.get(rid),comps.get(cid),gms.get(gid));
                            }
                        }
                    }
                }
            }
            data = new ConcurrentHashMap<String, JSONObject>(udata);

            CountDownLatch tL = new CountDownLatch(1);
            JSONObject tQ = new JSONObject();
            tQ.put("data", this.data);
            tQ.put("latch", tL);
            main.fliveTotals.getQueue().put(tQ);
            tL.await();

            for (JSONObject meUp : meUps) {
                if (meUp.get("command").toString().equals("delete")) {
                    update(meUp);
                } else {
                    updateME(meUp, meUp.get("what").toString());
                }
            }
            if (update) {
                JSONObject get_info = new JSONObject();

                get_info.put("command", "get");
                get_info.put("rid", new Integer(4));

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
                Date end_day = new Date();
                end_day.setHours(23);
                end_day.setMinutes(59);
                end_day.setSeconds(59);

                JSONObject where = new JSONObject();
                JSONObject game = new JSONObject();
                JSONArray and1_cond = new JSONArray();
                JSONObject t = new JSONObject();
                t.put("type", new Integer(0));
                JSONObject now = new JSONObject();
                JSONObject now_value = new JSONObject();
                now_value.put("@gte", (int) (today.getTime() / 1000));
                now.put("start_ts", now_value);
                JSONObject end = new JSONObject();
                JSONObject end_value = new JSONObject();
                end_value.put("@lte", (int) (end_day.getTime() / 1000));
                end.put("start_ts", end_value);
                JSONObject thc = new JSONObject();
                JSONObject fl = new JSONObject();
                fl.put("flive", true);
                thc.put("descr", fl);
                and1_cond.add(t);
                and1_cond.add(now);
                and1_cond.add(end);
                and1_cond.add(thc);
                game.put("@and", and1_cond);
                where.put("game", game);

                params.put("where", where);
                params.put("subscribe", false);

                get_info.put("params", params);
                client.sendMessage(get_info.toString());
                //System.out.println(new Date() + "_" + "_sent update message_" + worker);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void addNewGame(JSONObject sport, JSONObject region, JSONObject comp, JSONObject game) {
        List<JSONObject> gr1 = new ArrayList<JSONObject>();
        List<JSONObject> gr2 = new ArrayList<JSONObject>();
        List<JSONObject> gr3 = new ArrayList<JSONObject>();
        List<JSONObject> gr4 = new ArrayList<JSONObject>();
        List<JSONObject> spec_totals = new ArrayList<>();
        List<JSONObject> all_totals = new ArrayList<>();
        String sid = sport.get("_id").toString();
        String rid = region.get("_id").toString();
        String cid = comp.get("_id").toString();
        Map<String, JSONObject> mkts_u = (Map<String, JSONObject>) game.get("markets");
        for (String mid : mkts_u.keySet()) {
            JSONObject market = (JSONObject) mkts_u.get(mid);
            if (market.containsKey("type")) {
                if (((String) market.get("type")).equals("P1XP2") || ((String) market.get("type")).equals("P1P2")) {
                    gr1.add(market);
                }
                if (((String) market.get("type")).equals("1X12X2")) gr2.add(market);
                if (((String) market.get("type")).equals("1HalfP1XP2")
                        || ((String) market.get("type")).equals("1SetP1XP2")
                        || ((String) market.get("type")).equals("1PeriodP1XP2"))
                    gr3.add(market);
                if(sid.equals("848")) {
                    if(market.get("type").toString().equals("Gametotalpoints")
                            && !market.get("base").toString().equals("@")
                            && Double.parseDouble(market.get("base").toString())%1 != 0.0){
                        all_totals.add(market);
                        Map<String,JSONObject> events = (Map<String,JSONObject>)market.get("events");
                        for (String eid : events.keySet()) {
                            JSONObject event = events.get(eid);
                            if(event.get("type").toString().toLowerCase().equals("totalmore") ||
                                    event.get("type").toString().toLowerCase().equals("over")||
                                    event.get("type").toString().toLowerCase().equals("more")) {
                                if(Double.parseDouble(event.get("price").toString())>=1.4 &&
                                        Double.parseDouble(event.get("price").toString())<=2.55) {
                                    spec_totals.add(market);
                                }
                            }
                        }
                    }
                }
                else {
                    if(market.get("type").toString().equals("Total")
                            && !market.get("base").toString().equals("@")
                            && Double.parseDouble(market.get("base").toString())%1 != 0.0){
                        all_totals.add(market);
                        Map<String,JSONObject> events = (Map<String,JSONObject>)market.get("events");
                        for (String eid : events.keySet()) {
                            JSONObject event = events.get(eid);
                            if(event.get("type").toString().toLowerCase().equals("totalmore") ||
                                    event.get("type").toString().toLowerCase().equals("over")||
                                    event.get("type").toString().toLowerCase().equals("more")) {
                                if(Double.parseDouble(event.get("price").toString())>=1.4 &&
                                        Double.parseDouble(event.get("price").toString())<=2.55) {
                                    spec_totals.add(market);
                                }
                            }
                        }
                    }
                }
            }
        }
        if (gr1.size() > 1) {
            if (gr1.get(0).get("type").toString().equals("P1XP2"))
                gr1.remove(gr1.get(1));
            else if (gr1.get(1).get("type").toString().equals("P1XP2"))
                gr1.remove(gr1.get(0));
        }
        if(spec_totals.size()>0){
            JSONObject needTotal = spec_totals.get(0);
            for (JSONObject total : spec_totals) {
                if (!total.get("base").toString().equals("@")
                        && Double.parseDouble(total.get("base").toString())%1 != 0.0) {
                    if (Double.parseDouble(total.get("base").toString()) < Double.parseDouble(needTotal.get("base").toString())) needTotal = total;
                }
            }
            //System.out.println("Adding game with fullgame total base: "+needTotal.get("base")+" "+Double.parseDouble(needTotal.get("base").toString())%1);
            gr4.add(needTotal);
        }
        else if (all_totals.size()>0){
            JSONObject needTotal = all_totals.get(0);
            for (JSONObject total : all_totals) {
                if (!total.get("base").toString().equals("@")
                        && Double.parseDouble(total.get("base").toString())%1 != 0.0) {
                    if (Double.parseDouble(total.get("base").toString()) < Double.parseDouble(needTotal.get("base").toString()))
                    {
                        needTotal = total;
                    }
                }
            }
            // System.out.println("Adding game with fullgame total base: "+needTotal.get("base")+" "+Double.parseDouble(needTotal.get("base").toString())%1);
            gr4.add(needTotal);
        }
        JSONObject ng = new JSONObject();
        ng.put("command", "new");
        ng.put("what", "game");
        ng.put("type", "flive");
        ng.put("comp_name", comp.get("name"));
        ng.put("sport_name", sport.get("name"));
        ng.put("sport_alias", sport.get("alias"));
        VelocityContext game_row = new VelocityContext();
        StringWriter row = new StringWriter();
        Date gs = new Date();
        gs.setTime(Long.parseLong(game.get("start").toString()) * 1000);
        SimpleDateFormat dt1 = new SimpleDateFormat("dd/MM");
        SimpleDateFormat dt2 = new SimpleDateFormat("HH:mm");
        game_row.put("game", game);
        game_row.put("talias", "0");
        game_row.put("day", dt1.format(gs).toString());
        game_row.put("hour", dt2.format(gs).toString());
        JSONObject sp_row = new JSONObject();
        sp_row.put("id", sid);
        sp_row.put("name", sport.get("name"));
        sp_row.put("alias", sport.get("alias"));
        game_row.put("sport", sp_row);
        JSONObject rcid = new JSONObject();
        rcid.put("rid", rid);
        rcid.put("cid", cid);
        game_row.put("rc", rcid);
        game_row.put("comp_name", (String) comp.get("name"));
        game_row.put("region_alias", region.get("alias"));
        game_row.put("gr1", gr1);
        game_row.put("gr2", gr2);
        game_row.put("gr3", gr3);
        game_row.put("gr4", gr4);
        Velocity.mergeTemplate("game_row.vm", StandardCharsets.UTF_8.name(), game_row, row);
        game.put("view", row.toString());
        ng.put("data", game);
        update(ng);
    }

    void update(JSONObject update)
    {
        try {
            ConcurrentHashMap<String,Terminal> terminals = (ConcurrentHashMap<String,Terminal>)main.terminals;
            for(String tid : terminals.keySet())
            {
                if (terminals.get(tid) != null) {
                    Terminal t = terminals.get(tid);
                    JSONObject qObj = new JSONObject();
                    qObj.put("type","update");
                    qObj.put("data",update);
                    t.getQueue().put(qObj);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    HashMap<String,JSONObject> recalcEvents (HashMap<String,JSONObject> events, String multi) {
        for (String eid : events.keySet()) {
            JSONObject event = events.get(eid);
            String price = event.get("price").toString();
            event.put("price", recalcEvent(price, multi));
        }
        return events;
    }

    String recalcEvent (String price, String multi) {
        Double currCoef = Double.parseDouble(price);
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
        return  df.format(multiPrice).toString().replaceAll(",", ".");
    }

    void updateME(JSONObject update,String type) {
        try {
            switch (type) {
                case "market": {
                    if (update.get("command").toString().equals("new")) {
                        ConcurrentHashMap<String,Terminal> terminals = (ConcurrentHashMap<String,Terminal>)main.terminals;
                        for(String tid : terminals.keySet()) {
                            HashMap<String, JSONObject> events = (HashMap<String, JSONObject>) ((JSONObject) update.get("data")).get("events");
                            Terminal t = terminals.get(tid);
                            if (t != null) {
                                ((JSONObject) update.get("data")).put("events", recalcEvents(events,t.getMulti()));
                                if (update.containsKey("market_type")) {
                                    if (update.get("market_type").equals("P1XP2") ||
                                            update.get("market_type").equals("P1P2") ||
                                            update.get("market_type").equals("1X12X2") ||
                                            update.get("market_type").equals("1HalfP1XP2") ||
                                            update.get("market_type").equals("1SetP1XP2") ||
                                            update.get("market_type").equals("1PeriodP1XP2") ||
                                            update.get("market_type").equals("Total")) {
                                        JSONObject qObj = new JSONObject();
                                        qObj.put("type", "update");
                                        qObj.put("data", update);
                                        t.getQueue().put(qObj);
                                    }
                                    else if (update.get("gid").toString().equals(t.game_id)) {
                                        System.out.println("Vars update of game "+update.get("gid")+" for terminal "+t.id);
                                        JSONObject qObj = new JSONObject();
                                        qObj.put("type", "update");
                                        qObj.put("data", update);
                                        t.getQueue().put(qObj);
                                    }
                                }
                                else if (update.get("gid").toString().equals(t.game_id)) {
                                    System.out.println("Vars update of game "+update.get("gid")+" for terminal "+t.id);
                                    JSONObject qObj = new JSONObject();
                                    qObj.put("type", "update");
                                    qObj.put("data", update);
                                    t.getQueue().put(qObj);
                                }
                            }
                        }
                    }
                }; break;
                case "event": {
                    ConcurrentHashMap<String,Terminal> terminals = (ConcurrentHashMap<String,Terminal>)main.terminals;
                    for(String tid : terminals.keySet()) {
                        Terminal t = terminals.get(tid);
                        if (t != null) {
                            if (update.get("command").toString().equals("new")) {
                                JSONObject event = (JSONObject) update.get("data");
                                event.put("price", recalcEvent(event.get("price").toString(), t.getMulti()));
                                update.put("data", event);
                            }
                            else if (update.get("command").toString().equals("update")) {
                                update.put("value", recalcEvent(update.get("value").toString(),t.getMulti()));
                            }

                            if(update.containsKey("market_type")) {
                                if (update.get("market_type").equals("P1XP2") ||
                                        update.get("market_type").equals("P1P2") ||
                                        update.get("market_type").equals("1X12X2") ||
                                        update.get("market_type").equals("1HalfP1XP2") ||
                                        update.get("market_type").equals("1SetP1XP2") ||
                                        update.get("market_type").equals("1PeriodP1XP2") ||
                                        update.get("market_type").equals("Total")) {
                                    JSONObject qObj = new JSONObject();
                                    qObj.put("type", "update");
                                    qObj.put("data", update);
                                    t.getQueue().put(qObj);
                                }
                                else if (update.get("gid").toString().equals(t.game_id)) {
                                    System.out.println("Vars update of game "+update.get("gid")+" for terminal "+t.id);
                                    JSONObject qObj = new JSONObject();
                                    qObj.put("type", "update");
                                    qObj.put("data", update);
                                    t.getQueue().put(qObj);
                                }
                                else if (t.betslip.contains(update.get("id").toString())) {
                                    System.out.println("Betslip update!");
                                    JSONObject qObj = new JSONObject();
                                    qObj.put("type", "update");
                                    qObj.put("data", update);
                                    t.getQueue().put(qObj);
                                }
                            }
                            else if (update.get("gid").toString().equals(t.game_id)) {
                                System.out.println("Vars update of game "+update.get("gid")+" for terminal "+t.id);
                                JSONObject qObj = new JSONObject();
                                qObj.put("type", "update");
                                qObj.put("data", update);
                                t.getQueue().put(qObj);
                            }
                            else if (t.betslip.contains(update.get("id").toString())) {
                                System.out.println("Betslip update!");
                                JSONObject qObj = new JSONObject();
                                qObj.put("type", "update");
                                qObj.put("data", update);
                                t.getQueue().put(qObj);
                            }
                        }
                    }
                }; break;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
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
}
