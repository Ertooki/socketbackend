import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.eclipse.jetty.websocket.api.Session;
import org.glassfish.tyrus.client.ClientManager;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Administrator on 12.04.2017.
 */
public class MenuUpdater extends Thread {

    public Map<String, JSONObject> data= new ConcurrentHashMap<String,JSONObject>();
    ws_client client = new ws_client("Menu updater");
    JSONParser parser = new JSONParser();
    boolean update = false;
    BlockingQueue<JSONObject> queue = new LinkedBlockingQueue<JSONObject>();
    CountDownLatch latch = null;

    BlockingQueue<JSONObject> getQueue(){
        return queue;
    }

    MenuUpdater (CountDownLatch l) {
        this.latch = l;
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


    public void run() {
        try {
            while (!interrupted()) {
                if (!queue.isEmpty()) {
                    JSONObject qd = queue.take();
                    switch ((String) qd.get("type")) {
                        case "start": {
                            ClientManager cm = ClientManager.createClient();
                            cm.getProperties().put("org.glassfish.tyrus.incomingBufferSize", 104857600);
                            cm.connectToServer(client, new URI("ws://swarm.solidarbet.com:8086"));

                            JSONObject get_info = new JSONObject();

                            get_info.put("command","get");
                            get_info.put("rid", new Integer(2));

                            JSONObject params = new JSONObject();
                            params.put("source", "betting");

                            JSONObject what = new JSONObject();
                            JSONArray empty = new JSONArray();
                            JSONArray for_game = new JSONArray();
                            for_game.add("@count");
                            what.put("sport", empty);
                            what.put("region", empty);
                            what.put("competition", empty);
                            what.put("game", for_game);

                            params.put("what", what);

                            Date today = new Date();

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
                            where.put("game", game);

                            params.put("where",where);
                            params.put("subscribe", false);

                            get_info.put("params", params);

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
            main.sendNotification("Menu updater crash", new Date()+"Menu updater stopped");
        }
    }

    public void build_data(Set<String> sids_u, JSONObject sport)
    {
        try {
            for (String sid : sids_u)
            {
                JSONObject sport_node = new JSONObject();
                JSONObject region_node = new JSONObject();
                sport_node = (JSONObject)sport.get(sid);
                JSONObject region = new JSONObject();
                region = (JSONObject) sport_node.get("region");
                Set<String> region_ids = region.keySet();
                List<JSONObject> rgns = new ArrayList<JSONObject>();
                String comps = "";
                Map<String,JSONObject> regions = new HashMap<String,JSONObject>();
                for (String rid : region_ids)
                {
                    region_node = (JSONObject)region.get(rid);
                    JSONObject comp = new JSONObject();
                    comp = (JSONObject) region_node.get("competition");
                    Set<String> comp_ids = comp.keySet();
                    List<JSONObject> comp_menu = new ArrayList<JSONObject>();
                    int r_w = 0;
                    Map<String,JSONObject> cmps = new HashMap<String,JSONObject>();
                    for(String cid : comp_ids)
                    {
                        JSONObject comp_node = new JSONObject();
                        comp_node = (JSONObject)comp.get(cid);
                        JSONObject game = new JSONObject();
                        game = (JSONObject) comp_node.get("game");
                        Set<String> game_ids = game.keySet();
                        int c_w = game_ids.size();
                        Map<String,JSONObject> gms = new HashMap<String,JSONObject>();
                        for (String gid : game_ids)
                        {
                            gms.put(gid, new JSONObject());
                        }
                        r_w += c_w;
                        if (c_w>0)
                        {
                            String cname = ((String)comp_node.get("name")).replaceAll("^([^0-9-.]+)(([\\s\\-]+)([^0-9-.]+))?([\\s\\-\\.]+)([^0-9-.]+)(([\\s\\-]+)([^0-9-.]+)$)?([\\s\\-\\.]+)", "");
                            VelocityContext ctx = new VelocityContext();
                            StringWriter menu = new StringWriter();
                            Integer order = 9999;
                            if(comp_node.containsKey("order")) {
                                if (comp_node.get("order").getClass() != java.lang.Boolean.class) order = new Integer(Integer.parseInt(comp_node.get("order").toString()));
                            }
                            JSONObject c_row = new JSONObject();
                            c_row.put("_id",(long)comp_node.get("id"));
                            c_row.put("name",cname);
                            c_row.put("order", order);
                            c_row.put("c_w",c_w);
                            ctx.put("comp", c_row);
                            ctx.put("region_alias", region_node.get("alias"));
                            Velocity.mergeTemplate("comp_week.vm", StandardCharsets.UTF_8.name(), ctx, menu);
                            JSONObject cwo = new JSONObject();
                            cwo.put("menu", menu.toString());
                            cwo.put("order", order);
                            comp_menu.add(cwo);
                            JSONObject cdata = new JSONObject();
                            cdata.put("_id",(long)comp_node.get("id"));
                            cdata.put("name", comp_node.get("name"));
                            cdata.put("c_w",c_w);
                            cdata.put("view", menu.toString());
                            cdata.put("games", gms);
                            cmps.put(cid, cdata);
                        }
                    }
                    if (r_w>0)
                    {
                        VelocityContext ctx = new VelocityContext();
                        StringWriter menu = new StringWriter();
                        String region_name = region_node.get("name").toString();
                        int region_order = 99;
                        Collections.sort(comp_menu, new Comparator<JSONObject>() {
                            @Override
                            public int compare(JSONObject o1, JSONObject o2) {
                                return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                            }
                        });
                        if (region_node.containsKey("alias"))
                        {
                            switch(region_node.get("alias").toString())
                            {
                                case "Europe": {
                                    region_name = "Europe";
                                    region_order = 1;
                                }; break;
                                case "World": {
                                    region_name = "Welt";
                                    region_order = 2;
                                }; break;
                                case "England": {
                                    region_name = "England";
                                    region_order = 3;
                                }; break;
                                case "Germany": {
                                    region_name = "Deutschland";
                                    region_order = 4;
                                }; break;
                                case "Austria": {
                                    region_name = "Österreich";
                                    region_order = 5;
                                }; break;
                                case "Spain": {
                                    region_name = "Spanien";
                                    region_order = 6;
                                }; break;
                                case "Italy": {
                                    region_name = "Italien";
                                    region_order = 7;
                                }; break;
                                case "France": {
                                    region_name = "Frankreich";
                                    region_order = 8;
                                }; break;
                                case "Turkey": {
                                    region_name = "Türkei";
                                    region_order = 9;
                                }; break;
                                case "Denmark": {
                                    region_name = "Danemark";
                                    region_order = 10;
                                }; break;
                                case "Netherlands": {
                                    region_name = "Niederlande";
                                    region_order = 11;
                                }; break;
                                case "Belgium": {
                                    region_name = "Belgien";
                                    region_order = 12;
                                }; break;
                                case "Portugal": {
                                    region_name = "Portugal";
                                    region_order = 13;
                                }; break;
                                case "Russia": {
                                    region_name = "Russland";
                                    region_order = 14;
                                }; break;
                                case "Greece": {
                                    region_name = "Griechenland";
                                    region_order = 15;
                                }; break;
                                case "Scotland": {
                                    region_name = "Schottland";
                                    region_order = 16;
                                }; break;
                                case "Czech Republic": {
                                    region_name = "Tschechien";
                                    region_order = 17;
                                }; break;
                                case "Poland": {
                                    region_name = "Polen";
                                    region_order = 18;
                                }; break;
                                case "Hungary": {
                                    region_name = "Ungarn";
                                    region_order = 19;
                                }; break;
                                case "Ukraine": {
                                    region_name = "Ukraine";
                                    region_order = 20;
                                }; break;
                                case "NorthernIreland": {
                                    region_name = "Nordirland";
                                    region_order = 21;
                                }; break;
                                case "Israel": {
                                    region_name = "Israel";
                                    region_order = 22;
                                }; break;
                                case "Wales": {
                                    region_name = "Wales";
                                    region_order = 23;
                                }; break;
                                case "Australia": {
                                    region_name = "Australien";
                                    region_order = 24;
                                }; break;
                                case "Ruanda": {
                                    region_name = "Ruanda";
                                    region_order = 25;
                                }; break;
                                case "Japan": {
                                    region_name = "Japan";
                                    region_order = 26;
                                }; break;
                                case "India": {
                                    region_name = "Indien";
                                    region_order = 27;
                                }; break;
                                case "Bangladesh": {
                                    region_name = "Bangladesh";
                                    region_order = 28;
                                }; break;
                                case "Jordan": {
                                    region_name = "Jordanien";
                                    region_order = 29;
                                }; break;
                                case "SaudiArabia": {
                                    region_name = "Saudi Arabien";
                                    region_order = 30;
                                }; break;
                                case "UAE": {
                                    region_name = "V.A. Emirate";
                                    region_order = 31;
                                }; break;
                                case "Algeria": {
                                    region_name = "Algerien";
                                    region_order = 32;
                                }; break;
                                case "Egypt": {
                                    region_name = "Ägypten";
                                    region_order = 33;
                                }; break;
                                default: {
                                    region_name = region_node.get("name").toString();
                                    region_order = 99;
                                }; break;
                            }
                        }
                        JSONObject r_row = new JSONObject();
                        r_row.put("_id", (long) region_node.get("id"));
                        r_row.put("name", region_name);
                        r_row.put("alias", region_node.get("alias"));
                        r_row.put("order", region_order);
                        r_row.put("r_w", r_w);
                        ctx.put("region", r_row);
                        ctx.put("comps", comp_menu);
                        ctx.put("sid", sid);
                        Velocity.mergeTemplate("region_week.vm", StandardCharsets.UTF_8.name(), ctx, menu);
                        JSONObject rdata = new JSONObject();
                        rdata.put("_id", (long) region_node.get("id"));
                        rdata.put("alias", region_node.get("alias"));
                        rdata.put("r_w", r_w);
                        rdata.put("comps", cmps);
                        rdata.put("view", menu.toString());
                        rdata.put("order", region_order);
                        rdata.put("name", region_name);
                        rgns.add(rdata);
                        regions.put(rid, rdata);
                    }
                }
                Collections.sort(rgns, new Comparator<JSONObject>() {
                    @Override
                    public int compare(JSONObject o1, JSONObject o2) {
                        return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                    }
                });
                String rg_menu = "";
                for (JSONObject rgn : rgns)
                {
                    rg_menu += rgn.get("view").toString();
                }
                VelocityContext ctx1 = new VelocityContext();
                StringWriter menu = new StringWriter();
                ctx1.put("id", (long)sport_node.get("id"));
                ctx1.put("alias", sport_node.get("alias"));
                ctx1.put("name", sport_node.get("name"));
                ctx1.put("regions", rg_menu);
                Velocity.mergeTemplate("sport_menu.vm", StandardCharsets.UTF_8.name(), ctx1, menu);
                VelocityContext ctx2 = new VelocityContext();
                StringWriter top = new StringWriter();
                ctx2.put("id", sid);
                ctx2.put("order", new Integer(Integer.parseInt(sport_node.get("order").toString())));
                ctx2.put("alias", (String)sport_node.get("alias")+"_top");
                ctx2.put("name", sport_node.get("name"));
                Velocity.mergeTemplate("sport_top.vm", StandardCharsets.UTF_8.name(), ctx2, top);
                JSONObject sdata = new JSONObject();
                sdata.put("_id", sid);
                sdata.put("alias", sport_node.get("alias"));
                sdata.put("name", sport_node.get("name"));
                sdata.put("menu",menu.toString());
                sdata.put("top",top.toString());
                sdata.put("order", new Integer(Integer.parseInt(sport_node.get("order").toString())));
                sdata.put("regions", regions);
                data.put(sid, sdata);
            }
            update = true;
            JSONObject get_info = new JSONObject();

            get_info.put("command","get");
            get_info.put("rid", new Integer(2));

            JSONObject params = new JSONObject();
            params.put("source", "betting");

            JSONObject what = new JSONObject();
            JSONArray empty = new JSONArray();
            JSONArray for_game = new JSONArray();
            for_game.add("@count");
            what.put("sport", empty);
            what.put("region", empty);
            what.put("competition", empty);
            what.put("game", for_game);

            params.put("what", what);

            Date today = new Date();

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
            where.put("game", game);

            params.put("where",where);
            params.put("subscribe", false);

            get_info.put("params", params);

            client.sendMessage(get_info.toString());

            this.latch.countDown();
            System.out.println("Menu has data. Counter:"+ latch.getCount());
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void form_data_update(Set<String> sids_u, JSONObject sport)
    {
        try {
            Map<String, JSONObject> udata = new ConcurrentHashMap<String, JSONObject>();
            for (String sid : sids_u)
            {
                JSONObject sport_node = new JSONObject();
                JSONObject region_node = new JSONObject();
                sport_node = (JSONObject)sport.get(sid);
                JSONObject region = new JSONObject();
                region = (JSONObject) sport_node.get("region");
                Set<String> region_ids = region.keySet();
                List<JSONObject> rgns = new ArrayList<JSONObject>();
                String comps = "";
                Map<String,JSONObject> regions = new HashMap<String,JSONObject>();
                for (String rid : region_ids)
                {
                    region_node = (JSONObject)region.get(rid);
                    JSONObject comp = new JSONObject();
                    comp = (JSONObject) region_node.get("competition");
                    Set<String> comp_ids = comp.keySet();
                    List<JSONObject> comp_menu = new ArrayList<JSONObject>();
                    int r_w = 0;
                    Map<String,JSONObject> cmps = new HashMap<String,JSONObject>();
                    for(String cid : comp_ids)
                    {
                        JSONObject comp_node = new JSONObject();
                        comp_node = (JSONObject)comp.get(cid);
                        JSONObject game = new JSONObject();
                        game = (JSONObject) comp_node.get("game");
                        Set<String> game_ids = game.keySet();
                        int c_w = game_ids.size();
                        Map<String,JSONObject> gms = new HashMap<String,JSONObject>();
                        for (String gid : game_ids)
                        {
                            gms.put(gid, new JSONObject());
                        }
                        r_w += c_w;
                        if (c_w>0)
                        {
                            String cname = ((String)comp_node.get("name")).replaceAll("^([^0-9-.]+)(([\\s\\-]+)([^0-9-.]+))?([\\s\\-\\.]+)([^0-9-.]+)(([\\s\\-]+)([^0-9-.]+)$)?([\\s\\-\\.]+)", "");
                            VelocityContext ctx = new VelocityContext();
                            StringWriter menu = new StringWriter();
                            Integer order = 9999;
                            if(comp_node.containsKey("order")) {
                                if (comp_node.get("order").getClass() != java.lang.Boolean.class) order = new Integer(Integer.parseInt(comp_node.get("order").toString()));
                            }
                            JSONObject c_row = new JSONObject();
                            c_row.put("_id",(long)comp_node.get("id"));
                            c_row.put("name",cname);
                            c_row.put("order", order);
                            c_row.put("c_w",c_w);
                            ctx.put("comp", c_row);
                            ctx.put("region_alias", region_node.get("alias"));
                            Velocity.mergeTemplate("comp_week.vm", StandardCharsets.UTF_8.name(), ctx, menu);
                            JSONObject cwo = new JSONObject();
                            cwo.put("menu", menu.toString());
                            cwo.put("order", order);
                            comp_menu.add(cwo);
                            JSONObject cdata = new JSONObject();
                            cdata.put("_id",(long)comp_node.get("id"));
                            cdata.put("name", comp_node.get("name"));
                            cdata.put("c_w",c_w);
                            cdata.put("view", menu.toString());
                            cdata.put("games", gms);
                            cmps.put(cid, cdata);
                        }
                    }
                    if (r_w>0)
                    {
                        VelocityContext ctx = new VelocityContext();
                        StringWriter menu = new StringWriter();
                        String region_name = region_node.get("name").toString();
                        int region_order = 99;
                        Collections.sort(comp_menu, new Comparator<JSONObject>() {
                            @Override
                            public int compare(JSONObject o1, JSONObject o2) {
                                return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                            }
                        });
                        if (region_node.containsKey("alias"))
                        {
                            switch(region_node.get("alias").toString())
                            {
                                case "Europe": {
                                    region_name = "Europe";
                                    region_order = 1;
                                }; break;
                                case "World": {
                                    region_name = "Welt";
                                    region_order = 2;
                                }; break;
                                case "England": {
                                    region_name = "England";
                                    region_order = 3;
                                }; break;
                                case "Germany": {
                                    region_name = "Deutschland";
                                    region_order = 4;
                                }; break;
                                case "Austria": {
                                    region_name = "Österreich";
                                    region_order = 5;
                                }; break;
                                case "Spain": {
                                    region_name = "Spanien";
                                    region_order = 6;
                                }; break;
                                case "Italy": {
                                    region_name = "Italien";
                                    region_order = 7;
                                }; break;
                                case "France": {
                                    region_name = "Frankreich";
                                    region_order = 8;
                                }; break;
                                case "Turkey": {
                                    region_name = "Türkei";
                                    region_order = 9;
                                }; break;
                                case "Denmark": {
                                    region_name = "Danemark";
                                    region_order = 10;
                                }; break;
                                case "Netherlands": {
                                    region_name = "Niederlande";
                                    region_order = 11;
                                }; break;
                                case "Belgium": {
                                    region_name = "Belgien";
                                    region_order = 12;
                                }; break;
                                case "Portugal": {
                                    region_name = "Portugal";
                                    region_order = 13;
                                }; break;
                                case "Russia": {
                                    region_name = "Russland";
                                    region_order = 14;
                                }; break;
                                case "Greece": {
                                    region_name = "Griechenland";
                                    region_order = 15;
                                }; break;
                                case "Scotland": {
                                    region_name = "Schottland";
                                    region_order = 16;
                                }; break;
                                case "Czech Republic": {
                                    region_name = "Tschechien";
                                    region_order = 17;
                                }; break;
                                case "Poland": {
                                    region_name = "Polen";
                                    region_order = 18;
                                }; break;
                                case "Hungary": {
                                    region_name = "Ungarn";
                                    region_order = 19;
                                }; break;
                                case "Ukraine": {
                                    region_name = "Ukraine";
                                    region_order = 20;
                                }; break;
                                case "NorthernIreland": {
                                    region_name = "Nordirland";
                                    region_order = 21;
                                }; break;
                                case "Israel": {
                                    region_name = "Israel";
                                    region_order = 22;
                                }; break;
                                case "Wales": {
                                    region_name = "Wales";
                                    region_order = 23;
                                }; break;
                                case "Australia": {
                                    region_name = "Australien";
                                    region_order = 24;
                                }; break;
                                case "Ruanda": {
                                    region_name = "Ruanda";
                                    region_order = 25;
                                }; break;
                                case "Japan": {
                                    region_name = "Japan";
                                    region_order = 26;
                                }; break;
                                case "India": {
                                    region_name = "Indien";
                                    region_order = 27;
                                }; break;
                                case "Bangladesh": {
                                    region_name = "Bangladesh";
                                    region_order = 28;
                                }; break;
                                case "Jordan": {
                                    region_name = "Jordanien";
                                    region_order = 29;
                                }; break;
                                case "SaudiArabia": {
                                    region_name = "Saudi Arabien";
                                    region_order = 30;
                                }; break;
                                case "UAE": {
                                    region_name = "V.A. Emirate";
                                    region_order = 31;
                                }; break;
                                case "Algeria": {
                                    region_name = "Algerien";
                                    region_order = 32;
                                }; break;
                                case "Egypt": {
                                    region_name = "Ägypten";
                                    region_order = 33;
                                }; break;
                                default: {
                                    region_name = region_node.get("name").toString();
                                    region_order = 99;
                                }; break;
                            }
                        }
                        JSONObject r_row = new JSONObject();
                        r_row.put("_id", (long) region_node.get("id"));
                        r_row.put("name", region_name);
                        r_row.put("alias", region_node.get("alias"));
                        r_row.put("order", region_order);
                        r_row.put("r_w", r_w);
                        ctx.put("region", r_row);
                        ctx.put("comps", comp_menu);
                        ctx.put("sid", sid);
                        Velocity.mergeTemplate("region_week.vm", StandardCharsets.UTF_8.name(), ctx, menu);
                        JSONObject rdata = new JSONObject();
                        rdata.put("_id", (long) region_node.get("id"));
                        rdata.put("alias", region_node.get("alias"));
                        rdata.put("r_w", r_w);
                        rdata.put("comps", cmps);
                        rdata.put("view", menu.toString());
                        rdata.put("order", region_order);
                        rdata.put("name", region_name);
                        rgns.add(rdata);
                        regions.put(rid, rdata);
                    }
                }
                Collections.sort(rgns, new Comparator<JSONObject>() {
                    @Override
                    public int compare(JSONObject o1, JSONObject o2) {
                        return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                    }
                });
                String rg_menu = "";
                for (JSONObject rgn : rgns)
                {
                    rg_menu += rgn.get("view").toString();
                }
                VelocityContext ctx1 = new VelocityContext();
                StringWriter menu = new StringWriter();
                ctx1.put("id", (long)sport_node.get("id"));
                ctx1.put("alias", sport_node.get("alias"));
                ctx1.put("name", sport_node.get("name"));
                ctx1.put("regions", rg_menu);
                Velocity.mergeTemplate("sport_menu.vm", StandardCharsets.UTF_8.name(), ctx1, menu);
                VelocityContext ctx2 = new VelocityContext();
                StringWriter top = new StringWriter();
                ctx2.put("id", sid);
                ctx2.put("order", new Integer(Integer.parseInt(sport_node.get("order").toString())));
                ctx2.put("alias", (String)sport_node.get("alias")+"_top");
                ctx2.put("name", sport_node.get("name"));
                Velocity.mergeTemplate("sport_top.vm", StandardCharsets.UTF_8.name(), ctx2, top);
                JSONObject sdata = new JSONObject();
                sdata.put("_id", sid);
                sdata.put("alias", sport_node.get("alias"));
                sdata.put("name", sport_node.get("name"));
                sdata.put("menu",menu.toString());
                sdata.put("top",top.toString());
                sdata.put("order", new Integer(Integer.parseInt(sport_node.get("order").toString())));
                sdata.put("regions", regions);
                udata.put(sid, sdata);
            }
            update_data(udata);
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void update_data(Map<String,JSONObject> udata)
    {
        try
        {
            del_data("sport",udata,this.data);
            for (String sid : udata.keySet())
            {
                if (this.data.containsKey(sid))
                {
                    Map<String, JSONObject> regions = (Map<String, JSONObject>)data.get(sid).get("regions");
                    Map<String, JSONObject> regions_u = (Map<String, JSONObject>)udata.get(sid).get("regions");
                    del_data("region",regions_u, regions);
                    for (String rid : regions_u.keySet())
                    {
                        if (regions.containsKey(rid))
                        {
                            Map<String, JSONObject> comps = (Map<String, JSONObject>)regions.get(rid).get("comps");
                            Map<String, JSONObject> comps_u = (Map<String, JSONObject>)regions_u.get(rid).get("comps");
                            del_data("comp",comps_u, comps);
                            if (!regions_u.get(rid).get("r_w").equals(regions.get(rid).get("r_w")))
                            {
                                JSONObject obj = new JSONObject();
                                obj.put("command", "update");
                                obj.put("what", "region");
                                obj.put("type", "menu");
                                obj.put("rid", rid);
                                obj.put("sid", sid);
                                obj.put("param", "regionCount");
                                obj.put("value", (int)regions_u.get(rid).get("r_w"));
                                update(obj);
                            }
                            for (String cid : comps_u.keySet())
                            {
                                if (comps.containsKey(cid))
                                {
                                    if (!comps_u.get(cid).get("c_w").equals(comps.get(cid).get("c_w")))
                                    {
                                        JSONObject obj = new JSONObject();
                                        obj.put("command", "update");
                                        obj.put("what", "comp");
                                        obj.put("type", "menu");
                                        obj.put("cid", cid);
                                        obj.put("param", "compCount");
                                        obj.put("value", (int)comps_u.get(cid).get("c_w"));
                                        update(obj);
                                    }
                                }
                                else
                                {
                                    JSONObject obj = new JSONObject();
                                    obj.put("command", "new");
                                    obj.put("what", "comp");
                                    obj.put("type", "menu");
                                    JSONObject new_data = new JSONObject();
                                    new_data.put("regionId", rid);
                                    new_data.put("view", comps_u.get(cid).get("view"));
                                    new_data.put("compCount", comps_u.get(cid).get("c_w"));
                                    obj.put("data", new_data);
                                    update(obj);
                                }
                            }
                        }
                        else
                        {
                            JSONObject obj = new JSONObject();
                            obj.put("command", "new");
                            obj.put("what", "region");
                            obj.put("type", "menu");
                            JSONObject new_data = new JSONObject();
                            new_data.put("sportId", sid);
                            new_data.put("view", regions_u.get(rid).get("view"));
                            obj.put("data", new_data);
                            update(obj);
                        }
                    }
                }
                else
                {
                    JSONObject obj = new JSONObject();
                    obj.put("command", "new");
                    obj.put("what", "sport");
                    obj.put("type", "menu");
                    obj.put("data", udata.get(sid));
                    update(obj);
                }
            }
            data = new ConcurrentHashMap<String,JSONObject>(udata);
            JSONObject get_info = new JSONObject();

            get_info.put("command","get");
            get_info.put("rid", new Integer(2));

            JSONObject params = new JSONObject();
            params.put("source", "betting");

            JSONObject what = new JSONObject();
            JSONArray empty = new JSONArray();
            JSONArray for_game = new JSONArray();
            for_game.add("@count");
            what.put("sport", empty);
            what.put("region", empty);
            what.put("competition", empty);
            what.put("game", for_game);

            params.put("what", what);

            Date today = new Date();

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
            where.put("game", game);

            params.put("where",where);
            params.put("subscribe", false);

            get_info.put("params", params);

            client.sendMessage(get_info.toString());

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    void update(JSONObject update)
    {
        try {
            ConcurrentHashMap<String,Terminal> terminals = (ConcurrentHashMap<String,Terminal>)main.terminals;
            for(String tid : terminals.keySet())
            {
                Terminal t = terminals.get(tid);
                if (t != null) {
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

    public void del_data(String what, Map<String,JSONObject> map1,Map<String,JSONObject> map2)
    {
        try {
            Map<String,JSONObject> mmap1 = new HashMap<String,JSONObject> (map1);
            Map<String,JSONObject> mmap2 = new HashMap<String,JSONObject> (map2);
            Set<String> keys1 = mmap1.keySet();
            Set<String> keys2 = mmap2.keySet();
            keys2.removeAll(keys1);
            for (String key: keys2)
            {
                JSONObject del = new JSONObject();
                del.put("command", "delete");
                del.put("what",what);
                del.put("type","menu");
                del.put("id", key);
                update(del);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}