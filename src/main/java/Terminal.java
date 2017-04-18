import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.eclipse.jetty.websocket.api.Session;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.StringWriter;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by Administrator on 18.04.2017.
 */
public class Terminal extends Thread {

    private String id;
    public String game_id;
    private String multi;
    public List<String> betslip = new ArrayList<>();
    private CountDownLatch latch;
    private Session s;
    public Map<String,JSONObject> updates = new ConcurrentHashMap<>();

    Terminal (String tid, ArrayList<String> bets, String gid, String m, Session session) {
        this.id = tid;
        this.betslip = bets;
        this.game_id = gid;
        this.multi = m;
        this.latch = new CountDownLatch(4);
        this.s = session;
    }
     public long getCount () {
        return  this.latch.getCount();
     }

    public synchronized void setCount (int n) {
        this.latch = new CountDownLatch(n);
    }

    public synchronized Session getSession () {
        return this.s;
    }

    public synchronized String getMulti () {
        return this.multi;
    }

    public void run(){
        try {
            build_live();
            build_flive();
            build_favorite();
            build_menu();
            latch.await();
            for (String uid : updates.keySet()) {
                if (s.isOpen()) sendIt(updates.get(uid));
            }
            updates.clear();
            while(!isInterrupted()) {}
        }
        catch (Exception e) {
            main.errorLogger.error("Error happened", e);
        }
        finally {
            System.out.println("Terminal " +  this.id + " stopped");
        }
    }

    public void build_day(JSONObject data)
    {
        try
        {
            JSONObject obj = new JSONObject();
            obj.put("command", "get_day");
            JSONObject sport = (JSONObject)data.get("sport");
            Set<String> sport_ids = sport.keySet();
            List<JSONObject> games = new ArrayList<JSONObject>();
            for (String sid : sport_ids)
            {
                JSONObject sport_node = new JSONObject();
                JSONObject region_node = new JSONObject();
                sport_node = (JSONObject)sport.get(sid);

                JSONObject region = new JSONObject();
                region = (JSONObject) sport_node.get("region");
                Set<String> region_ids = region.keySet();
                for (String rid : region_ids)
                {
                    region_node = (JSONObject)region.get(rid);
                    JSONObject comp = new JSONObject();
                    comp = (JSONObject) region_node.get("competition");
                    Set<String> comp_ids = comp.keySet();
                    for(String cid : comp_ids)
                    {
                        JSONObject comp_node = new JSONObject();
                        comp_node = (JSONObject)comp.get(cid);
                        JSONObject game = new JSONObject();
                        game = (JSONObject) comp_node.get("game");
                        Set<String> game_ids = game.keySet();
                        List<String> gvl = new ArrayList<String>();
                        int c_l = 0;
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
                            if (game_node.containsKey("game_external_id")) add.put("external-id", game_node.get("game_external_id").toString());
                            if (game_node.get("game_number").getClass() != java.lang.Boolean.class) add.put("alias", game_node.get("game_number").toString());
                            else add.put("alias", gid);
                            if (game_node.containsKey("team2_name")) {
                                add.put("team1",game_node.get("team1_name"));
                                add.put("team2",game_node.get("team2_name"));
                            }
                            else add.put("team1",game_node.get("team1_name"));
                            if (game_node.containsKey("markets_count")) add.put("mc", new Integer(Integer.parseInt(game_node.get("markets_count").toString())));
                            if (game_node.containsKey("exclude_ids")) {
                                if (!game_node.get("exclude_ids").getClass().isArray()) add.put("excl_id", game_node.get("exclude_ids").toString());
                                else add.put("excl_id", game_node.get("exclude_ids"));
                            }
                            JSONObject market = new JSONObject();
                            market = (JSONObject)game_node.get("market");
                            Set<String> market_ids = market.keySet();
                            List<JSONObject> gr1 = new ArrayList<JSONObject>();
                            List<JSONObject> gr2 = new ArrayList<JSONObject>();
                            List<JSONObject> gr3 = new ArrayList<JSONObject>();
                            List<JSONObject> gr4 = new ArrayList<JSONObject>();
                            List<JSONObject> total = new ArrayList<JSONObject>();
                            for (String mid : market_ids)
                            {
                                JSONObject market_node= new JSONObject ();
                                market_node = (JSONObject) market.get(mid);
                                JSONObject nme = new JSONObject();
                                nme.put("_id",mid);
                                nme.put("gid",gid);
                                if (market_node.containsKey("express_id")) nme.put("exp_id", market_node.get("express_id"));

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
                                }
                                else if (main.gerMarkets.containsKey(mType + " " + mBase)){
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

                                JSONObject event = new JSONObject();
                                event = (JSONObject)market_node.get("event");
                                Set<String> event_ids = event.keySet();
                                List<JSONObject> evnts = new ArrayList<JSONObject> ();
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
                                    if (event_node.containsKey("price")) ne.put("price",event_node.get("price").toString());
                                    else ne.put("price", "1.01");
                                    String price = ne.get("price").toString();
                                    Double currCoef = Double.parseDouble(price);
                                    Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
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
                                    ne.put("price", df.format(multiPrice).toString().replaceAll(",", "."));
                                    evnts.add(ne);
                                }
                                Collections.sort(evnts, new Comparator<JSONObject>() {
                                    @Override
                                    public int compare(JSONObject o1, JSONObject o2) {
                                        return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                                    }
                                });
                                nme.put("events",evnts);
                                VelocityContext mv = new VelocityContext();
                                StringWriter mvr = new StringWriter();
                                mv.put("market", nme);
                                Velocity.mergeTemplate("market.vm", StandardCharsets.UTF_8.name(), mv, mvr);
                                nme.put("view", mvr.toString());
                                if (market_node.containsKey("type"))
                                {
                                    if (((String)market_node.get("type")).equals("P1XP2") || ((String)market_node.get("type")).equals("P1P2"))
                                    {
                                        gr1.add(nme);
                                    }
                                    if (((String)market_node.get("type")).equals("1X12X2")) gr2.add(nme);
                                    if (((String)market_node.get("type")).equals("1HalfP1XP2")
                                            || ((String)market_node.get("type")).equals("1SetP1XP2")
                                            || ((String)market_node.get("type")).equals("1PeriodP1XP2")) gr3.add(nme);
                                    if(((String)market_node.get("type")).equals("Total")) total.add(nme);
                                }
                            }
                            Collections.sort(total, new Comparator<JSONObject>() {
                                @Override
                                public int compare(JSONObject o1, JSONObject o2) {
                                    return new Double(Double.parseDouble(o1.get("base").toString())).compareTo(new Double(Double.parseDouble(o2.get("base").toString())));
                                }
                            });
                            if (gr1.size()>1)
                            {
                                if (gr1.get(0).get("type").toString().equals("P1XP2"))
                                    gr1.remove(gr1.get(1));
                                else if (gr1.get(1).get("type").toString().equals("P1XP2"))
                                    gr1.remove(gr1.get(0));
                            }
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
                            }
                            else if (total.size()>0)
                            {
                                gr4.add(total.get(0));
                            }

                            VelocityContext game_row = new VelocityContext();
                            StringWriter row = new StringWriter();
                            Date gs = new Date();
                            gs.setTime(Long.parseLong(add.get("start").toString())*1000);
                            SimpleDateFormat dt1 = new SimpleDateFormat("dd/MM");
                            SimpleDateFormat dt2 = new SimpleDateFormat("HH:mm");
                            game_row.put("game", add);
                            game_row.put("talias", "2");
                            game_row.put("day", dt1.format(gs).toString());
                            game_row.put("hour", dt2.format(gs).toString());
                            JSONObject sp_row = new JSONObject();
                            sp_row.put("id",sid);
                            sp_row.put("name", sport_node.get("name"));
                            sp_row.put("alias",sport_node.get("alias"));
                            game_row.put("sport", sp_row);
                            JSONObject rcid = new JSONObject();
                            rcid.put("rid", rid);
                            rcid.put("cid",cid);
                            game_row.put("rc", rcid);
                            game_row.put("region_alias", region_node.get("alias"));
                            game_row.put("comp_name", (String)comp_node.get("name"));
                            game_row.put("gr1", gr1);
                            game_row.put("gr2", gr2);
                            game_row.put("gr3", gr3);
                            game_row.put("gr4", gr4);
                            Velocity.mergeTemplate("game_row.vm", StandardCharsets.UTF_8.name(), game_row, row);
                            JSONObject gm = new JSONObject();
                            gm.put("id", game_node.get("id"));
                            gm.put("start", add.get("start"));
                            gm.put("alias", add.get("alias"));
                            gm.put("view", row.toString());
                            games.add(gm);
                        }
                    }
                }
            }
            Collections.sort(games, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    Long start1 = Long.parseLong(o1.get("start").toString());
                    Long start2 = Long.parseLong(o2.get("start").toString());
                    if (!start1.equals(start2))
                    {	return start1.compareTo(start2); }
                    else
                    {	return (new Long(Long.parseLong(o1.get("alias").toString()))).compareTo(new Long(Long.parseLong(o2.get("alias").toString()))); }
                }
            });
            String html = "";
            for (JSONObject game : games)
            {
                html += game.get("view").toString();
            }
            obj.put("data", html);
            if(s.isOpen()) sendIt(obj);
            this.latch.countDown();
            for (String uid : updates.keySet()) {
                if (s.isOpen()) sendIt(updates.get(uid));
            }
            updates.clear();
        }
        catch (Exception e)
        {
            main.errorLogger.error("Error happened", e);
        }
    }

    void build_region(JSONObject data, String comm)
    {
        try
        {
            JSONObject obj = new JSONObject();
            obj.put("command", comm);
            JSONParser parser = new JSONParser();
            JSONObject sport = (JSONObject)data.get("sport");
            Set<String> sport_ids = sport.keySet();
            Properties p = new Properties();
            p.setProperty("file.resource.loader.path", "./src/templates");
            Velocity.init(p);
            List<JSONObject> comps = new ArrayList<JSONObject>();
            for (String sid : sport_ids)
            {
                JSONObject sport_node = new JSONObject();
                JSONObject region_node = new JSONObject();
                sport_node = (JSONObject)sport.get(sid);
                JSONObject region = new JSONObject();
                region = (JSONObject) sport_node.get("region");
                Set<String> region_ids = region.keySet();
                for (String rid : region_ids)
                {
                    region_node = (JSONObject)region.get(rid);
                    JSONObject comp = new JSONObject();
                    comp = (JSONObject) region_node.get("competition");
                    Set<String> comp_ids = comp.keySet();
                    for(String cid : comp_ids)
                    {
                        JSONObject comp_node = new JSONObject();
                        comp_node = (JSONObject)comp.get(cid);
                        JSONObject game = new JSONObject();
                        game = (JSONObject) comp_node.get("game");
                        Set<String> game_ids = game.keySet();
                        List<String> gvl = new ArrayList<String>();
                        List<JSONObject> games = new ArrayList<JSONObject>();
                        int c_l = 0;
                        String html = "";
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
                            if (game_node.containsKey("game_external_id")) add.put("external-id", game_node.get("game_external_id").toString());
                            if (game_node.get("game_number").getClass() != java.lang.Boolean.class) add.put("alias", game_node.get("game_number").toString());
                            else add.put("alias", gid);
                            if (game_node.containsKey("team2_name")) {
                                add.put("team1",game_node.get("team1_name"));
                                add.put("team2",game_node.get("team2_name"));
                            }
                            else add.put("team1",game_node.get("team1_name"));
                            if (game_node.containsKey("markets_count")) add.put("mc", new Integer(Integer.parseInt(game_node.get("markets_count").toString())));
                            if (game_node.containsKey("exclude_ids")) {
                                if (!game_node.get("exclude_ids").getClass().isArray()) add.put("excl_id", game_node.get("exclude_ids").toString());
                                else add.put("excl_id", game_node.get("exclude_ids"));
                            }
                            JSONObject market = new JSONObject();
                            market = (JSONObject)game_node.get("market");
                            Set<String> market_ids = market.keySet();
                            List<JSONObject> gr1 = new ArrayList<JSONObject>();
                            List<JSONObject> gr2 = new ArrayList<JSONObject>();
                            List<JSONObject> gr3 = new ArrayList<JSONObject>();
                            List<JSONObject> gr4 = new ArrayList<JSONObject>();
                            List<JSONObject> total = new ArrayList<JSONObject>();
                            for (String mid : market_ids)
                            {
                                JSONObject market_node= new JSONObject ();
                                market_node = (JSONObject) market.get(mid);
                                JSONObject nme = new JSONObject();
                                nme.put("_id",mid);
                                nme.put("gid",gid);
                                if (market_node.containsKey("express_id")) nme.put("exp_id", market_node.get("express_id"));

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
                                }
                                else if (main.gerMarkets.containsKey(mType + " " + mBase)){
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

                                JSONObject event = new JSONObject();
                                event = (JSONObject)market_node.get("event");
                                Set<String> event_ids = event.keySet();
                                List<JSONObject> evnts = new ArrayList<JSONObject> ();
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
                                    if (event_node.containsKey("price")) ne.put("price",event_node.get("price").toString());
                                    else ne.put("price", "1.01");
                                    String price = ne.get("price").toString();
                                    Double currCoef = Double.parseDouble(price);
                                    Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
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
                                    ne.put("price", df.format(multiPrice).toString().replaceAll(",", "."));
                                    evnts.add(ne);
                                }
                                Collections.sort(evnts, new Comparator<JSONObject>() {
                                    @Override
                                    public int compare(JSONObject o1, JSONObject o2) {
                                        return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                                    }
                                });
                                nme.put("events",evnts);
                                VelocityContext mv = new VelocityContext();
                                StringWriter mvr = new StringWriter();
                                mv.put("market", nme);
                                Velocity.mergeTemplate("market.vm", StandardCharsets.UTF_8.name(), mv, mvr);
                                nme.put("view", mvr.toString());
                                if (market_node.containsKey("type"))
                                {
                                    if (((String)market_node.get("type")).equals("P1XP2") || ((String)market_node.get("type")).equals("P1P2"))
                                    {
                                        gr1.add(nme);
                                    }
                                    if (((String)market_node.get("type")).equals("1X12X2")) gr2.add(nme);
                                    if (((String)market_node.get("type")).equals("1HalfP1XP2")
                                            || ((String)market_node.get("type")).equals("1SetP1XP2")
                                            || ((String)market_node.get("type")).equals("1PeriodP1XP2")) gr3.add(nme);
                                    if(((String)market_node.get("type")).equals("Total")) total.add(nme);
                                }
                            }
                            Collections.sort(total, new Comparator<JSONObject>() {
                                @Override
                                public int compare(JSONObject o1, JSONObject o2) {
                                    return new Double(Double.parseDouble(o1.get("base").toString())).compareTo(new Double(Double.parseDouble(o2.get("base").toString())));
                                }
                            });
                            if (gr1.size()>1)
                            {
                                if (gr1.get(0).get("type").toString().equals("P1XP2"))
                                    gr1.remove(gr1.get(1));
                                else if (gr1.get(1).get("type").toString().equals("P1XP2"))
                                    gr1.remove(gr1.get(0));
                            }
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
                            }
                            else if (total.size()>0)
                            {
                                gr4.add(total.get(0));
                            }
                            VelocityContext game_row = new VelocityContext();
                            StringWriter row = new StringWriter();
                            Date gs = new Date();
                            gs.setTime(Long.parseLong(add.get("start").toString())*1000);
                            SimpleDateFormat dt1 = new SimpleDateFormat("dd/MM");
                            SimpleDateFormat dt2 = new SimpleDateFormat("HH:mm");
                            game_row.put("game", add);
                            game_row.put("talias", "2");
                            game_row.put("day", dt1.format(gs).toString());
                            game_row.put("hour", dt2.format(gs).toString());
                            JSONObject sp_row = new JSONObject();
                            sp_row.put("id",sid);
                            sp_row.put("name", sport_node.get("name"));
                            sp_row.put("alias",sport_node.get("alias"));
                            game_row.put("sport", sp_row);
                            JSONObject rcid = new JSONObject();
                            rcid.put("rid", rid);
                            rcid.put("cid",cid);
                            game_row.put("rc", rcid);
                            game_row.put("region_alias", region_node.get("alias"));
                            game_row.put("comp_name", (String)comp_node.get("name"));
                            game_row.put("gr1", gr1);
                            game_row.put("gr2", gr2);
                            game_row.put("gr3", gr3);
                            game_row.put("gr4", gr4);
                            Velocity.mergeTemplate("game_row.vm", StandardCharsets.UTF_8.name(), game_row, row);
                            JSONObject gm = new JSONObject();
                            gm.put("id", game_node.get("id"));
                            gm.put("start", add.get("start"));
                            gm.put("alias", add.get("alias"));
                            gm.put("view", row.toString());
                            games.add(gm);
                        }
                        Collections.sort(games, new Comparator<JSONObject>() {
                            @Override
                            public int compare(JSONObject o1, JSONObject o2) {
                                Long start1 = Long.parseLong(o1.get("start").toString());
                                Long start2 = Long.parseLong(o2.get("start").toString());
                                if (!start1.equals(start2))
                                {	return start1.compareTo(start2); }
                                else
                                {	return (new Long(Long.parseLong(o1.get("alias").toString()))).compareTo(new Long(Long.parseLong(o2.get("alias").toString()))); }
                            }
                        });
                        for (JSONObject game_obj : games)
                        {
                            gvl.add(game_obj.get("view").toString());
                        }
                        VelocityContext fl = new VelocityContext();
                        StringWriter f1 = new StringWriter();
                        String cname = ((String)comp_node.get("name")).replaceAll("^([^0-9-.]+)(([\\s\\-]+)([^0-9-.]+))?([\\s\\-\\.]+)([^0-9-.]+)(([\\s\\-]+)([^0-9-.]+)$)?([\\s\\-\\.]+)", "");
                        fl.put("ralias", region_node.get("alias"));
                        fl.put("name", cname);
                        fl.put("id", cid);
                        fl.put("descr","week");
                        fl.put("games", gvl);
                        Velocity.mergeTemplate("comp_filter.vm", StandardCharsets.UTF_8.name(), fl, f1);
                        html += f1.toString();
                        JSONObject cdata = new JSONObject();
                        cdata.put("id", cid);
                        cdata.put("view", f1.toString());
                        comps.add(cdata);
                    }
                    obj.put("data", comps);
                    obj.put("sid",sid);
                    obj.put("regionId", rid);
                    obj.put("sportAlias", sport_node.get("alias"));
                    if(s.isOpen()) sendIt(obj);
                    this.latch.countDown();
                    for (String uid : updates.keySet()) {
                        if (s.isOpen()) sendIt(updates.get(uid));
                    }
                    updates.clear();
                }
            }
        }
        catch (Exception e)
        {
            main.errorLogger.error("Error happened",e);
        }
    }

    void build_comp(JSONObject data, String comm)
    {
        try
        {
            JSONObject obj = new JSONObject();
            obj.put("command", comm);
            JSONParser parser = new JSONParser();
            JSONObject sport = (JSONObject)data.get("sport");
            Set<String> sport_ids = sport.keySet();
            Properties p = new Properties();
            p.setProperty("file.resource.loader.path", "./src/templates");
            List<JSONObject> games = new ArrayList<JSONObject>();
            Velocity.init(p);
            List<JSONObject> comps = new ArrayList<JSONObject>();
            for (String sid : sport_ids)
            {
                JSONObject sport_node = new JSONObject();
                JSONObject region_node = new JSONObject();
                sport_node = (JSONObject)sport.get(sid);
                JSONObject region = new JSONObject();
                region = (JSONObject) sport_node.get("region");
                Set<String> region_ids = region.keySet();
                for (String rid : region_ids)
                {
                    region_node = (JSONObject)region.get(rid);
                    JSONObject comp = new JSONObject();
                    comp = (JSONObject) region_node.get("competition");
                    Set<String> comp_ids = comp.keySet();
                    String html = "";
                    for(String cid : comp_ids)
                    {
                        JSONObject comp_node = new JSONObject();
                        comp_node = (JSONObject)comp.get(cid);
                        JSONObject game = new JSONObject();
                        game = (JSONObject) comp_node.get("game");
                        Set<String> game_ids = game.keySet();
                        List<String> gvl = new ArrayList<String>();
                        int c_l = 0;
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
                            if (game_node.containsKey("game_external_id")) add.put("external-id", game_node.get("game_external_id").toString());
                            if (game_node.get("game_number").getClass() != java.lang.Boolean.class) add.put("alias", game_node.get("game_number").toString());
                            else add.put("alias", gid);
                            if (game_node.containsKey("team2_name")) {
                                add.put("team1",game_node.get("team1_name"));
                                add.put("team2",game_node.get("team2_name"));
                            }
                            else add.put("team1",game_node.get("team1_name"));
                            if (game_node.containsKey("markets_count")) add.put("mc", new Integer(Integer.parseInt(game_node.get("markets_count").toString())));
                            if (game_node.containsKey("exclude_ids")) {
                                if (!game_node.get("exclude_ids").getClass().isArray()) add.put("excl_id", game_node.get("exclude_ids").toString());
                                else add.put("excl_id", game_node.get("exclude_ids"));
                            }
                            JSONObject market = new JSONObject();
                            market = (JSONObject)game_node.get("market");
                            Set<String> market_ids = market.keySet();
                            List<JSONObject> gr1 = new ArrayList<JSONObject>();
                            List<JSONObject> gr2 = new ArrayList<JSONObject>();
                            List<JSONObject> gr3 = new ArrayList<JSONObject>();
                            List<JSONObject> gr4 = new ArrayList<JSONObject>();
                            List<JSONObject> total = new ArrayList<JSONObject>();
                            for (String mid : market_ids)
                            {
                                JSONObject market_node= new JSONObject ();
                                market_node = (JSONObject) market.get(mid);
                                JSONObject nme = new JSONObject();
                                nme.put("_id",mid);
                                nme.put("gid",gid);
                                if (market_node.containsKey("express_id")) nme.put("exp_id", market_node.get("express_id"));

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
                                }
                                else if (main.gerMarkets.containsKey(mType + " " + mBase)){
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

                                JSONObject event = new JSONObject();
                                event = (JSONObject)market_node.get("event");
                                Set<String> event_ids = event.keySet();
                                List<JSONObject> evnts = new ArrayList<JSONObject> ();
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
                                    if (event_node.containsKey("price")) ne.put("price",event_node.get("price").toString());
                                    else ne.put("price", "1.01");
                                    String price = ne.get("price").toString();
                                    Double currCoef = Double.parseDouble(price);
                                    Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
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
                                    ne.put("price", df.format(multiPrice).toString().replaceAll(",", "."));
                                    evnts.add(ne);
                                }
                                Collections.sort(evnts, new Comparator<JSONObject>() {
                                    @Override
                                    public int compare(JSONObject o1, JSONObject o2) {
                                        return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                                    }
                                });
                                nme.put("events",evnts);
                                VelocityContext mv = new VelocityContext();
                                StringWriter mvr = new StringWriter();
                                mv.put("market", nme);
                                Velocity.mergeTemplate("market.vm", StandardCharsets.UTF_8.name(), mv, mvr);
                                nme.put("view", mvr.toString());
                                if (market_node.containsKey("type"))
                                {
                                    if (((String)market_node.get("type")).equals("P1XP2") || ((String)market_node.get("type")).equals("P1P2"))
                                    {
                                        gr1.add(nme);
                                    }
                                    if (((String)market_node.get("type")).equals("1X12X2")) gr2.add(nme);
                                    if (((String)market_node.get("type")).equals("1HalfP1XP2")
                                            || ((String)market_node.get("type")).equals("1SetP1XP2")
                                            || ((String)market_node.get("type")).equals("1PeriodP1XP2")) gr3.add(nme);
                                    if(((String)market_node.get("type")).equals("Total")) total.add(nme);
                                }
                            }
                            Collections.sort(total, new Comparator<JSONObject>() {
                                @Override
                                public int compare(JSONObject o1, JSONObject o2) {
                                    return new Double(Double.parseDouble(o1.get("base").toString())).compareTo(new Double(Double.parseDouble(o2.get("base").toString())));
                                }
                            });
                            if (gr1.size()>1)
                            {
                                if (gr1.get(0).get("type").toString().equals("P1XP2"))
                                    gr1.remove(gr1.get(1));
                                else if (gr1.get(1).get("type").toString().equals("P1XP2"))
                                    gr1.remove(gr1.get(0));
                            }
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
                            }
                            else if (total.size()>0)
                            {
                                gr4.add(total.get(0));
                            }
                            VelocityContext game_row = new VelocityContext();
                            StringWriter row = new StringWriter();
                            Date gs = new Date();
                            gs.setTime(Long.parseLong(add.get("start").toString())*1000);
                            SimpleDateFormat dt1 = new SimpleDateFormat("dd/MM");
                            SimpleDateFormat dt2 = new SimpleDateFormat("HH:mm");
                            game_row.put("game", add);
                            game_row.put("talias", "2");
                            game_row.put("day", dt1.format(gs).toString());
                            game_row.put("hour", dt2.format(gs).toString());
                            JSONObject sp_row = new JSONObject();
                            sp_row.put("id",sid);
                            sp_row.put("name", sport_node.get("name"));
                            sp_row.put("alias",sport_node.get("alias"));
                            game_row.put("sport", sp_row);
                            JSONObject rcid = new JSONObject();
                            rcid.put("rid", rid);
                            rcid.put("cid",cid);
                            game_row.put("rc", rcid);
                            game_row.put("region_alias", region_node.get("alias"));
                            game_row.put("comp_name", (String)comp_node.get("name"));
                            game_row.put("gr1", gr1);
                            game_row.put("gr2", gr2);
                            game_row.put("gr3", gr3);
                            game_row.put("gr4", gr4);
                            Velocity.mergeTemplate("game_row.vm", StandardCharsets.UTF_8.name(), game_row, row);
                            JSONObject gm = new JSONObject();
                            gm.put("id", game_node.get("id"));
                            gm.put("start", add.get("start"));
                            gm.put("alias", add.get("alias"));
                            gm.put("view", row.toString());
                            games.add(gm);
                        }
                        Collections.sort(games, new Comparator<JSONObject>() {
                            @Override
                            public int compare(JSONObject o1, JSONObject o2) {
                                Long start1 = Long.parseLong(o1.get("start").toString());
                                Long start2 = Long.parseLong(o2.get("start").toString());
                                if (!start1.equals(start2))
                                {	return start1.compareTo(start2); }
                                else
                                {	return (new Long(Long.parseLong(o1.get("alias").toString()))).compareTo(new Long(Long.parseLong(o2.get("alias").toString()))); }
                            }
                        });
                        for (JSONObject game_obj : games)
                        {
                            gvl.add(game_obj.get("view").toString());
                        }
                        VelocityContext fl = new VelocityContext();
                        StringWriter f1 = new StringWriter();
                        String cname = ((String)comp_node.get("name")).replaceAll("^([^0-9-.]+)(([\\s\\-]+)([^0-9-.]+))?([\\s\\-\\.]+)([^0-9-.]+)(([\\s\\-]+)([^0-9-.]+)$)?([\\s\\-\\.]+)", "");
                        fl.put("ralias", region_node.get("alias"));
                        fl.put("name", cname);
                        fl.put("id", cid);
                        fl.put("descr","week");
                        fl.put("games", gvl);
                        Velocity.mergeTemplate("comp_filter.vm", StandardCharsets.UTF_8.name(), fl, f1);
                        html += f1.toString();
                        JSONObject cdata = new JSONObject();
                        cdata.put("id", cid);
                        cdata.put("view", f1.toString());
                        comps.add(cdata);
                        if (comm.equals("get_comp")) {
                            obj.put("compId", cid);
                            obj.put("sid",sid);
                            obj.put("data", html);
                            obj.put("sportAlias", sport_node.get("alias"));
                            if(s.isOpen()) sendIt(obj);
                            this.latch.countDown();
                            for (String uid : updates.keySet()) {
                                if (s.isOpen()) sendIt(updates.get(uid));
                            }
                            updates.clear();
                        }
                    }
                    if (comm.equals("get_region")) obj.put("regionId", rid);
                }
                if (comm.equals("get_region")) {
                    obj.put("data", comps);
                    obj.put("sid",sid);

                    obj.put("sportAlias", sport_node.get("alias"));
                    if(s.isOpen()) sendIt(obj);
                    this.latch.countDown();
                    for (String uid : updates.keySet()) {
                        if (s.isOpen()) sendIt(updates.get(uid));
                    }
                    updates.clear();
                }
            }
        }
        catch (Exception e)
        {
            main.errorLogger.error("Error happened",e);
        }
    }


    void build_search (JSONObject data, String comm)
    {
        try
        {
            JSONObject obj = new JSONObject();
            obj.put("command", comm);
            JSONObject new_data = new JSONObject();
            JSONParser parser = new JSONParser();
            JSONObject sport = (JSONObject)data.get("sport");
            Set<String> sport_ids = sport.keySet();
            Properties p = new Properties();
            p.setProperty("file.resource.loader.path", "./src/templates");
            Velocity.init(p);
            List<JSONObject> comps = new ArrayList<JSONObject>();
            if (sport_ids.size()>0)
            {
                for (String sid : sport_ids)
                {
                    JSONObject sport_node = new JSONObject();
                    JSONObject region_node = new JSONObject();
                    sport_node = (JSONObject)sport.get(sid);
                    new_data.put("sAlias", sport_node.get("alias").toString());
                    new_data.put("sName", sport_node.get("name").toString());
                    JSONObject region = new JSONObject();
                    region = (JSONObject) sport_node.get("region");
                    Set<String> region_ids = region.keySet();
                    for (String rid : region_ids)
                    {
                        region_node = (JSONObject)region.get(rid);
                        JSONObject comp = new JSONObject();
                        comp = (JSONObject) region_node.get("competition");
                        Set<String> comp_ids = comp.keySet();
                        for(String cid : comp_ids)
                        {
                            JSONObject comp_node = new JSONObject();
                            comp_node = (JSONObject)comp.get(cid);
                            JSONObject game = new JSONObject();
                            game = (JSONObject) comp_node.get("game");
                            Set<String> game_ids = game.keySet();
                            for (String gid : game_ids)
                            {
                                JSONObject game_node = new JSONObject();
                                game_node = (JSONObject)game.get(gid);
                                JSONObject add = new JSONObject();
                                add.put("_id", Long.parseLong(gid));
                                add.put("cid", Long.parseLong(cid));
                                add.put("sid", Integer.parseInt(sid));
                                add.put("start", game_node.get("start_ts").toString());
                                add.put("type", new Integer(Integer.parseInt(game_node.get("type").toString())));
                                add.put("alias", new Integer(Integer.parseInt(game_node.get("game_number").toString())));
                                if (game_node.containsKey("game_external_id")) add.put("external-id", game_node.get("game_external_id").toString());
                                if (game_node.containsKey("team2_name")){
                                    add.put("team1",game_node.get("team1_name"));
                                    add.put("team2",game_node.get("team2_name"));
                                }
                                else add.put("team1",game_node.get("team1_name"));
                                if (game_node.containsKey("markets_count")) add.put("mc", new Integer(Integer.parseInt(game_node.get("markets_count").toString())));
                                String st = "";
                                String tm = "";
                                if (game_node.containsKey("info"))
                                {
                                    JSONObject info = new JSONObject();
                                    info = (JSONObject)game_node.get("info");
                                    if (info.containsKey("current_game_state"))
                                    {
                                        String state = (String)info.get("current_game_state");
                                        add.put("state", (String)info.get("current_game_state"));
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
                                    else add.put("true_time", tm);
                                    if (info.containsKey("score1")) add.put("score1",Integer.parseInt((String)info.get("score1")));
                                    if (info.containsKey("score2")) add.put("score2",Integer.parseInt((String)info.get("score2")));
                                }
                                if (game_node.containsKey("live_events"))
                                {
                                    JSONArray live_events = (JSONArray)game_node.get("live_events");
                                    int rt1 = 0, rt2 = 0, yt1 = 0, yt2 = 0;
                                    for (int i = 0; i<live_events.size(); i++)
                                    {
                                        JSONObject le = (JSONObject)live_events.get(i);
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
                                }
                                if (game_node.containsKey("exclude_ids")) {
                                    if (!game_node.get("exclude_ids").getClass().isArray()) add.put("excl_id", game_node.get("exclude_ids").toString());
                                    else add.put("excl_id", game_node.get("exclude_ids"));
                                }
                                JSONObject market = new JSONObject();
                                market = (JSONObject)game_node.get("market");
                                Set<String> market_ids = market.keySet();
                                List<JSONObject> gr1 = new ArrayList<JSONObject>();
                                List<JSONObject> gr2 = new ArrayList<JSONObject>();
                                List<JSONObject> gr3 = new ArrayList<JSONObject>();
                                List<JSONObject> gr4 = new ArrayList<JSONObject>();
                                List<JSONObject> total = new ArrayList<JSONObject>();
                                List<JSONObject> total2 = new ArrayList<JSONObject>();
                                for (String mid : market_ids)
                                {
                                    JSONObject market_node= new JSONObject ();
                                    market_node = (JSONObject) market.get(mid);
                                    JSONObject nme = new JSONObject();
                                    nme.put("_id",mid);
                                    nme.put("gid",gid);
                                    if (market_node.containsKey("express_id")) nme.put("exp_id", market_node.get("express_id"));

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
                                    }
                                    else if (main.gerMarkets.containsKey(mType + " " + mBase)){
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

                                    JSONObject event = new JSONObject();
                                    event = (JSONObject)market_node.get("event");
                                    Set<String> event_ids = event.keySet();
                                    List<JSONObject> evnts = new ArrayList<JSONObject> ();
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
                                        if (event_node.containsKey("price")) ne.put("price",event_node.get("price").toString());
                                        else ne.put("price", "1.01");
                                        String price = ne.get("price").toString();
                                        Double currCoef = Double.parseDouble(price);
                                        Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
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
                                        ne.put("price", df.format(multiPrice).toString().replaceAll(",", "."));
                                        evnts.add(ne);
                                    }
                                    Collections.sort(evnts, new Comparator<JSONObject>() {
                                        @Override
                                        public int compare(JSONObject o1, JSONObject o2) {
                                            return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                                        }
                                    });
                                    nme.put("events",evnts);
                                    VelocityContext mv = new VelocityContext();
                                    StringWriter mvr = new StringWriter();
                                    mv.put("market", nme);
                                    Velocity.mergeTemplate("market.vm", StandardCharsets.UTF_8.name(), mv, mvr);
                                    nme.put("view", mvr.toString());
                                    if (market_node.containsKey("type"))
                                    {
                                        if (game_node.get("type").toString().equals("0"))
                                        {
                                            if (((String)market_node.get("type")).equals("P1XP2") || ((String)market_node.get("type")).equals("P1P2"))
                                            {
                                                gr1.add(nme);
                                            }
                                            if (((String)market_node.get("type")).equals("1X12X2")) gr2.add(nme);
                                            if (((String)market_node.get("type")).equals("1HalfP1XP2")
                                                    || ((String)market_node.get("type")).equals("1SetP1XP2")
                                                    || ((String)market_node.get("type")).equals("1PeriodP1XP2")) gr3.add(nme);
                                            if(((String)market_node.get("type")).equals("Total")) total.add(nme);
                                        }
                                        else if (game_node.get("type").toString().equals("1"))
                                        {
                                            if (((String)market_node.get("type")).equals("P1XP2") || ((String)market_node.get("type")).equals("P1P2")) gr1.add(nme);
                                            if (((String)market_node.get("type")).equals("1X12X2")) gr2.add(nme);
                                            if (((String)market_node.get("type")).equals("NextGoal")) gr3.add(nme);
                                            if (sid == "844")
                                            {
                                                if (((String)market_node.get("type")).equals("1HalfP1XP2")) gr1.add(nme);
                                                if (((String)market_node.get("type")).equals("1Half1X12X2")) gr2.add(nme);
                                                if (((String)market_node.get("type")).equals("1HalfNextGoal")) gr3.add(nme);
                                                if (((String)market_node.get("type")).equals("FirstHalfTotal")) total2.add(nme);
                                            }
                                            if(((String)market_node.get("type")).equals("Total")) total.add(nme);
                                        }
                                    }
                                }
                                Collections.sort(total, new Comparator<JSONObject>() {
                                    @Override
                                    public int compare(JSONObject o1, JSONObject o2) {
                                        return new Double(Double.parseDouble(o1.get("base").toString())).compareTo(new Double(Double.parseDouble(o2.get("base").toString())));
                                    }
                                });
                                Collections.sort(total2, new Comparator<JSONObject>() {
                                    @Override
                                    public int compare(JSONObject o1, JSONObject o2) {
                                        return new Double(Double.parseDouble(o1.get("base").toString())).compareTo(new Double(Double.parseDouble(o2.get("base").toString())));
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
                                VelocityContext game_row = new VelocityContext();
                                StringWriter row = new StringWriter();
                                game_row.put("game", add);
                                game_row.put("talias", 2);
                                game_row.put("region_alias", region.get("alias"));
                                if (add.get("type").toString().equals("0"))
                                {
                                    Date gs = new Date();
                                    gs.setTime(Long.parseLong(add.get("start").toString())*1000);
                                    SimpleDateFormat dt1 = new SimpleDateFormat("dd/MM");
                                    SimpleDateFormat dt2 = new SimpleDateFormat("HH:mm");
                                    game_row.put("day", dt1.format(gs).toString());
                                    game_row.put("hour", dt2.format(gs).toString());
                                }
                                else
                                {
                                    Date gs = new Date();
                                    gs.setTime(Long.parseLong(add.get("start").toString())*1000);
                                    SimpleDateFormat dt = new SimpleDateFormat("dd.MM hh:mm");
                                    game_row.put("start", dt.format(gs).toString());
                                }
                                JSONObject sp_row = new JSONObject();
                                sp_row.put("id",sid);
                                sp_row.put("name", sport_node.get("name"));
                                sp_row.put("alias",sport_node.get("alias"));
                                game_row.put("sport", sp_row);
                                JSONObject rcid = new JSONObject();
                                rcid.put("rid", rid);
                                rcid.put("cid",cid);
                                game_row.put("rc", rcid);
                                game_row.put("comp_name", (String)comp_node.get("name"));
                                game_row.put("gr1", gr1);
                                game_row.put("gr2", gr2);
                                game_row.put("gr3", gr3);
                                game_row.put("gr4", gr4);
                                Velocity.mergeTemplate("game_row.vm", StandardCharsets.UTF_8.name(), game_row, row);
                                new_data.put("view", row.toString());
                            }
                        }
                    }
                    obj.put("data", new_data);
                }
            }
            else
            {
                obj.put("data", null);
            }
            if(s.isOpen()) sendIt(obj);
            this.latch.countDown();
            for (String uid : updates.keySet()) {
                if (s.isOpen()) sendIt(updates.get(uid));
            }
            updates.clear();
        }
        catch (Exception e)
        {
            main.errorLogger.error("Error happened", e);
        }
    }

    void send_event(JSONObject data)
    {
        try
        {
            JSONObject obj = new JSONObject();
            obj.put("command", "get_coeff");
            JSONObject event = (JSONObject)data.get("event");
            Set<String> ev_ids = event.keySet();
            for (String eid : ev_ids)
            {
                JSONObject ev_node = (JSONObject)event.get(eid);
                obj.put("id", ev_node.get("id"));
                String price = ev_node.get("price").toString();
                Double currCoef = Double.parseDouble(price);
                Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
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
                obj.put("value", df.format(multiPrice).toString().replaceAll(",", "."));
            }
            if(s.isOpen()) sendIt(obj);
            this.latch.countDown();
            for (String uid : updates.keySet()) {
                if (s.isOpen()) sendIt(updates.get(uid));
            }
            updates.clear();
        }
        catch (Exception e)
        {
            main.errorLogger.error("Error happened", e);
        }
    }

    void get_vars(JSONObject varsJson)
    {
        try
        {
            JSONObject obj = new JSONObject();
            obj.put("command", "betvars");
            obj.put("gid", varsJson.get("gid").toString());
            List<JSONObject> mkts = new ArrayList<JSONObject>();
            String sid = varsJson.get("sid").toString();
            String rid = varsJson.get("rid").toString();
            String cid = varsJson.get("cid").toString();
            String gid = varsJson.get("gid").toString();
            String tid = varsJson.get("tid").toString();
            switch(varsJson.get("type").toString())
            {
                case "0":
                {
                    JSONObject sport = main.flw.data.get(sid);
                    JSONObject region = ((Map<String, JSONObject>)sport.get("regions")).get(rid);
                    JSONObject comp = ((Map<String, JSONObject>)region.get("comps")).get(cid);
                    JSONObject game = ((Map<String, JSONObject>)comp.get("games")).get(gid);
                    Map<String, JSONObject> markets = (Map<String, JSONObject>)game.get("markets");
                    obj.put("sport_alias", sport.get("alias"));
                    for (String mid : markets.keySet())
                    {
                        JSONObject mnode = markets.get(mid);
                        JSONObject m = new JSONObject();
                        JSONArray eids = new JSONArray();
                        m.put("id", mid);
                        m.put("order", mnode.get("order").toString());
                        Map<String,JSONObject> events = (Map<String, JSONObject>)mnode.get("events");
                        List<JSONObject> eventList = new ArrayList<JSONObject>();
                        for (String eid : events.keySet())
                        {
                            eventList.add(events.get(eid));
                        }
                        Collections.sort(eventList, new Comparator<JSONObject>() {
                            @Override
                            public int compare(JSONObject o1, JSONObject o2) {
                                return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                            }
                        });
                        Map<String,JSONObject> newEvents = new HashMap<String, JSONObject>();
                        List<String> eIds = new ArrayList<String>();
                        for (JSONObject ne : eventList)
                        {
                            String price = ne.get("price").toString();
                            Double currCoef = Double.parseDouble(price);
                            Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
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
                            ne.put("price", df.format(multiPrice).toString().replaceAll(",", "."));
                            newEvents.put(ne.get("_id").toString(), ne);
                            eIds.add(ne.get("_id").toString());
                        }
                        mnode.remove("events");
                        mnode.put("events", newEvents);
                        VelocityContext mv = new VelocityContext();
                        StringWriter mvr = new StringWriter();
                        mv.put("market", mnode);
                        mv.put("events", newEvents);
                        mv.put("eIds", eIds);
                        Velocity.mergeTemplate("market.vm", StandardCharsets.UTF_8.name(), mv, mvr);
                        m.put("html", mvr.toString());
                        m.put("eIds", eIds);
                        m.put("group", mnode.get("group"));
                        mkts.add(m);
                    }
                    Collections.sort(mkts, new Comparator<JSONObject>() {
                        @Override
                        public int compare(JSONObject o1, JSONObject o2) {
                            return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                        }
                    });
                    obj.put("data", mkts);
                    if(s.isOpen()) sendIt(obj);
                    this.latch.countDown();
                    for (String uid : updates.keySet()) {
                        if (s.isOpen()) sendIt(updates.get(uid));
                    }
                    updates.clear();
                }; break;
                case "1":
                {
                    Map<String,JSONObject> live_data = new ConcurrentHashMap<String,JSONObject>();
                    live_data.putAll(main.football.data);
                    live_data.putAll(main.tennis.data);
                    live_data.putAll(main.hockey_basket.data);
                    live_data.putAll(main.other.data);
                    JSONObject sport = live_data.get(sid);
                    JSONObject region = ((Map<String, JSONObject>)sport.get("regions")).get(rid);
                    JSONObject comp = ((Map<String, JSONObject>)region.get("comps")).get(cid);
                    JSONObject game = ((Map<String, JSONObject>)comp.get("games")).get(gid);
                    JSONObject statistic = new JSONObject();
                    if (sid.equals("844"))
                    {
                        JSONObject info = new JSONObject();
                        info.put("current_game_time", game.get("time"));
                        info.put("true_time", game.get("true_time"));
                        info.put("id", gid);
                        info.put("team1_name", game.get("team1"));
                        info.put("score1", game.get("score1"));
                        info.put("short1_color", game.get("short1_color"));
                        info.put("shirt1_color", game.get("shirt1_color"));
                        info.put("team2_name", game.get("team2"));
                        info.put("score2", game.get("score2"));
                        info.put("short2_color", game.get("short2_color"));
                        info.put("shirt2_color", game.get("shirt2_color"));
                        info.put("last_event", game.get("last_event"));
                        statistic.put("info",info);
                        statistic.put("live_events", game.get("live_events"));
                    }
                    statistic.put("sport_alias", sport.get("alias"));
                    obj.put("statistic", statistic);
                    Map<String, JSONObject> markets = (Map<String, JSONObject>)game.get("markets");
                    for (String mid : markets.keySet())
                    {
                        JSONObject mnode = markets.get(mid);
                        JSONObject m = new JSONObject();
                        JSONArray eids = new JSONArray();
                        m.put("id", mid);
                        m.put("order", mnode.get("order").toString());
                        Map<String,JSONObject> events = (Map<String, JSONObject>)mnode.get("events");
                        List<JSONObject> eventList = new ArrayList<JSONObject>();
                        for (String eid : events.keySet())
                        {
                            eventList.add(events.get(eid));
                        }
                        Collections.sort(eventList, new Comparator<JSONObject>() {
                            @Override
                            public int compare(JSONObject o1, JSONObject o2) {
                                return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                            }
                        });
                        Map<String,JSONObject> newEvents = new HashMap<String, JSONObject>();
                        List<String> eIds = new ArrayList<String>();
                        for (JSONObject ne : eventList)
                        {
                            String price = ne.get("price").toString();
                            Double currCoef = Double.parseDouble(price);
                            Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
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
                            ne.put("price", df.format(multiPrice).toString().replaceAll(",", "."));
                            newEvents.put(ne.get("_id").toString(), ne);
                            eIds.add(ne.get("_id").toString());
                        }
                        mnode.remove("events");
                        mnode.put("events", newEvents);
                        VelocityContext mv = new VelocityContext();
                        StringWriter mvr = new StringWriter();
                        mv.put("market", mnode);
                        mv.put("events", newEvents);
                        mv.put("eIds", eIds);
                        Velocity.mergeTemplate("market.vm", StandardCharsets.UTF_8.name(), mv, mvr);
                        m.put("html", mvr.toString());
                        m.put("eIds", eIds);
                        m.put("group", mnode.get("group"));
                        mkts.add(m);
                    }
                    Collections.sort(mkts, new Comparator<JSONObject>() {
                        @Override
                        public int compare(JSONObject o1, JSONObject o2) {
                            return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                        }
                    });
                    obj.put("data", mkts);
                    if(s.isOpen()) sendIt(obj);
                    this.latch.countDown();
                    for (String uid : updates.keySet()) {
                        if (s.isOpen()) sendIt(updates.get(uid));
                    }
                    updates.clear();
                }; break;
                case "2":
                {
                    //System.out.println("VARS ANOTHR!");
                    Date when = new Date();
                    String rqId = UUID.randomUUID().toString();
                    JSONObject request = new JSONObject();
                    request.put("command", "betvars");
                    request.put("tid", tid);
                    request.put("when", when);
                    main.requests.put(rqId, request);

                    JSONObject get_info = new JSONObject();
                    get_info.put("command","get");
                    get_info.put("rid", rqId);
                    JSONObject params = new JSONObject();
                    params.put("source", "betting");
                    JSONObject what = new JSONObject();
                    JSONArray empty = new JSONArray();
                    what.put("game", empty);
                    what.put("market", empty);
                    what.put("event", empty);
                    params.put("what", what);
                    JSONObject where = new JSONObject();
                    JSONObject game = new JSONObject();
                    game.put("id",new Long(gid));
                    where.put("game", game);
                    params.put("where",where);
                    params.put("subscribe", false);
                    get_info.put("params", params);
                    main.client.sendMessage(get_info.toString());
                    // System.out.println("VARS ANOTHR AFTER sendMessage");
                }; break;
            }
        }
        catch (Exception e)
        {
            main.errorLogger.error("Error happened", e);
        }
    }

    void addVars (JSONObject data)
    {
        try
        {
            JSONObject obj = new JSONObject();
            obj.put("command", "betvars");
            JSONObject game = new JSONObject();
            game = (JSONObject) data.get("game");
            if (game != null) {
                Set<String> game_ids = game.keySet();
                List<JSONObject> mkts = new ArrayList<JSONObject>();
                for (String gid : game_ids) {
                    obj.put("gid", gid);
                    JSONObject game_node = (JSONObject) game.get(gid);
                    JSONObject market = new JSONObject();
                    market = (JSONObject) game_node.get("market");
                    Set<String> market_ids = market.keySet();

                    for (String mid : market_ids) {
                        JSONObject market_node = new JSONObject();
                        market_node = (JSONObject) market.get(mid);
                        JSONObject nme = new JSONObject();
                        nme.put("_id", mid);
                        nme.put("gid", gid);
                        if (market_node.containsKey("express_id"))
                            nme.put("exp_id", market_node.get("express_id"));

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
                            //System.out.print("type_");
                        }
                        else if (main.gerMarkets.containsKey(mType + " " + mBase)){
                            gerMarket = (JSONObject) main.gerMarkets.get(mType + " " + mBase);
                            //System.out.print("type+base_");
                        }
                        else if (main.gerMarkets.containsKey(mName)){
                            gerMarket = (JSONObject) main.gerMarkets.get(mName);
                            //System.out.print("name_");
                        }
                        else if (main.gerMarkets.containsKey(mName + " " + mBase)){
                            gerMarket = (JSONObject) main.gerMarkets.get(mName + " " + mBase);
                            //System.out.print("name+base_");
                        }

                        if (gerMarket.containsKey("name")) {
                            if (!mBase.equals("@")) nme.put("name", gerMarket.get("name") + " " + mBase);
                            else nme.put("name", gerMarket.get("name"));
                        }
                        else if (!mBase.equals("@"))nme.put("name", mName + " " + mBase);
                        else nme.put("name", mName);

                        if (market_node.containsKey("order"))
                            nme.put("order", market_node.get("order").toString());
                        else
                            nme.put("order", "999");

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
                            String [] eTypeArr = eType.split("( +)");
                            eType = String.join(" ", eTypeArr);
                            String eName = ne.get("name").toString();
                            String [] eNameArr = eName.split("( +)");
                            eName = String.join(" ", eNameArr);

                            //System.out.print(mType + "_" + mName + "_" + eName + "_" + eType + "_");

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

                            //System.out.println(ne.get("name"));

                            if (event_node.containsKey("order"))
                                ne.put("order", new Integer(Integer.parseInt(event_node.get("order").toString())));
                            else
                                ne.put("order", 999);
                            if (market_node.containsKey("express_id"))
                                ne.put("exp_id", market_node.get("express_id"));
                            if (event_node.containsKey("price")) ne.put("price",event_node.get("price").toString());
                            else ne.put("price", "1.01");
                            String price = ne.get("price").toString();
                            Double currCoef = Double.parseDouble(price);
                            Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
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
                            ne.put("price", df.format(multiPrice).toString().replaceAll(",", "."));
                            evnts.add(ne);
                        }
                        Collections.sort(evnts, new Comparator<JSONObject>() {
                            @Override
                            public int compare(JSONObject o1, JSONObject o2) {
                                return new Integer(Integer.parseInt(o1.get("order").toString())).compareTo(new Integer(Integer.parseInt(o2.get("order").toString())));
                            }
                        });
                        List<String> eIds = new ArrayList<String>();
                        Map<String, JSONObject> mapEvents = new HashMap<String,JSONObject> ();
                        JSONParser parser = new JSONParser();
                        for (JSONObject se : evnts)
                        {
                            eIds.add(se.get("_id").toString());
                            mapEvents.put(se.get("_id").toString(), se);
                        }
                        nme.put("events", evnts);
                        VelocityContext mv = new VelocityContext();
                        StringWriter mvr = new StringWriter();
                        mv.put("market", nme);
                        mv.put("eIds", eIds);
                        mv.put("events", mapEvents);
                        Velocity.mergeTemplate("market.vm", StandardCharsets.UTF_8.name(), mv, mvr);
                        nme.put("view", mvr.toString());
                        JSONObject m = new JSONObject();
                        m.put("id", mid);
                        m.put("order", nme.get("order").toString());
                        m.put("html", nme.get("view"));
                        m.put("eIds", eIds);
                        m.put("group", nme.get("group"));
                        mkts.add(m);
                    }
                }
                if(s.isOpen()) sendIt(obj);
                this.latch.countDown();
                for (String uid : updates.keySet()) {
                    if (s.isOpen()) sendIt(updates.get(uid));
                }
                updates.clear();
            }
            else{
                obj.put("data", null);
                if(s.isOpen()) sendIt(obj);
                this.latch.countDown();
                for (String uid : updates.keySet()) {
                    if (s.isOpen()) sendIt(updates.get(uid));
                }
                updates.clear();
            }

        }
        catch (Exception e)
        {

            main.errorLogger.error("Error happened",e);
        }
    }

    public void get_total(JSONObject totalJson)
    {
        try
        {
            String sid = totalJson.get("sid").toString();
            String rid = totalJson.get("rid").toString();
            String cid = totalJson.get("cid").toString();
            String gid = totalJson.get("gid").toString();
            String tid = totalJson.get("tid").toString();
            JSONObject obj = new JSONObject();
            obj.put("command", "get_total");
            obj.put("gid", gid);
            List<JSONObject> mkts = new ArrayList<JSONObject>();
            switch (totalJson.get("type").toString()) {
                case "0": {
                    JSONObject sport = main.flw.data.get(sid);
                    JSONObject region = ((Map<String, JSONObject>) sport.get("regions")).get(rid);
                    JSONObject comp = ((Map<String, JSONObject>) region.get("comps")).get(cid);
                    JSONObject game = ((Map<String, JSONObject>) comp.get("games")).get(gid);
                    Map<String, JSONObject> markets = (Map<String, JSONObject>) game.get("markets");
                    List<JSONObject> totals = new ArrayList<JSONObject>();
                    for (String mid : markets.keySet()) {
                        JSONObject mnode = markets.get(mid);
                        if (mnode.containsKey("type")) {
                            if (((String) mnode.get("type")).equals("Total"))
                                totals.add(mnode);
                        }
                    }
                    Collections.sort(totals, new Comparator<JSONObject>() {
                        @Override
                        public int compare(JSONObject o1, JSONObject o2) {
                            return new Double(String.valueOf(o1.get("base")))
                                    .compareTo(new Double(String.valueOf(o2.get("base"))));
                        }
                    });
                    if (totals.size() > 0)
                        obj.put("data", totals.get(0));
                    else
                        obj.put("data", null);
                };	break;
                case "1": {
                    Map<String,JSONObject> live_data = new ConcurrentHashMap<String,JSONObject>();
                    live_data.putAll(main.football.data);
                    live_data.putAll(main.tennis.data);
                    live_data.putAll(main.hockey_basket.data);
                    live_data.putAll(main.other.data);
                    JSONObject sport = live_data.get(sid);
                    JSONObject region = ((Map<String, JSONObject>) sport.get("regions")).get(rid);
                    JSONObject comp = ((Map<String, JSONObject>) region.get("comps")).get(cid);
                    JSONObject game = ((Map<String, JSONObject>) comp.get("games")).get(gid);
                    Map<String, JSONObject> markets = (Map<String, JSONObject>) game.get("markets");
                    List<JSONObject> totals = new ArrayList<JSONObject>();
                    for (String mid : markets.keySet()) {
                        JSONObject mnode = markets.get(mid);
                        if (mnode.containsKey("type")) {
                            if (((String) mnode.get("type")).equals("Total")
                                    //|| ((String) mnode.get("type")).equals("SetTotal")
                                    || ((String) mnode.get("type")).equals("FirstHalfTotal")
                                    || ((String) mnode.get("type")).equals("Gametotalpoints")
                                    )
                                totals.add(mnode);
                        }
                    }
                    Collections.sort(totals, new Comparator<JSONObject>() {
                        @Override
                        public int compare(JSONObject o1, JSONObject o2) {
                            return new Double(String.valueOf(o1.get("base")))
                                    .compareTo(new Double(String.valueOf(o2.get("base"))));
                        }
                    });
                    if (sid.equals("844"))
                    {
                        if (totals.size()>0)
                        {
                            JSONObject tf = new JSONObject();
                            for (JSONObject total : totals){
                                if (Double.parseDouble(total.get("base").toString()) == 2.5){
                                    tf = total; break;
                                }
                            }
                            if (!tf.isEmpty())
                                obj.put("data", tf);
                            else
                                obj.put("data", totals.get(0));
                        }
                        else
                            obj.put("data", null);
                    }
                    else {
                        if (totals.size() > 0)
                            obj.put("data", totals.get(0));
                        else
                            obj.put("data", null);
                    }
                }; break;
            }
            if(s.isOpen()) sendIt(obj);
            this.latch.countDown();
            for (String uid : updates.keySet()) {
                if (s.isOpen()) sendIt(updates.get(uid));
            }
            updates.clear();
        }
        catch (Exception e)
        {
            JSONObject obj = new JSONObject();
            obj.put("command", "get_total");
            obj.put("gid", totalJson.get("gid").toString());
            obj.put("data", null);
            main.errorLogger.error("Error happened", e);
            if(s.isOpen()) sendIt(obj);
            this.latch.countDown();
        }
    }

    public void sendIt(JSONObject obj) {
        try{
            if (s.isOpen()){
                System.out.println(new  Date() + " " + id + " sent "+obj.get("command").toString());
                s.getRemote().sendStringByFuture(obj.toString());
            }
        }
        catch (Exception e) {
            if (e.getClass() == IllegalStateException.class) {
                System.out.println("BlOCK");
            }
            main.errorLogger.error(e);
        }
    }

    void build_live() {
        try {
            JSONObject obj = new JSONObject();
            List<JSONObject> sp = new ArrayList<JSONObject>();
            List<JSONObject> cm = new ArrayList<JSONObject>();
            List<JSONObject> gm = new ArrayList<JSONObject>();
            obj.put("type", "live");
            obj.put("command", "build");
            Map<String,JSONObject> live_data = new ConcurrentHashMap<String,JSONObject>();
            live_data.putAll(main.football.data);
            live_data.putAll(main.tennis.data);
            live_data.putAll(main.hockey_basket.data);
            live_data.putAll(main.other.data);

            for (String sid : live_data.keySet()) {
                JSONObject sport = live_data.get(sid);
                JSONObject sp_cl = new JSONObject();
                int s_l = 0;
                Map<String,JSONObject> regions = ((Map<String,JSONObject>)sport.get("regions"));
                String compString = "";
                for (String rid : regions.keySet()){
                    JSONObject region = regions.get(rid);
                    Map<String,JSONObject> comps = ((Map<String,JSONObject>)region.get("comps"));
                    for (String cid : comps.keySet()) {
                        JSONObject comp = comps.get(cid);
                        JSONObject cm_cl = new JSONObject();
                        cm_cl.put("view", comp.get("filter"));
                        cm.add(cm_cl);
                        Map<String,JSONObject> games = ((Map<String,JSONObject>)comp.get("games"));
                        int c_l = 0;
                        for(String gid : games.keySet()) {
                            JSONObject game = games.get(gid);
                            JSONObject gm_cl = new JSONObject();
                            gm_cl.put("start", Long.parseLong(game.get("start").toString()));
                            gm_cl.put("alias", game.get("alias"));
                            gm_cl.put("sport", sport.get("_id"));
                            Map<String,JSONObject> markets = ((Map<String,JSONObject>)game.get("markets"));
                            List<JSONObject> gr1 = new ArrayList<JSONObject>();
                            List<JSONObject> gr2 = new ArrayList<JSONObject>();
                            List<JSONObject> gr3 = new ArrayList<JSONObject>();
                            List<JSONObject> gr4 = new ArrayList<JSONObject>();
                            List<JSONObject> total = new ArrayList<JSONObject>();
                            List<JSONObject> total2 = new ArrayList<JSONObject>();
                            Map<String, JSONObject> mkts = new HashMap<String, JSONObject>();
                            for (String mid : markets.keySet()) {
                                JSONObject market_node = markets.get(mid);
                                Map<String, JSONObject> evnts = (Map<String, JSONObject>)markets.get(mid).get("events");
                                for (String eid : evnts.keySet()) {
                                    /*String price = evnts.get(eid).get("price").toString();
                                    Double currCoef = Double.parseDouble(price);
                                    Double multiPrice = Double.parseDouble(price) * Double.parseDouble(multi);
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
                                    evnts.get(eid).put("price", df.format(multiPrice).toString().replaceAll(",", "."));*/
                                }
                                if (market_node.containsKey("type")) {
                                    if (((String)market_node.get("type")).equals("P1XP2") || ((String)market_node.get("type")).equals("P1P2")) gr1.add(market_node);
                                    if (((String)market_node.get("type")).equals("1X12X2") && sid.equals("844")) gr2.add(market_node);
                                    else if (main.sportPartsT.containsKey(sid)){
                                        if (((String)market_node.get("type")).toLowerCase().contains(main.sportPartsT.get(sid).toLowerCase()+"p1p2") ||
                                                ((String)market_node.get("type")).toLowerCase().contains(main.sportPartsT.get(sid).toLowerCase()+"p1xp2")){
                                            gr2.add(market_node);
                                        }
                                    }
                                    if (((String)market_node.get("type")).equals("NextGoal") && sid.equals("844")) gr3.add(market_node);
                                    if (sid.equalsIgnoreCase("844")) {
                                        if (((String)market_node.get("type")).equals("1HalfP1XP2")) gr1.add(market_node);
                                        if (((String)market_node.get("type")).equals("1Half1X12X2")) gr2.add(market_node);
                                        if (((String)market_node.get("type")).equals("1HalfNextGoal")) gr3.add(market_node);
                                        if (((String)market_node.get("type")).equals("FirstHalfTotal")) total2.add(market_node);
                                    }
                                    if(((String)market_node.get("type")).equals("Total")
                                            //|| ((String)market_node.get("type")).equals("SetTotal")
                                            // || ((String)market_node.get("type")).equals("PeriodTotal")
                                            || ((String)market_node.get("type")).equals("Gametotalpoints")) total.add(market_node);
                                }
                            }
                            Collections.sort(total, new Comparator<JSONObject>() {
                                @Override
                                public int compare(JSONObject o1, JSONObject o2) {
                                    return new Double(Double.parseDouble(o1.get("base").toString())).compareTo(Double.parseDouble(o2.get("base").toString()));
                                }
                            });
                            Collections.sort(total2, new Comparator<JSONObject>() {
                                @Override
                                public int compare(JSONObject o1, JSONObject o2) {
                                    return new Double(Double.parseDouble(o1.get("base").toString())).compareTo(Double.parseDouble(o2.get("base").toString()));
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
                            game_row.put("region_alias", region.get("alias"));
                            game_row.put("gr1", gr1);
                            game_row.put("gr2", gr2);
                            game_row.put("gr3", gr3);
                            game_row.put("gr4", gr4);
                            if (!sid.equals("844") && main.sportPartsT.containsKey(sid)) game_row.put("part", main.sportPartsT.get(sid).toLowerCase());
                            Velocity.mergeTemplate("game_row.vm", StandardCharsets.UTF_8.name(), game_row, row);
                            gm_cl.put("view", row.toString());
                            gm.add(gm_cl);
                            c_l++; s_l++;
                        }
                        StringWriter cli = new StringWriter();
                        VelocityContext ctx = new VelocityContext();
                        String comp_name = (String)comp.get("name");
                        comp_name = comp_name.replaceFirst("-", "<br><hr>").replace("(LIVE)", "");
                        ctx.put("sport", "live");
                        ctx.put("id", sid);
                        ctx.put("cntr", region.get("alias"));
                        JSONObject crow = new JSONObject();
                        crow.put("_id",cid);
                        crow.put("name", comp_name);
                        crow.put("count",c_l);
                        ctx.put("comp",crow);
                        Velocity.mergeTemplate("comp_li.vm", StandardCharsets.UTF_8.name(), ctx, cli);
                        compString += cli.toString();
                    }
                }
                StringWriter hl_sport = new StringWriter();
                VelocityContext ctx = new VelocityContext();
                ctx.put("id",sid);
                ctx.put("name", sport.get("name"));
                ctx.put("alias", sport.get("alias"));
                ctx.put("order", new Integer(sport.get("order").toString()));
                if (!sid.equals("844")){
                    if (main.sportPartsGer.containsKey(sid)){
                        ctx.put("part", main.sportPartsGer.get(sid));
                    }
                    else ctx.put("part", "Satz");
                }
                Velocity.mergeTemplate("hl_sport.vm", StandardCharsets.UTF_8.name(), ctx, hl_sport);
                VelocityContext ctx3 = new VelocityContext();
                StringWriter sfl = new StringWriter();
                ctx3.put("id", sid);
                ctx3.put("alias", (String)sport.get("alias"));
                ctx3.put("order", new Integer(sport.get("order").toString()));
                ctx3.put("name", sport.get("name"));
                ctx3.put("sport", "live");
                ctx3.put("comps", compString);
                ctx3.put("fl", s_l);
                Velocity.mergeTemplate("sfl.vm", StandardCharsets.UTF_8.name(), ctx3, sfl);
                sp_cl.put("header",hl_sport.toString());
                sp_cl.put("menu", sfl.toString());
                sp_cl.put("order",sport.get("order"));
                sp.add(sp_cl);
            }
            Collections.sort(sp, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    return (new Integer(Integer.parseInt(o1.get("order").toString()))).compareTo(Integer.parseInt(o2.get("order").toString()));
                }
            });
            Collections.sort(gm, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    if (!new Long(Long.parseLong(o1.get("start").toString())).equals(new Long(Long.parseLong(o2.get("start").toString()))))
                        return (new Long(Long.parseLong(o1.get("start").toString()))).compareTo(new Long(Long.parseLong(o2.get("start").toString())));
                    else
                        return (new Integer(Integer.parseInt(o1.get("alias").toString()))).compareTo(new Integer(Integer.parseInt(o2.get("alias").toString())));
                }
            });
            obj.put("sport", sp);
            obj.put("game", gm);
            if (s.isOpen()) sendIt(obj);
            this.latch.countDown();
        }
        catch (Exception e) {
            e.printStackTrace();
            main.errorLogger.error("Error happened", e);
        }
    }

    void build_flive()
    {
        JSONObject obj = new JSONObject();
        List<JSONObject> sp = new ArrayList<JSONObject>();
        List<JSONObject> cm = new ArrayList<JSONObject>();
        List<JSONObject> gm = new ArrayList<JSONObject>();
        obj.put("type", "flive");
        obj.put("command", "build");
        try
        {
            ConcurrentHashMap<String, JSONObject> sports = (ConcurrentHashMap<String, JSONObject>)main.flw.data;
            for (String sid : sports.keySet())
            {
                JSONObject sport = sports.get(sid);
                JSONObject sp_cl = new JSONObject();
                sp_cl.put("menu", sport.get("menu"));
                sp_cl.put("order",sport.get("order"));
                sp.add(sp_cl);
                Map<String,JSONObject> regions = ((Map<String,JSONObject>)sport.get("regions"));
                for (String rid : regions.keySet())
                {
                    JSONObject region = regions.get(rid);
                    Map<String,JSONObject> comps = ((Map<String,JSONObject>)region.get("comps"));
                    for (String cid : comps.keySet())
                    {
                        JSONObject comp = comps.get(cid);
                        JSONObject cm_cl = new JSONObject();
                        cm.add(cm_cl);
                        Map<String,JSONObject> gms = ((Map<String,JSONObject>)comp.get("games"));
                        Map<String,JSONObject> games = ((Map<String,JSONObject>)comp.get("games"));
                        for(String gid : games.keySet())
                        {
                            JSONObject game = games.get(gid);
                            JSONObject gm_cl = new JSONObject();
                            gm_cl.put("start", Long.parseLong(game.get("start").toString()));
                            gm_cl.put("alias", game.get("alias"));
                            gm_cl.put("sport", sport.get("_id"));
                            Map<String,JSONObject> markets = ((Map<String,JSONObject>)game.get("markets"));
                            List<JSONObject> gr1 = new ArrayList<JSONObject>();
                            List<JSONObject> gr2 = new ArrayList<JSONObject>();
                            List<JSONObject> gr3 = new ArrayList<JSONObject>();
                            List<JSONObject> gr4 = new ArrayList<JSONObject>();
                            List<JSONObject> total = new ArrayList<JSONObject>();
                            List<JSONObject> total2 = new ArrayList<JSONObject>();
                            Map<String, JSONObject> mkts = new HashMap<String, JSONObject>();
                            for (String mid : markets.keySet())
                            {
                                JSONObject market_node= new JSONObject ();
                                market_node = markets.get(mid);
                                if (market_node.containsKey("type"))
                                {
                                    if (((String)market_node.get("type")).equals("P1XP2") || ((String)market_node.get("type")).equals("P1P2"))
                                    {
                                        gr1.add(market_node);
                                    }
                                    if (((String)market_node.get("type")).equals("1X12X2")) gr2.add(market_node);
                                    if (((String)market_node.get("type")).equals("1HalfP1XP2")
                                            || ((String)market_node.get("type")).equals("1SetP1XP2")
                                            || ((String)market_node.get("type")).equals("1PeriodP1XP2")) gr3.add(market_node);
                                    if(((String)market_node.get("type")).equals("Total")) total.add(market_node);
                                }
                            }
                            Collections.sort(total, new Comparator<JSONObject>() {
                                @Override
                                public int compare(JSONObject o1, JSONObject o2) {
                                    return new Double(Double.parseDouble(o1.get("base").toString())).compareTo(Double.parseDouble(o2.get("base").toString()));
                                }
                            });
                            if (gr1.size()>1)
                            {
                                if (gr1.get(0).get("type").toString().equals("P1XP2"))
                                    gr1.remove(gr1.get(1));
                                else if (gr1.get(1).get("type").toString().equals("P1XP2"))
                                    gr1.remove(gr1.get(0));
                            }
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
                            }
                            else if (total.size()>0)
                            {
                                gr4.add(total.get(0));
                            }
                            VelocityContext game_row = new VelocityContext();
                            StringWriter row = new StringWriter();
                            Date gs = new Date();
                            gs.setTime(Long.parseLong(game.get("start").toString())*1000);
                            SimpleDateFormat dt1 = new SimpleDateFormat("dd/MM");
                            SimpleDateFormat dt2 = new SimpleDateFormat("HH:mm");
                            game_row.put("game", game);
                            game_row.put("talias", "0");
                            game_row.put("day", dt1.format(gs).toString());
                            game_row.put("hour", dt2.format(gs).toString());
                            JSONObject sp_row = new JSONObject();
                            sp_row.put("id",sid);
                            sp_row.put("name", sport.get("name"));
                            sp_row.put("alias",sport.get("alias"));
                            game_row.put("sport", sp_row);
                            JSONObject rcid = new JSONObject();
                            rcid.put("rid", rid);
                            rcid.put("cid",cid);
                            game_row.put("rc", rcid);
                            game_row.put("region_alias", region.get("alias"));
                            game_row.put("comp_name", (String)comp.get("name"));
                            game_row.put("gr1", gr1);
                            game_row.put("gr2", gr2);
                            game_row.put("gr3", gr3);
                            game_row.put("gr4", gr4);
                            Velocity.mergeTemplate("game_row.vm", StandardCharsets.UTF_8.name(), game_row, row);
                            gm_cl.put("view", row.toString());
                            gm.add(gm_cl);
                        }
                    }
                }
            }
            Collections.sort(sp, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    return (new Integer((int)o1.get("order"))).compareTo(new Integer((int)o2.get("order")));
                }
            });
            Collections.sort(gm, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    if (!new Long((long)o1.get("start")).equals(new Long((long)o2.get("start"))))
                        return (new Long((long)o1.get("start"))).compareTo(new Long((long)o2.get("start")));
                    else
                        return (new Long(Long.parseLong((String)o1.get("alias")))).compareTo(new Long(Long.parseLong((String)o2.get("alias"))));
                }
            });
            obj.put("sport", sp);
            obj.put("comp", cm);
            obj.put("game", gm);
            if (s.isOpen()) sendIt(obj);
            this.latch.countDown();
        }
        catch (Exception e)
        {
            main.errorLogger.error("Error happened", e);
        }
    }

    void build_menu()
    {
        JSONObject obj = new JSONObject();
        List<JSONObject> sp = new ArrayList<JSONObject>();
        obj.put("type", "menu");
        obj.put("command", "build");
        try
        {
            for (String sid : main.mw.data.keySet())
            {
                JSONObject sport = main.mw.data.get(sid);
                JSONObject sp_cl = new JSONObject();
                sp_cl.put("top", sport.get("top"));
                sp_cl.put("menu", sport.get("menu"));
                sp_cl.put("order",sport.get("order"));
                sp_cl.put("sAlias", sport.get("alias"));
                sp_cl.put("sName", sport.get("name"));
                sp.add(sp_cl);
            }
            Collections.sort(sp, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    return (new Integer((int)o1.get("order"))).compareTo(new Integer((int)o2.get("order")));
                }
            });
            obj.put("sport", sp);
            if(s.isOpen()) sendIt(obj);
            this.latch.countDown();
        }
        catch (Exception e)
        {
            main.errorLogger.error("Error happened", e);
        }
    }

    void build_favorite()
    {
        JSONObject obj = new JSONObject();
        List<JSONObject> sp = new ArrayList<JSONObject>();
        List<JSONObject> cm = new ArrayList<JSONObject>();
        List<JSONObject> gm = new ArrayList<JSONObject>();
        obj.put("type", "favorite");
        obj.put("command", "build");
        try
        {
            ConcurrentHashMap<String, JSONObject> sports = (ConcurrentHashMap<String, JSONObject>)main.flw.data;
            for (String sid : sports.keySet())
            {
                JSONObject sport = sports.get(sid);
                JSONObject sp_cl = new JSONObject();
                sp_cl.put("menu", sport.get("menu"));
                sp_cl.put("order",sport.get("order"));
                sp.add(sp_cl);
                Map<String,JSONObject> regions = ((Map<String,JSONObject>)sport.get("regions"));
                for (String rid : regions.keySet())
                {
                    JSONObject region = regions.get(rid);
                    Map<String,JSONObject> comps = ((Map<String,JSONObject>)region.get("comps"));
                    for (String cid : comps.keySet())
                    {
                        JSONObject comp = comps.get(cid);
                        JSONObject cm_cl = new JSONObject();
                        cm.add(cm_cl);
                        Map<String,JSONObject> gms = ((Map<String,JSONObject>)comp.get("games"));
                        Map<String,JSONObject> games = ((Map<String,JSONObject>)comp.get("games"));
                        for(String gid : games.keySet())
                        {
                            JSONObject game = games.get(gid);
                            JSONObject gm_cl = new JSONObject();
                            gm_cl.put("start", Long.parseLong(game.get("start").toString()));
                            gm_cl.put("alias", game.get("alias"));
                            gm_cl.put("sport", sport.get("_id"));
                            Map<String,JSONObject> markets = ((Map<String,JSONObject>)game.get("markets"));
                            List<JSONObject> gr1 = new ArrayList<JSONObject>();
                            List<JSONObject> gr2 = new ArrayList<JSONObject>();
                            List<JSONObject> gr3 = new ArrayList<JSONObject>();
                            List<JSONObject> gr4 = new ArrayList<JSONObject>();
                            List<JSONObject> total = new ArrayList<JSONObject>();
                            List<JSONObject> total2 = new ArrayList<JSONObject>();
                            Map<String, JSONObject> mkts = new HashMap<String, JSONObject>();
                            for (String mid : markets.keySet())
                            {
                                JSONObject market_node= new JSONObject ();
                                market_node = markets.get(mid);
                                if (market_node.containsKey("type"))
                                {
                                    if (((String)market_node.get("type")).equals("P1XP2") || ((String)market_node.get("type")).equals("P1P2"))
                                    {
                                        gr1.add(market_node);
                                    }
                                    if (((String)market_node.get("type")).equals("1X12X2")) gr2.add(market_node);
                                    if (((String)market_node.get("type")).equals("1HalfP1XP2")
                                            || ((String)market_node.get("type")).equals("1SetP1XP2")
                                            || ((String)market_node.get("type")).equals("1PeriodP1XP2")) gr3.add(market_node);
                                    if(((String)market_node.get("type")).equals("Total")) total.add(market_node);
                                }
                            }
                            Collections.sort(total, new Comparator<JSONObject>() {
                                @Override
                                public int compare(JSONObject o1, JSONObject o2) {
                                    return new Double(Double.parseDouble(o1.get("base").toString())).compareTo(Double.parseDouble(o2.get("base").toString()));
                                }
                            });
                            if (gr1.size()>1)
                            {
                                if (gr1.get(0).get("type").toString().equals("P1XP2"))
                                    gr1.remove(gr1.get(1));
                                else if (gr1.get(1).get("type").toString().equals("P1XP2"))
                                    gr1.remove(gr1.get(0));
                            }
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
                            }
                            else if (total.size()>0)
                            {
                                gr4.add(total.get(0));
                            }
                            VelocityContext game_row = new VelocityContext();
                            StringWriter row = new StringWriter();
                            Date gs = new Date();
                            gs.setTime(Long.parseLong(game.get("start").toString())*1000);
                            SimpleDateFormat dt1 = new SimpleDateFormat("dd/MM");
                            SimpleDateFormat dt2 = new SimpleDateFormat("HH:mm");
                            game_row.put("game", game);
                            game_row.put("talias", "2");
                            game_row.put("day", dt1.format(gs).toString());
                            game_row.put("hour", dt2.format(gs).toString());
                            JSONObject sp_row = new JSONObject();
                            sp_row.put("id",sid);
                            sp_row.put("name", sport.get("name"));
                            sp_row.put("alias",sport.get("alias"));
                            game_row.put("sport", sp_row);
                            JSONObject rcid = new JSONObject();
                            rcid.put("rid", rid);
                            rcid.put("cid",cid);
                            game_row.put("rc", rcid);
                            game_row.put("region_alias", region.get("alias"));
                            game_row.put("comp_name", (String)comp.get("name"));
                            game_row.put("gr1", gr1);
                            game_row.put("gr2", gr2);
                            game_row.put("gr3", gr3);
                            game_row.put("gr4", gr4);
                            Velocity.mergeTemplate("game_row.vm", StandardCharsets.UTF_8.name(), game_row, row);
                            gm_cl.put("view", row.toString());
                            gm.add(gm_cl);
                        }
                    }
                }
            }
            Collections.sort(sp, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    return (new Integer((int)o1.get("order"))).compareTo(new Integer((int)o2.get("order")));
                }
            });
            Collections.sort(gm, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    if (!new Long((long)o1.get("start")).equals(new Long((long)o2.get("start"))))
                        return (new Long((long)o1.get("start"))).compareTo(new Long((long)o2.get("start")));
                    else
                        return (new Long(Long.parseLong((String)o1.get("alias")))).compareTo(new Long(Long.parseLong((String)o2.get("alias"))));
                }
            });
            obj.put("sport", sp);
            obj.put("comp", cm);
            obj.put("game", gm);
            if (s.isOpen()) sendIt(obj);
            this.latch.countDown();
        }
        catch (Exception e)
        {
            main.errorLogger.error("Error happened", e);
        }
    }
}
