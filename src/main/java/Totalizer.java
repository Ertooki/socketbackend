import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Administrator on 20.04.2017.
 */
public class Totalizer extends Thread {

    private BlockingQueue<JSONObject> queue = new LinkedBlockingQueue<>();
    public Map<String,JSONObject> totals = new ConcurrentHashMap<>();
    CountDownLatch latch;
    String name;

    Totalizer(CountDownLatch count, String n) {
        latch = count;
        name = n;
    }

    public BlockingQueue<JSONObject> getQueue(){
        return queue;
    }

    public void run(){
        try {
            latch.countDown();
            while (!interrupted()) {
                JSONObject qObj = queue.take();
                CountDownLatch l = (CountDownLatch)qObj.get("latch");
                Map<String,JSONObject> sports = (Map<String,JSONObject>)qObj.get("data");
                Map<String,JSONObject> upTotals = new ConcurrentHashMap<>();
                for (String sid : sports.keySet()){
                    JSONObject sport = sports.get(sid);
                    Map<String,JSONObject> regions = (Map<String,JSONObject>)sport.get("regions");
                    for (String rid  : regions.keySet()){
                        JSONObject region = regions.get(rid);
                        Map<String,JSONObject> comps = (Map<String,JSONObject>)region.get("comps");
                        for (String cid : comps.keySet()) {
                            JSONObject comp = comps.get(cid);
                            Map<String,JSONObject> games = (Map<String,JSONObject>)comp.get("games");
                            for (String gid : games.keySet()) {
                                JSONObject game = games.get(gid);
                                Map<String,JSONObject> markets = (Map<String,JSONObject>)game.get("markets");
                                List<JSONObject> fullgame_totals = new ArrayList<>();
                                List<JSONObject> halfgame_totals = new ArrayList<>();
                                List<JSONObject> all_totals = new ArrayList<>();
                                for (String mid : markets.keySet()) {
                                    JSONObject market = markets.get(mid);
                                    if (market.containsKey("type")) {
                                        if (sid.equals("844")) {
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
                                                            fullgame_totals.add(market);
                                                        }
                                                        else if(market.get("base").toString().equals("2.5")) fullgame_totals.add(market);
                                                    }
                                                }

                                            }
                                            else if(market.get("type").toString().equals("FirstHalfTotal")
                                                    && !market.get("base").toString().equals("@")
                                                    && Double.parseDouble(market.get("base").toString())%1 != 0.0) {
                                                halfgame_totals.add(market);
                                            }
                                        }
                                        else if(sid.equals("848")){
                                            if(market.get("type").toString().equals("Gametotalpoints")
                                                    && !market.get("base").toString().equals("@")
                                                    && Double.parseDouble(market.get("base").toString())%1 != 0.0){
                                                all_totals.add(market);
                                                Map<String,JSONObject> events = (Map<String,JSONObject>)market.get("events");
                                                for (String eid : events.keySet()) {
                                                    JSONObject event = events.get(eid);
                                                    if(event.get("type").toString().toLowerCase().equals("totalmore") ||
                                                            event.get("type").toString().toLowerCase().equals("over") ||
                                                            event.get("type").toString().toLowerCase().equals("more")) {
                                                        if(Double.parseDouble(event.get("price").toString())>=1.4 &&
                                                                Double.parseDouble(event.get("price").toString())<=2.55) {
                                                            fullgame_totals.add(market);
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
                                                            event.get("type").toString().toLowerCase().equals("over") ||
                                                            event.get("type").toString().toLowerCase().equals("more")) {
                                                        if(Double.parseDouble(event.get("price").toString())>=1.4 &&
                                                                Double.parseDouble(event.get("price").toString())<=2.55) {
                                                            fullgame_totals.add(market);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                if (sid.equals("844")) {
                                    JSONObject soccerTotals = new JSONObject();
                                    if (fullgame_totals.size()>0){
                                        JSONObject needTotal = fullgame_totals.get(0);
                                        for (JSONObject total : fullgame_totals) {
                                            if (!total.get("base").toString().equals("@")
                                                    && Double.parseDouble(total.get("base").toString())%1 != 0.0) {
                                                if (total.get("base").toString().equals("2.5")) {
                                                    needTotal = total;
                                                    break;
                                                }
                                                if (Double.parseDouble(total.get("base").toString()) < Double.parseDouble(needTotal.get("base").toString()))
                                                {
                                                    needTotal = total;
                                                }
                                            }
                                        }
                                        soccerTotals.put("full",needTotal);
                                    }
                                    else if(all_totals.size()>0){
                                        JSONObject needTotal = all_totals.get(0);
                                        for (JSONObject total : all_totals) {
                                            if (!total.get("base").toString().equals("@")
                                                    && Double.parseDouble(total.get("base").toString())%1 != 0.0) {
                                                if (total.get("base").toString().equals("2.5")) {
                                                    needTotal = total;
                                                    break;
                                                }
                                                if (Double.parseDouble(total.get("base").toString()) < Double.parseDouble(needTotal.get("base").toString()))
                                                {
                                                    needTotal = total;
                                                }
                                            }
                                        }
                                        soccerTotals.put("full",needTotal);
                                    }
                                    if(game.get("type").toString().equals("1")){
                                        if(game.containsKey("state")){
                                            if(game.get("state")!=null){
                                                if(game.get("state").toString().equals("set1") || game.get("state").toString().equals("wait")) {
                                                    if(halfgame_totals.size()>0){
                                                        JSONObject needTotal = halfgame_totals.get(0);
                                                        for (JSONObject total : halfgame_totals) {
                                                            if (!total.get("base").toString().equals("@")
                                                                    //al
                                                                    && Double.parseDouble(total.get("base").toString())%1 != 0.0) {
                                                                if (total.get("base").toString().equals("2.5")) {
                                                                    needTotal = total;
                                                                    break;
                                                                }
                                                                if (Double.parseDouble(total.get("base").toString()) < Double.parseDouble(needTotal.get("base").toString()))
                                                                {
                                                                    needTotal = total;
                                                                }
                                                            }
                                                        }
                                                        soccerTotals.put("half",needTotal);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    upTotals.put(gid,soccerTotals);
                                }
                                else {
                                    if(fullgame_totals.size()>0){
                                        JSONObject needTotal = fullgame_totals.get(0);
                                        for (JSONObject total : fullgame_totals) {
                                            if (!total.get("base").toString().equals("@")
                                                    && Double.parseDouble(total.get("base").toString())%1 != 0.0) {
                                                if (Double.parseDouble(total.get("base").toString()) < Double.parseDouble(needTotal.get("base").toString())) needTotal = total;
                                            }
                                        }
                                        upTotals.put(gid,needTotal);
                                    }
                                    else if(all_totals.size()>0){
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
                                        upTotals.put(gid,needTotal);
                                    }
                                }
                            }
                        }
                    }
                }
                this.totals = upTotals;
                l.countDown();
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally {
            main.sendNotification("Totalizer crash", new Date()+"\n\n  Totalizer "+name+" stopped.");
        }
    }
}
