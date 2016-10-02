import twitter4j.TwitterException;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import twitter4j.UserMentionEntity;

import java.io.*;
import java.util.*;
import java.util.TimeZone;

import org.apache.commons.lang3.StringEscapeUtils;

import java.text.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class Append {
	
	public static Status post;
	
	public static void main(String[] args) {
		if (args.length < 1) {
            System.out.println("WARNING: args.length is < 1");
            System.exit(-1);
        }

        //args[0] = input raw json documents. each one is appended to a single output json.
        //args[1] = "start" to identify first file to be processed in the batch.
        //          "end"   to identify last file to be processed in the batch.
        //          "inter" to identify any intermediate files to be processed in the batch.
        //args[2] = output json file name.
        
        try {
        	BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/dbasak/Documents/workspace/"+args[2]), "UTF-8"));
        	BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/Users/dbasak/Documents/workspace/Append/bin/"+args[0], true), "UTF-8"));
        	
        	String ln;
        	String tweet;
        	String hashtag;
        	String usermention;
        	String urls;
        	String emoji;
        	String text_en;
        	String text_es;
        	String text_ko;
        	String text_tr;
        	boolean last_file;
        	boolean first_file;
        	
        	System.out.println(args[1]);
        	
        	if (args[1].equalsIgnoreCase("start")) {
        		first_file = true;
        		last_file = false;
        	} else if (args[1].equalsIgnoreCase("end")) {
        		first_file = false;
        		last_file = true;
        	} else {
        		first_file = false;
        		last_file = false;
        	}
        	
        	System.out.println(first_file);
        		
        	
        	// Politics, News, Entertainment, Sports, Technology
        	KeyMatch[] kArray = new KeyMatch[5];
        	
        	kArray[0] = new KeyMatch();
        	kArray[0].setTopic("Politics");
        	kArray[0].setKeyArray(new String[]{"CLINTON", "TRUMP", "ELECTION", "REPUBLICAN", "DEMOCRAT", "PRESIDENT", "GOP"});
        	kArray[1] = new KeyMatch();
        	kArray[1].setTopic("News");
        	kArray[1].setKeyArray(new String[]{"SYRIA", "ISIS", "ISIL", "ISLAMIC", " WAR ", "BOMBING"});
        	kArray[2] = new KeyMatch();
        	kArray[2].setTopic("Entertainment");
        	kArray[2].setKeyArray(new String[]{"#GOT", "GAMEOFTHRONES", "THRONES"});
        	kArray[3] = new KeyMatch();
        	kArray[3].setTopic("Sports");
        	kArray[3].setKeyArray(new String[]{"USOPEN", "DJOKOVIC", "WAWRINKA", "KAROLINA", "PLISKOVA", "ANGELIQUE", "KERBER", "NADAL", "FEDERER", "TENNIS"});
        	kArray[4] = new KeyMatch();
        	kArray[4].setTopic("Tech");
        	kArray[4].setKeyArray(new String[]{"IPHONE", "APPLE", "IPHONE7", "WATCH2", "APPLE EVENT", "WATCH 2"});
        	
    		int cnt = 0;
    		boolean brk = false;
    		String topic = "General";
    		
    		Pattern pattern = Pattern.compile("http(s?)://[[!#$&-;=?-\\[\\]_a-z0-9A-Z~]&&[^\\s]]+");
    		Pattern pattern2 = Pattern.compile("((\\Q:)\\E|\\Q:D\\E|\\Q:(\\E|\\Q:wink:\\E))|(?<!&)([\\x{2712}\\x{2714}\\x{2716}\\x{271d}\\x{2721}\\x{2728}\\x{2733}\\x{2734}\\x{2744}\\x{2747}\\x{274c}\\x{274e}\\x{2753}-\\x{2755}\\x{2757}\\x{2763}\\x{2764}\\x{2795}-\\x{2797}\\x{27a1}\\x{27b0}\\x{27bf}\\x{2934}\\x{2935}\\x{2b05}-\\x{2b07}\\x{2b1b}\\x{2b1c}\\x{2b50}\\x{2b55}\\x{3030}\\x{303d}\\x{1f004}\\x{1f0cf}\\x{1f170}\\x{1f171}\\x{1f17e}\\x{1f17f}\\x{1f18e}\\x{1f191}-\\x{1f19a}\\x{1f201}\\x{1f202}\\x{1f21a}\\x{1f22f}\\x{1f232}-\\x{1f23a}\\x{1f250}\\x{1f251}\\x{1f300}-\\x{1f321}\\x{1f324}-\\x{1f393}\\x{1f396}\\x{1f397}\\x{1f399}-\\x{1f39b}\\x{1f39e}-\\x{1f3f0}\\x{1f3f3}-\\x{1f3f5}\\x{1f3f7}-\\x{1f4fd}\\x{1f4ff}-\\x{1f53d}\\x{1f549}-\\x{1f54e}\\x{1f550}-\\x{1f567}\\x{1f56f}\\x{1f570}\\x{1f573}-\\x{1f579}\\x{1f587}\\x{1f58a}-\\x{1f58d}\\x{1f590}\\x{1f595}\\x{1f596}\\x{1f5a5}\\x{1f5a8}\\x{1f5b1}\\x{1f5b2}\\x{1f5bc}\\x{1f5c2}-\\x{1f5c4}\\x{1f5d1}-\\x{1f5d3}\\x{1f5dc}-\\x{1f5de}\\x{1f5e1}\\x{1f5e3}\\x{1f5ef}\\x{1f5f3}\\x{1f5fa}-\\x{1f64f}\\x{1f680}-\\x{1f6c5}\\x{1f6cb}-\\x{1f6d0}\\x{1f6e0}-\\x{1f6e5}\\x{1f6e9}\\x{1f6eb}\\x{1f6ec}\\x{1f6f0}\\x{1f6f3}\\x{1f910}-\\x{1f918}\\x{1f980}-\\x{1f984}\\x{1f9c0}\\x{3297}\\x{3299}\\x{a9}\\x{ae}\\x{203c}\\x{2049}\\x{2122}\\x{2139}\\x{2194}-\\x{2199}\\x{21a9}\\x{21aa}\\x{231a}\\x{231b}\\x{2328}\\x{2388}\\x{23cf}\\x{23e9}-\\x{23f3}\\x{23f8}-\\x{23fa}\\x{24c2}\\x{25aa}\\x{25ab}\\x{25b6}\\x{25c0}\\x{25fb}-\\x{25fe}\\x{2600}-\\x{2604}\\x{260e}\\x{2611}\\x{2614}\\x{2615}\\x{2618}\\x{261d}\\x{2620}\\x{2622}\\x{2623}\\x{2626}\\x{262a}\\x{262e}\\x{262f}\\x{2638}-\\x{263a}\\x{2648}-\\x{2653}\\x{2660}\\x{2663}\\x{2665}\\x{2666}\\x{2668}\\x{267b}\\x{267f}\\x{2692}-\\x{2694}\\x{2696}\\x{2697}\\x{2699}\\x{269b}\\x{269c}\\x{26a0}\\x{26a1}\\x{26aa}\\x{26ab}\\x{26b0}\\x{26b1}\\x{26bd}\\x{26be}\\x{26c4}\\x{26c5}\\x{26c8}\\x{26ce}\\x{26cf}\\x{26d1}\\x{26d3}\\x{26d4}\\x{26e9}\\x{26ea}\\x{26f0}-\\x{26f5}\\x{26f7}-\\x{26fa}\\x{26fd}\\x{2702}\\x{2705}\\x{2708}-\\x{270d}\\x{270f}]|\\x{23}\\x{20e3}|\\x{2a}\\x{20e3}|\\x{30}\\x{20e3}|\\x{31}\\x{20e3}|\\x{32}\\x{20e3}|\\x{33}\\x{20e3}|\\x{34}\\x{20e3}|\\x{35}\\x{20e3}|\\x{36}\\x{20e3}|\\x{37}\\x{20e3}|\\x{38}\\x{20e3}|\\x{39}\\x{20e3}|\\x{1f1e6}[\\x{1f1e8}-\\x{1f1ec}\\x{1f1ee}\\x{1f1f1}\\x{1f1f2}\\x{1f1f4}\\x{1f1f6}-\\x{1f1fa}\\x{1f1fc}\\x{1f1fd}\\x{1f1ff}]|\\x{1f1e7}[\\x{1f1e6}\\x{1f1e7}\\x{1f1e9}-\\x{1f1ef}\\x{1f1f1}-\\x{1f1f4}\\x{1f1f6}-\\x{1f1f9}\\x{1f1fb}\\x{1f1fc}\\x{1f1fe}\\x{1f1ff}]|\\x{1f1e8}[\\x{1f1e6}\\x{1f1e8}\\x{1f1e9}\\x{1f1eb}-\\x{1f1ee}\\x{1f1f0}-\\x{1f1f5}\\x{1f1f7}\\x{1f1fa}-\\x{1f1ff}]|\\x{1f1e9}[\\x{1f1ea}\\x{1f1ec}\\x{1f1ef}\\x{1f1f0}\\x{1f1f2}\\x{1f1f4}\\x{1f1ff}]|\\x{1f1ea}[\\x{1f1e6}\\x{1f1e8}\\x{1f1ea}\\x{1f1ec}\\x{1f1ed}\\x{1f1f7}-\\x{1f1fa}]|\\x{1f1eb}[\\x{1f1ee}-\\x{1f1f0}\\x{1f1f2}\\x{1f1f4}\\x{1f1f7}]|\\x{1f1ec}[\\x{1f1e6}\\x{1f1e7}\\x{1f1e9}-\\x{1f1ee}\\x{1f1f1}-\\x{1f1f3}\\x{1f1f5}-\\x{1f1fa}\\x{1f1fc}\\x{1f1fe}]|\\x{1f1ed}[\\x{1f1f0}\\x{1f1f2}\\x{1f1f3}\\x{1f1f7}\\x{1f1f9}\\x{1f1fa}]|\\x{1f1ee}[\\x{1f1e8}-\\x{1f1ea}\\x{1f1f1}-\\x{1f1f4}\\x{1f1f6}-\\x{1f1f9}]|\\x{1f1ef}[\\x{1f1ea}\\x{1f1f2}\\x{1f1f4}\\x{1f1f5}]|\\x{1f1f0}[\\x{1f1ea}\\x{1f1ec}-\\x{1f1ee}\\x{1f1f2}\\x{1f1f3}\\x{1f1f5}\\x{1f1f7}\\x{1f1fc}\\x{1f1fe}\\x{1f1ff}]|\\x{1f1f1}[\\x{1f1e6}-\\x{1f1e8}\\x{1f1ee}\\x{1f1f0}\\x{1f1f7}-\\x{1f1fb}\\x{1f1fe}]|\\x{1f1f2}[\\x{1f1e6}\\x{1f1e8}-\\x{1f1ed}\\x{1f1f0}-\\x{1f1ff}]|\\x{1f1f3}[\\x{1f1e6}\\x{1f1e8}\\x{1f1ea}-\\x{1f1ec}\\x{1f1ee}\\x{1f1f1}\\x{1f1f4}\\x{1f1f5}\\x{1f1f7}\\x{1f1fa}\\x{1f1ff}]|\\x{1f1f4}\\x{1f1f2}|\\x{1f1f5}[\\x{1f1e6}\\x{1f1ea}-\\x{1f1ed}\\x{1f1f0}-\\x{1f1f3}\\x{1f1f7}-\\x{1f1f9}\\x{1f1fc}\\x{1f1fe}]|\\x{1f1f6}\\x{1f1e6}|\\x{1f1f7}[\\x{1f1ea}\\x{1f1f4}\\x{1f1f8}\\x{1f1fa}\\x{1f1fc}]|\\x{1f1f8}[\\x{1f1e6}-\\x{1f1ea}\\x{1f1ec}-\\x{1f1f4}\\x{1f1f7}-\\x{1f1f9}\\x{1f1fb}\\x{1f1fd}-\\x{1f1ff}]|\\x{1f1f9}[\\x{1f1e6}\\x{1f1e8}\\x{1f1e9}\\x{1f1eb}-\\x{1f1ed}\\x{1f1ef}-\\x{1f1f4}\\x{1f1f7}\\x{1f1f9}\\x{1f1fb}\\x{1f1fc}\\x{1f1ff}]|\\x{1f1fa}[\\x{1f1e6}\\x{1f1ec}\\x{1f1f2}\\x{1f1f8}\\x{1f1fe}\\x{1f1ff}]|\\x{1f1fb}[\\x{1f1e6}\\x{1f1e8}\\x{1f1ea}\\x{1f1ec}\\x{1f1ee}\\x{1f1f3}\\x{1f1fa}]|\\x{1f1fc}[\\x{1f1eb}\\x{1f1f8}]|\\x{1f1fd}\\x{1f1f0}|\\x{1f1fe}[\\x{1f1ea}\\x{1f1f9}]|\\x{1f1ff}[\\x{1f1e6}\\x{1f1f2}\\x{1f1fc}])+");
    		Matcher matcher;
    		Matcher matcher2;
        	
        	while ((ln = br.readLine()) != null) {
        		cnt++;
        		if (ln.length() > 2) {
        			try {
        				post = TwitterObjectFactory.createStatus(ln.substring(1, ln.length()));
        			} catch (TwitterException tweetex) {
        				tweetex.printStackTrace();
        				System.out.println("\n!!Erronous line: \n"+ln);
        				continue;
        			}
	        		
	        		        		
	        		SimpleDateFormat sDate = new SimpleDateFormat("yyyy-MM-dd'T'hh:'00:00Z'");
	        		sDate.setTimeZone(TimeZone.getTimeZone("GMT"));
	        		
	        		if (cnt==1 && first_file) {
	        			tweet = "[{";
	        		} else {
	        			tweet = ",{";
	        		}
	        		
	        		brk = false;
	        		
	        		for (KeyMatch kIndex : kArray) {
	        			for (String str : kIndex.getKeyArray()) {
	        				if (post.getText().toUpperCase().contains(str)) {
	        					topic = kIndex.getTopic();
	    	        			brk = true;
	    	        			break;
	    	        		}
	        			}
	        			if (brk)
	        				break;
	        		}
	        		
	        		if (post.getHashtagEntities().length > 0) {
	        			HashtagEntity[] hashTag = post.getHashtagEntities();
	        			hashtag = "[";
	        			for (HashtagEntity hte : hashTag) {
		        			hashtag = hashtag + "\"" + hte.getText() + "\",";
		        		}
		        		hashtag = hashtag.substring(0, hashtag.length() - 1) + "]";
	        		} else {
	        			hashtag = "[]";
	        		}
	        		
	        		if (post.getUserMentionEntities().length > 0) {
	        			UserMentionEntity[] userMention = post.getUserMentionEntities();
	        			usermention = "[";
	        			for (UserMentionEntity uMen : userMention) {
		        			usermention = usermention + "\"" + uMen.getScreenName() + "\",";
		        		}
		        		usermention = usermention.substring(0, usermention.length() - 1) + "]";
	        		} else {
	        			usermention = "[]";
	        		}
	        		
	        		matcher = pattern.matcher(post.getText());
	        		urls = "[";
	        		while (matcher.find()) {
	        			urls = urls + "\"" + matcher.group(0) + "\",";
	        		}
	        		if (urls.length() > 1)
	        			urls = urls.substring(0, urls.length() - 1);
	        		urls = urls + "]";
	        		
	        		matcher2 = pattern2.matcher(post.getText());
	        		emoji = "[";
	        		while (matcher2.find()) {
	        			emoji = emoji + "\"" + matcher2.group(0) + "\",";
	        		}

	        		if (emoji.length() > 1)
	        			emoji = emoji.substring(0, emoji.length() - 1);
	        		emoji = emoji + "]";
	        		
	        		
	        		switch (post.getLang()) {
	        		case "en":
	        			text_en = "\"" + StringEscapeUtils.escapeJson(post.getText()) + "\"";
	        			text_es = "null";
	        			text_ko = "null";
	        			text_tr = "null";
	        			break;
	        		case "es":
	        			text_en = "null";
	        			text_es = "\"" + StringEscapeUtils.escapeJson(post.getText()) + "\"";
	        			text_ko = "null";
	        			text_tr = "null";
	        			break;
	        		case "ko":
	        			text_en = "null";
	        			text_es = "null";
	        			text_ko = "\"" + StringEscapeUtils.escapeJson(post.getText()) + "\"";
	        			text_tr = "null";
	        			break;
	        		case "tr":
	        			text_en = "null";
	        			text_es = "null";
	        			text_ko = "null";
	        			text_tr = "\"" + StringEscapeUtils.escapeJson(post.getText()) + "\"";
	        			break;
	        		default:
	        			text_en = "null";
	        			text_es = "null";
	        			text_ko = "null";
	        			text_tr = "null";
	        			break;
	        		}
	        		
	        		tweet = tweet + "\"tweet_date\":\"" + sDate.format(post.getCreatedAt()) + "\",";
	        		tweet = tweet + "\"id\":" + post.getId() + ",";
	        		tweet = tweet + "\"tweet_lang\":\"" + post.getLang() + "\",";
	        		tweet = tweet + "\"topic\":\"" + topic + "\",";
	        		tweet = tweet + "\"tweet_text\":\"" + StringEscapeUtils.escapeJson(post.getText()) + "\",";
	        		if (post.getGeoLocation() != null)
	        			tweet = tweet + "\"tweet_loc\":\"" + post.getGeoLocation().getLatitude() + "," + post.getGeoLocation().getLongitude() + "\",";
	        		else
	        			tweet = tweet + "\"coordinates\":null,";
	        		tweet = tweet + "\"hashtags\":" + hashtag + ",";
	        		tweet = tweet + "\"mentions\":" + usermention + ",";
	        		tweet = tweet + "\"tweet_urls\":" + urls + ",";
	        		tweet = tweet + "\"text_en\":" + text_en + ",";
	        		tweet = tweet + "\"text_es\":" + text_es + ",";
	        		tweet = tweet + "\"text_ko\":" + text_ko + ",";
	        		tweet = tweet + "\"text_tr\":" + text_tr + ",";
	        		tweet = tweet + "\"tweet_emoticons\":" + emoji;
	        		tweet = tweet + "}\n";
	        		
	        		
	        		//System.out.print(tweet);
	        		bw.write(tweet);
        		}
        	}
        	
        	if (last_file)
        		bw.write("]");
        	bw.flush();
        	bw.close();
        	br.close();
        	
        } catch (IOException fnf) {
        	System.out.println("Error...!"+ "    " + new Date().toString());
            fnf.printStackTrace();
            System.exit(-1);
        } catch (Exception fnf) {
        	System.out.println("Error...!"+ "    " + new Date().toString());
            fnf.printStackTrace();
            System.exit(-1);
        }
	}
}
