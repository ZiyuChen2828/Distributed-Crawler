package crawlerUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;


// this is a helper class for doing some independent work for the crawler
public class CrawlerHelper {

	// this function requst and featch the robots.txt from the host
	public BufferedReader requestRobotFile(String requesturl) {
		URL url = null;
		InputStreamReader reader = null;
		BufferedReader br = null;
		try {
			url = new URL(requesturl);
			reader = new InputStreamReader(url.openStream());
			br = new BufferedReader(reader);
		} catch (Exception e) {
			// unable to request the robot page
			e.printStackTrace();
			return null;
		}
		return br;
	}
	
	// after fetching the file into bufferreader
	// this function parse the contents into a RobotsTxtInfo object
	public void parseRobotFile(BufferedReader inbuff, RobotsInfo robotInfo) {
		int flag = -1;
		// keep check of agent name for the current block
		String agent = null;
		try {
			String line = inbuff.readLine();
			while(line != null) {
				// this is a comment line or empty line
				if (line.isEmpty() || line.startsWith("#")) {
					// read next line
					line = inbuff.readLine();
					continue;
				}
				// split the line on ':' into 2 parts
				String[] lineSplit = line.split(":", 2);
				if (lineSplit.length != 2) continue;
				// lower case
				String type = lineSplit[0].trim().toLowerCase();
				String val = lineSplit[1].trim();
				
				// handle comments
				// User-agent: BadBot # replace 'BadBot' with the actual user-agent of the bot
				// Disallow: / # keep them out
				if (val.contains("#")) {
					val = val.substring(0, val.indexOf('#')).trim();
				}
				if (type.equals("user-agent")) {

					robotInfo.addUserAgent(val);
					agent = val;
				} else {
					if (agent == null) {
						// invalid robot text, did not specify agent first
						robotInfo = null;
						break;
					}
					if (type.equalsIgnoreCase("disallow")) {
						// ignore empty
						if (!val.equals("")) {
							// ignore wilde card
							if (!val.contains("*")) {
								robotInfo.addDisallowedLink(agent, val);
							}
						}

					}
					else if (type.equalsIgnoreCase("sitemap")) robotInfo.addSitemapLink(val);
					else if (type.equalsIgnoreCase("allow")) robotInfo.addAllowedLink(agent, val);
					else if (type.equalsIgnoreCase("crawl-delay")) {
						Integer delay = Integer.parseInt(val);
						robotInfo.addCrawlDelay(agent, delay);
					}
				}

				line = inbuff.readLine();
			}
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		String s = "User-agent: BadBot# replace 'BadBot' with the actual user-agent of the bot";
		String[] lineSplit = s.split(":", 2);
//		System.out.println(lineSplit.length);
//		System.out.println(lineSplit[1].trim().equals(""));
		String type = lineSplit[0].trim().toLowerCase();
		String val = lineSplit[1].trim();
		if (val.contains("#")) {
			val = val.substring(0, val.indexOf('#')).trim();
		}
		System.out.println(val);
	}

}
