package crawler;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang3.SystemUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.LocalCluster;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DBContent;
import edu.upenn.cis455.storage.DBWrapper;

public class CrawlerSingle {
		
	private String m_rootUrl = null;
	private String m_DBPath = null;
	private int m_maxSize;
	private DBWrapper db;
	private Queue<String> frontier = null;
	// urls that have been searched during this search
	private Set<String> visited;
	// mapping of host to delay
	public Map<String, RobotsTxtInfo> hostRobotMapping = null;
	public Map<String, Long> hostLastCrawledMapping = null;
	private Map<String, Long> urlLastcrawledMapping = null;
	public int numFiles = 0;
	public int numFilesMax = Integer.MAX_VALUE;
	
	InetAddress monitorHost = null;
	public String monitorHostName = "cis455.cis.upenn.edu";
	DatagramSocket datagramSocket = null;
	
	public int downloaded = 0;
	public int xmlDownloaded = 0;
	public int htmlDownloaded = 0;
	private XPathCrawlerTools tools;
	
	
	// MS2
	private static final String CRAWL_SPOUT = "CRAWL_SPOUT";
    private static final String CRAWL_BOLT = "CRAWL_BOLT";
    private static final String DOC_BOLT = "DOCUMENT_BOLT";
    private static final String URL_BOLT = "URL_BOLT";
	
	public CrawlerSingle() {
	}
	
	// 3 args
	public CrawlerSingle(String rootUrl, String DBPath, int maxSize) {
		this.m_rootUrl = rootUrl;
		this.m_DBPath = DBPath;
		this.m_maxSize = maxSize;
		db = DBWrapper.getInstance(DBPath);
		setup();
		setMonitorHost();
	}
	// 4 args with host
	public CrawlerSingle(String rootUrl, String DBPath, int maxSize, String host) {
		this.m_rootUrl = rootUrl;
		this.m_DBPath = DBPath;
		this.m_maxSize = maxSize;
		db = DBWrapper.getInstance(DBPath);
		setup();
		this.monitorHostName = host;
		setMonitorHost();
	}
	// 4 args with numfiles
	public CrawlerSingle(String rootUrl, String DBPath, int maxSize, int numFilesMax) {
		this.m_rootUrl = rootUrl;
		this.m_DBPath = DBPath;
		this.m_maxSize = maxSize;
		db = DBWrapper.getInstance(DBPath);
		setup();
		this.numFilesMax = numFilesMax;
		setMonitorHost();
	}
	// 5 args
	public CrawlerSingle(String rootUrl, String DBPath, int maxSize, int numFilesMax, String host) {
		this.m_rootUrl = rootUrl;
		this.m_DBPath = DBPath;
		this.m_maxSize = maxSize;
		db = DBWrapper.getInstance(DBPath);
		frontier = new LinkedList<>();
		setup();
		this.numFilesMax = numFilesMax;
		this.monitorHostName = host;
		setMonitorHost();
	}
	// 5 args
	public CrawlerSingle(String rootUrl, String DBPath, int maxSize,  String host, int numFilesMax) {
		this.m_rootUrl = rootUrl;
		this.m_DBPath = DBPath;
		this.m_maxSize = maxSize;
		db = DBWrapper.getInstance(DBPath);
		frontier = new LinkedList<>();
		setup();
		this.numFilesMax = numFilesMax;
		this.monitorHostName = host;
		setMonitorHost();
	}	
	
	public void setup() {
		frontier = new LinkedList<>();
		visited = new HashSet<>();
		hostRobotMapping = new HashMap<>();
		hostLastCrawledMapping = new HashMap<>();
		urlLastcrawledMapping = new HashMap<>();
		tools = new XPathCrawlerTools();
	}
	
	public void setMonitorHost() {
//		try {
//			this.monitorHost = InetAddress.getByName(this.monitorHostName);
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		}
//		try {
//			this.datagramSocket = new DatagramSocket();
//		} catch (SocketException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
			
	public void crawl() {
		Queue<String> front = this.frontier;
		System.out.println();
		front.offer(m_rootUrl);
		while (!frontier.isEmpty()) {
			if (numFiles >= numFilesMax) break;
			String cur = front.poll();
			URLInfo curUrlInfo = new URLInfo(cur);
			cur = curUrlInfo.getNormalizedUrl();
//			System.out.println("cur url:****** " + cur);
			String protocol = curUrlInfo.getProto();
			String host = curUrlInfo.getHostName();
			String robotUrl = protocol + host + "/robots.txt";
			RobotsTxtInfo robotInfo;
			long lastCrawled;
			// The crawler must be careful not to search the same page multiple times during a given crawl
			// first thing first check this condition using a visited set 
			if (visited.contains(cur)) {
				System.out.println(cur + " has been searched during this crawl");
				continue;
			}
			// mark page as searched
			// generate new or get generated robot.txt
			if (hostRobotMapping.containsKey(host)) {
				robotInfo = hostRobotMapping.get(host);
				lastCrawled = hostLastCrawledMapping.containsKey(host)? hostLastCrawledMapping.get(host): -1;
			} else {
				robotInfo = new RobotsTxtInfo();
				BufferedReader robotTxtBr = tools.requestRobotFile(robotUrl);
				if (robotTxtBr != null) {
					tools.parseRobotFile(robotTxtBr, robotInfo);
				}
				if (robotInfo != null) hostRobotMapping.put(host, robotInfo);
				lastCrawled = -1;
			}
			// check if url is disallowed, return false means didn't pass checking, is disallowed
			if (!isAllowed(curUrlInfo, robotInfo)) {
				System.out.println(cur + ": Not Allowed!!!");
				continue;
			}
			// check if the url should wait more time
			if (shouldWait(robotInfo, lastCrawled)) {
//				System.out.println("should wait");
				front.offer(cur);
				continue;
			}
			lastCrawled = System.currentTimeMillis();
			hostLastCrawledMapping.put(host, lastCrawled);
			visited.add(cur);
			HttpClient client = new HttpClient(cur);
			// process request and response
			numFiles++;
			monitor(cur);
			if (!client.requestAndParseResponse("head")) continue;
			// handle redirect
			if (client.getStatusCode() == 301) {
				// if we get a 301 redirect response, should update url, URLInfo and HttpClient and check head again
				String redirectUrl = client.getRedirectUrl();
				cur = redirectUrl;
				curUrlInfo = new URLInfo(cur);
				// check if this url is allowed to crawl
				if (!isAllowed(curUrlInfo, robotInfo)) {
					System.out.println(cur + ": Not Allowed!!!");
					continue;
				}
				if (shouldWait(robotInfo, lastCrawled)) {
					// enqueue to the end of queue
					front.offer(cur);
					continue;
				}
				client = new HttpClient(cur);
				monitor(cur);
				if (!client.requestAndParseResponse("head")) continue;
			}
			// check status code, types and content lenght
			if (!sanityCkeck(client, cur)) continue;
			// check if file has crawled or has been modified since last crawled
			DBContent page = db.getContent(cur);
			if (!shouldCrawl(client, page, cur)) {
				System.out.println(cur + ": Not modified");
				// should still extra links even if we don't need to crawl the page
				if (client.getContentType().contains("html")) {
					String content = page.getContent();
					fetchLinksFromDB(content, cur);
				}
				continue;
			}
			// send get request and parse response with content body
			monitor(cur);
			if (!client.requestAndParseResponse("get")) {
				System.out.println(cur + ": Not Fetchable");
				continue;
			}
			// update last crawled time
//			lastCrawled = System.currentTimeMillis();
			// store page content
			System.out.println(cur + ": Downloading");
//			System.out.println(client.getContentLength() + ", " + client.getDownledContentSize());
			downloaded++;
			if (client.getContentType().contains("html")) htmlDownloaded++;
			else if (client.getContentType().contains("xml")) xmlDownloaded++;
			storeContent(client, cur, lastCrawled);
//			robotInfo.updateLastAccessed();
			// update last crawled time for this host
//			hostLastCrawledMapping.put(host, lastCrawled);
			// fetch links
			if (client.getContentType().contains("html")) {
				fetchLinksFromWeb(cur);
			}
			db.sync();
//			System.out.println("downloaded: " + downloaded);
		}
		System.out.println(numFiles + " files retrived");
		System.out.println(downloaded + " files downloaed");
		System.out.println(xmlDownloaded + " xml files downloaed");
		System.out.println(htmlDownloaded + " html files downloaed");
		db.close();
	}
	
	// store the crawled content into database
	public void storeContent(HttpClient client, String url, long lastCrawled) {
		String contentBody = client.getBody();
		// see if database has it first
		DBContent page = db.getContent(url);
		// if no, new a DBContent
		if (page == null) {
			page = new DBContent(url, contentBody, lastCrawled);
			page.setContentType(client.getContentType());
		}
//		else System.out.println("database already has contents for " + url);
		page.setContentType(client.getContentType());
//		page.setLastAccessed(lastCrawled);
		db.setContent(page);
	}
	
	// fetch reference links from a page
	public void fetchLinksFromWeb(String url) {
		// https://github.com/jhy/jsoup
		try {
			Document doc = Jsoup.connect(url).get();
			Elements links = doc.select("a[href]");
			for (Element link : links){
				// store absolute url
//				System.out.println("   " + link.attr("abs:href"));
				this.frontier.offer(link.attr("abs:href"));
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	// fetch links from local database
	public void fetchLinksFromDB(String content, String cur) {
		// https://github.com/jhy/jsoup
		try {
			Document doc = Jsoup.parse(content, cur);
			Elements links = doc.select("a[href]");
			for (Element link : links){
				// store absolute url
//				System.out.println("    " + link.attr("abs:href"));
				this.frontier.offer(link.attr("abs:href"));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
	
	// this function checks status code, content type and see if content size is larger that maxsize
	// snity check for information provied by head response
	public boolean sanityCkeck(HttpClient client, String url) {
		// check status code
		int statusCode = client.getStatusCode();
		if (statusCode != 200) return false;
		// check content type
		String contentType = client.getContentType();
		// content type can not be null
		if (contentType == null) return false;
		// content type can only be one of following
		if (!contentType.equals("text/html") && !contentType.equals("text/xml") 
				&& !contentType.equals("application/xml") && !contentType.endsWith("+xml")) return false;
		// check content length
		int contentLen = client.getContentLength();
		// get crawled size of content-size not available
		if (contentLen == -1) contentLen = client.getDownledContentSize();
//		System.out.println("contentLen: " + contentLen);
		int maxBytes = m_maxSize * 1024 * 1024;
//		System.out.println("maxBytes: " + maxBytes);
		if (contentLen > maxBytes) {
			System.out.println(url + ": has size of " + contentLen + ", Over max size " + maxBytes);
			return false;
		}
		return true;
	}
	
	// 1 check if the file has been crawled yet
	// 2 if yes check if the file has been modified since last crawled
	// crawl this page if return true
	public boolean shouldCrawl(HttpClient client, DBContent page, String url) {
		// if page is null, we don't have this page yet, crawl it, return true
		if (page == null) {
//			System.out.println(url + " not in database yet, should crawl it");
			return true;
		}
		long lastCrawled = page.getLastAccessed();
		// get the last modified time of the file based on last-modified header
		long lastModified = client.getLastModified();
		if (lastModified == -1) {
//			System.out.println("can't find last modified, should crawl " + url);
			return true;
		}
		if (lastModified > lastCrawled) {
//			System.out.println(url + " has been modified since last crawled, should crawl");
			return true;
		}
		return false;
	}
	
	// check if more time should be waited before crawling this host again based on crawl-delay information
	public boolean shouldWait(RobotsTxtInfo robotInfo, long hostLastCrawled) {
		// no robot found, just crawl the url
		if (robotInfo == null) {
			return false;
		}
		// this host not crawled yet
		if (hostLastCrawled == -1) {
			return false;
		}
		long waitTime = System.currentTimeMillis() - hostLastCrawled;
		// check if delay is specified
		// if not specified, shouldn't wait
		if (!robotInfo.crawlContainAgent("cis455crawler") && !robotInfo.crawlContainAgent("*")) {
			return false;
		}
		// check if has waited the required delay time
		int lastCrawl = robotInfo.crawlContainAgent("cis455crawler")? robotInfo.getCrawlDelay("cis455crawler"): robotInfo.getCrawlDelay("*");
		long lastCrawlLong = lastCrawl * 1000;
		// haven't wated enough time since the host was last crawled, this url should wait
		if (waitTime < lastCrawlLong) return true;
		return false;
	}
	
	// check if this page is allowed to crawl
	public boolean isAllowed(URLInfo curUrlInfo, RobotsTxtInfo robotInfo) {
		// get this file path part with host part
		String url = curUrlInfo.getFilePath();
		ArrayList<String> disallowedLinks = null;
		if (robotInfo.containsUserAgent("cis455crawler") || robotInfo.containsUserAgent("*")) {
			disallowedLinks = robotInfo.containsUserAgent("cis455crawler")? robotInfo.getDisallowedLinks("cis455crawler"): robotInfo.getDisallowedLinks("*");
		}
		// doesn't specify disallow, we can crawl this page
		if (disallowedLinks == null) return true;
		for (String link: disallowedLinks) {
			// link: /, we are not allowed to crawl any page in this site
			if (link.equals("/")) return false;
			// remove '/' at the tail
			if (link.endsWith("/")) link = link.substring(0, link.length()-1);
			if (url.startsWith(link)) {
				return false;
			}
		}
		// check through all disallowed links, pass all of them
		return true;
	}
	
	public void monitor(String url) {
		/* The commands below need to be run for every single URL */
		byte[] data = ("ziyuchen;" + url).getBytes();
		DatagramPacket packet = new DatagramPacket(data, data.length, this.monitorHost, 10455);
		try {
			this.datagramSocket.send(packet);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// get stats for crawling
	public int getXmlDownloaded() {
		return this.xmlDownloaded;
	}
	
	public int getDownloaded() {
		return this.downloaded;
	}
	
	public int getHtmlDownloaded() {
		return this.htmlDownloaded;
	}	
	
	public int getNumRetrived() {
		return this.numFiles;
	}	


	public static void main(String args[]) {
		
//		String rootUrl;
//		String dbUrl;
//		int maxSize = 100;
//		int numfile = -1;
//		String host = "cis455.cis.upenn.edu";
//		
//		if (args.length < 3) {
//			System.out.println("not enough arguments to start the crawler");
//			return;
//		}
//		
//		if (args.length == 3) {
//			rootUrl = args[0];
//			dbUrl = args[1];
//			try {
//				maxSize = Integer.parseInt(args[2]);
//			}catch(Exception e) {
//				maxSize = 100;
//			}
//			XPathCrawler xc = new XPathCrawler(rootUrl, dbUrl, maxSize);
//			xc.crawl();
//		} 
//		
//		else if (args.length == 4) {
//			rootUrl = args[0];
//			dbUrl = args[1];
//			try {
//				maxSize = Integer.parseInt(args[2]);
//			}catch(Exception e) {
//				maxSize = 100;
//			}
//			try {
//				numfile = Integer.parseInt(args[3]);
//			}catch(Exception e) {
//				numfile = 1000;
//			}
//			XPathCrawler xc = new XPathCrawler(rootUrl, dbUrl, maxSize, numfile);
//			xc.crawl();
//			
//		}
//		
//		else if (args.length == 5) {
//			rootUrl = args[0];
//			dbUrl = args[1];
//			try {
//				maxSize = Integer.parseInt(args[2]);
//			}catch(Exception e) {
//				maxSize = 100;
//			}
//			try {
//				numfile = Integer.parseInt(args[3]);
//			}catch(Exception e) {
//				numfile = 1000;
//			}
//			host = args[4];
//			XPathCrawler xc = new XPathCrawler(rootUrl, dbUrl, maxSize, numfile, host);
//			xc.crawl();
//		}
		/* TODO: Implement crawler */
//		XPathCrawler xc = new XPathCrawler("https://docs.oracle.com/en/cloud", "database", 5, 100);
		
		XPathCrawler xc = new XPathCrawler("http://crawltest.cis.upenn.edu/", "database", 5);
//		xc.crawl();

	}
	
}
