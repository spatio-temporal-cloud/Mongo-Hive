package synchronization;

import java.util.*;
import java.util.regex.Pattern;
import java.io.*;
import java.text.*;

import com.mongodb.*;

import org.bson.BSONObject;
import org.bson.types.*;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.DeviceType;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;


public class MongodbSynchronization {
	public static MongoClient mongoClient = null;
	public static DB db = null;
	public static DBCollection leadsColl = null;
	public static DBCollection sessionsColl = null;
	public static DBCollection ordersColl = null;
	public static DBCollection apiCallsColl = null;
	public static DBCollection errorsColl = null;
	public static DBCollection qualsColl = null;
	public static DBCollection customersColl = null;	
	public static DBCollection visitsColl = null;
	public static DBCollection errorCodesColl = null;
	
	public static BufferedWriter sessionsOutput = null;
	public static BufferedWriter leadsOutput = null;
	public static BufferedWriter ordersOutput = null;
	public static BufferedWriter apiCallsOutput = null;
	public static BufferedWriter errorsOutput = null;
	public static BufferedWriter qualsOutput = null;
	public static BufferedWriter customersOutput = null;
	public static BufferedWriter visitsOutput = null;
	
	public static File sessionsFile = null;
	public static File leadsFile = null;
	public static File ordersFile = null;
	public static File apiCallsFile = null;
	public static File errorsFile = null;
	public static File qualsFile = null;
	public static File customersFile = null;
	public static File visitsFile = null;
	
	
	public static String startString = null;
	public static String endString = null;
	public static Date start = null;
	public static Date end = null;
	
	public static String fileName = null;
	public static String outputFileDest = null;
	public static String filePrefix = null;
	public static BasicDBObject query = null;
	public static SimpleDateFormat formatter = null;
	public static Map<String, String> errorCodes = null;

	
	
	public static void prepareConnections(String propPath) {
		
		if (propPath == null)
		propPath = "/home/zengmingyu/scripts/testprop.properties";
		
		String mongodbServerIP = null;
		int port = -1;
		String leadsCollectionName = null;
		String ordersCollectionName = null;
		String sessionsCollectionName = null;
		String apicallsCollectionName = null;
		String errorsCollectionName = null;
		String qualsCollectionName = null;
		String customersCollectionName = null;
		String visitsCollectionName = null;
		String errorCodesCollectionName = null;
		
		String databaseName = null;
		
		try { 
			FileInputStream fis = new FileInputStream(propPath);

			Properties properties = new Properties();
			properties.load(fis);
			mongodbServerIP = properties.getProperty("mongodb_server_ip", "127.0.0.1");
			port = Integer.valueOf(properties.getProperty("mongodb_port", "27017"));
			databaseName = properties.getProperty("database_name", "maize");
			leadsCollectionName = properties.getProperty("leads_collection_name", "leads");
			ordersCollectionName = properties.getProperty("orders_collection_name", "orders");
			sessionsCollectionName = properties.getProperty("sessions_collection_name", "sessions");
			apicallsCollectionName = properties.getProperty("apicalls_collection_name", "apicalls");
			errorsCollectionName = properties.getProperty("errors_collection_name", "errors");
			qualsCollectionName = properties.getProperty("quals_collection_name", "quals");
			customersCollectionName = properties.getProperty("customers_collection_name", "customers");
			visitsCollectionName = properties.getProperty("visits_collection_name", "visits");
			errorCodesCollectionName = properties.getProperty("errorCodes_collection_name", "errorCodes");
		    outputFileDest = properties.getProperty("output_file_dest", "/home/zengmingyu/data/");
		    
		    File dir = new File(outputFileDest);
		    if (!dir.exists())
		    	dir.mkdirs();


			fis.close();
		} catch (FileNotFoundException e) 
		{
			System.out.println("properties file not found");
			System.exit(0);
		} catch (IOException e) 
		{
			System.out.println("FileiInputStream close exception");
		}
			
		
	    /*mongodbServerIP = "localhost";
		port = 27017;
		databaseName = "maize";
		sessionsCollectionName = "sessions";
		leadsCollectionName = "leads";
		ordersCollectionName = "orders";
		apicallsCollectionName = "apicalls";
		errorsCollectionName = "errors";
		qualsCollectionName = "quals";
		customersCollectionName = "customers";
		visitsCollectionName = "visits";
		errorCodesCollectionName = "errorCodes";
		
		System.out.println("monogodbServerIP = " + mongodbServerIP);
		System.out.println("port = " + port);
		System.out.println("databaseName = " + databaseName);
		System.out.println("leadsCollectionName = " + leadsCollectionName);
		System.out.println("sessionsCollectionName = " + sessionsCollectionName);
		System.out.println("ordersCollectionName = " + ordersCollectionName);
		System.out.println("apicallsCollectionName = " + apicallsCollectionName);
		System.out.println("errorsCollectionName = " + errorsCollectionName);
		System.out.println("qualsCollectionName = " + qualsCollectionName);
		System.out.println("customersCollectionName = " + customersCollectionName);
		System.out.println("visitsCollectionName = " + visitsCollectionName);
		System.out.println("errorCodesCollectionName = " + errorCodesCollectionName);
		System.out.println("outputFileDest = " + outputFileDest);*/

	    

		





		try {
		 mongoClient = new MongoClient(mongodbServerIP, port);
		} catch (Exception e) {
			System.out.println("problems occurred connecting host");
			System.exit(0);
		}
		
		
		db = mongoClient.getDB(databaseName);
	
		
	
		
		if (db.collectionExists(leadsCollectionName) == true) 
		{
			leadsColl = db.getCollection(leadsCollectionName);

		} else {		
			System.out.println(" leads collection doesn't exist");
			System.exit(0);
		}
		
		 		
		if ( db.collectionExists(sessionsCollectionName) == true ) 
		{
			 sessionsColl = db.getCollection(sessionsCollectionName);
		} else {
			System.out.println(" sessions collection doesn't exist");
			System.exit(0);
		}
 		
	
	 
	 if (db.collectionExists(ordersCollectionName) == true) 
	 {
		 ordersColl = db.getCollection(ordersCollectionName);
	 } else {
		 System.out.println(" orders collection doesn't exist");
			System.exit(0);
	 }
	 

	 if (db.collectionExists(errorsCollectionName) == true) 
	 {
		 errorsColl = db.getCollection(errorsCollectionName);
	 } else {
		 System.out.println(" errors collection doesn't exist");
			System.exit(0);
	 }
	 

	 if (db.collectionExists(apicallsCollectionName) == true) 
	 {
		 apiCallsColl = db.getCollection(apicallsCollectionName);
	 } else {
		 System.out.println(" apicalls collection doesn't exist");
			System.exit(0);
	 }
	 

	 if (db.collectionExists(qualsCollectionName) == true) 
	 {
		 qualsColl = db.getCollection(qualsCollectionName);
	 } else {
		 System.out.println(" quals collection doesn't exist");
			System.exit(0);
	 }
	 

	 if (db.collectionExists(customersCollectionName) == true) 
	 {
		 customersColl = db.getCollection(customersCollectionName);
	 } else {
		 System.out.println(" customers collection doesn't exist");
			System.exit(0);
	 }
	 
	 if (db.collectionExists(visitsCollectionName) == true) 
	 {
		 visitsColl = db.getCollection(visitsCollectionName);
	 } else {
		 System.out.println(" visits collection doesn't exist");
			System.exit(0);
	 }
	 
	 if (db.collectionExists(errorCodesCollectionName) == true) 
	 {
		 errorCodesColl = db.getCollection(errorCodesCollectionName);
	 } else {
		 System.out.println(" errorCodes collection doesn't exist");
			System.exit(0);
	 }
	 
		
		
	    
	}

	public static void getFromSessions()
	{
		DBCursor result = sessionsColl.find(query);
	
		//DBCursor result = sessionsColl.find();
		
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				//if (count == 501)
					//break;
				StringBuilder sessionBuilder = processSession(result.next());
				sessionsOutput.write(sessionBuilder.toString() + "\n");
				//System.out.print(sessionBuilder.toString() + "\n");
			}
			
			sessionsOutput.flush();
			sessionsOutput.close();
			System.out.println("session count = " + count);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			result.close();
		}
		
		
		
		
	}
	
	public static void getFromCustomers()
	{
		DBCursor result = customersColl.find(query);
		//DBCursor result = customersColl.find();
		
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				//if (count == 501)
					//break;
				StringBuilder customerDBObject = processCustomer(result.next());
				customersOutput.write(customerDBObject.toString() + "\n");
				//System.out.print(customerDBObject.toString() + "\n");
				
			}
			customersOutput.flush();
			customersOutput.close();
			System.out.println("customer count = " + count);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			result.close();
		}
		//DBObject customerTmp = customersColl.findOne();
		//System.out.println(processCustomer(customerTmp).toString());
	
		
	}
	
	public static void getFromApiCalls()
	{
	    DBCursor result = apiCallsColl.find(query);
		//DBCursor result = apiCallsColl.find();
			
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				//if (count == 501)
					//break;
				StringBuilder apiCallsDBObject = processApiCall(result.next());
				apiCallsOutput.write(apiCallsDBObject.toString() + "\n");
				//System.out.print(apiCallsDBObject.toString() + "\n");
			}
			apiCallsOutput.flush();
			apiCallsOutput.close();
			System.out.println("apicall count = " + count);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			result.close();
		}
		
		//DBObject apiCallTmp = apiCallsColl.findOne();
		//System.out.println(processApiCall(apiCallTmp).toString());
	
		
	}
	
	public static void getFromQuals()
	{
		DBCursor result = qualsColl.find(query);
		//DBCursor result = qualsColl.find();
			
		try {
		    int count = 0;
			while (result.hasNext())
			{
				count++;
				//if (count == 501)
					//break;
				StringBuilder qualDBObject = processQual(result.next());
				qualsOutput.write(qualDBObject.toString() + "\n");
				//System.out.print(qualDBObject.toString() + "\n");
				
			}
			qualsOutput.flush();
			qualsOutput.close();
			System.out.println("qual count = " + count);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			result.close();
		}
	
		//DBObject qualTmp = qualsColl.findOne();
		//System.out.println(processQual(qualTmp).toString());
	}
	
	public static void getFromErrors() 
	{
		DBCursor result = errorsColl.find(query);
		//DBCursor result = errorsColl.find();
		
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				//if (count == 501) 
					//break;
				StringBuilder errorDBObject = processError(result.next());
				errorsOutput.write(errorDBObject.toString() + "\n");
				//System.out.print(errorDBObject.toString() + "\n");
			}
			errorsOutput.flush();
			errorsOutput.close();
			System.out.println("error count = " + count);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			result.close();
		}
	
		//DBObject errorTmp = errorsColl.findOne();
		//System.out.println(processError(errorTmp).toString());
		
	
	
		
	}

	public static void getFromOrders() 
	{
		DBCursor result = ordersColl.find(query);
		//DBCursor result = ordersColl.find();
			
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				//if (count == 501)
					//break;
				StringBuilder orderDBObject = processOrder(result.next());
				ordersOutput.write(orderDBObject.toString() + "\n");
				//System.out.print(orderDBObject.toString() + "\n");
			}
			ordersOutput.flush();
			ordersOutput.close();
			System.out.println("order count = " + count);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			result.close();
		}
	
		
		//DBObject orderTmp = ordersColl.findOne();
		//System.out.println(processOrder(orderTmp).toString());
		
	
		
	}
	
	public static void getFromLeads() 
	{
		DBCursor result = leadsColl.find(query);
		//DBCursor result = leadsColl.find();
		
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				//if (count == 501)
					//break;
				StringBuilder leadDBObject = processLead(result.next());
				leadsOutput.write(leadDBObject.toString() + "\n");
				//System.out.print(leadDBObject.toString() + "\n");
			}
			leadsOutput.flush();
			leadsOutput.close();
			System.out.println("lead count = " + count);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			result.close();
		}
	
		
	
	
		//DBObject leadTmp = leadsColl.findOne();
		//System.out.println(processLead(leadTmp).toString());
		
	}

	public static void getFromVisits() 
	{
		DBCursor result = visitsColl.find(query);
		//DBCursor result = visitsColl.find();
		
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				//if (count == 501) 
					//break;
				StringBuilder visitDBObject = processVisit(result.next());
				visitsOutput.write(visitDBObject.toString() + "\n");
				//System.out.print(visitDBObject.toString() + "\n");
			}
			visitsOutput.flush();
			visitsOutput.close();
			System.out.println("visit count = " + count);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			result.close();
		}
	
		//DBObject errorTmp = errorsColl.findOne();
		//System.out.println(processError(errorTmp).toString());
		
	
	
		
	}



	
	
	public static StringBuilder processSession(DBObject session)
	{
		StringBuilder output = new StringBuilder();
		
		String userAgentString = null;
		if (session != null && session.containsField("userAgent") && 
				session.get("userAgent") != null)
			userAgentString = (String) session.get("userAgent");
		BasicDBObject userAgentDBObject = parseUserAgent(userAgentString);
		
		String userAgent_operatingSystem  = null;
		if (userAgentDBObject != null && userAgentDBObject.containsField("operatingSystem") && 
				userAgentDBObject.get("operatingSystem") != null)
			userAgent_operatingSystem = userAgentDBObject.getString("operatingSystem");
		
		String userAgent_browser  = null;
		if (userAgentDBObject != null && userAgentDBObject.containsField("browser") && 
				userAgentDBObject.get("browser") != null)
			userAgent_browser = userAgentDBObject.getString("browser");
		
		String userAgent_deviceType  = null;
		if (userAgentDBObject != null && userAgentDBObject.containsField("deviceType") && 
				userAgentDBObject.get("deviceType") != null)
			userAgent_deviceType = userAgentDBObject.getString("deviceType");
		
		String userAgent_mobileDevice  = null;
		if (userAgentDBObject != null && userAgentDBObject.containsField("mobileDevice") && 
				userAgentDBObject.get("mobileDevice") != null)
			userAgent_mobileDevice = userAgentDBObject.getString("mobileDevice");
		
		
		StringBuilder apiCalls  = new StringBuilder();
		if (session != null && session.containsField("apiCalls") && 
				session.get("apiCalls") != null &&
				session.get("apiCalls") instanceof ArrayList)
		{
			ArrayList<ObjectId> apiCalls1 = (ArrayList<ObjectId>) session.get("apiCalls");
			if (!apiCalls1.isEmpty())
			{
				for (ObjectId apiCall : apiCalls1)
					apiCalls.append(apiCall.toString() + '\002');
				apiCalls.deleteCharAt(apiCalls.length() - 1);
			}
//			add else block on 30th,DEC,2013
			else
				apiCalls.append("\\N");
		}

		
		Date dateTime = null;
		if (session != null && session.containsField("dateTime") && 
				session.get("dateTime") != null 
				&& session.get("dateTime") instanceof Date)
			dateTime = (Date) session.get("dateTime");	
		
		String date_time = null;
		if (dateTime != null)
		{
			//dateTime.setHours(dateTime.getHours() - 8);
			date_time = formatter.format(dateTime);
			//System.out.println(dateTime.toString());
			//System.out.println(date_time + '\n');
		}
		String type = "SESSION";
		if (session != null && session.containsField("type") && 
				session.get("type") != null)
			type = (String) session.get("type");
		
		String session_id = null; 
		if (session != null && session.containsField("_id") && 
				session.get("_id") != null)
			session_id = session.get("_id").toString();
//		add else block on 30th,DEC,2013
		else 
			session_id="\\N";
		
		String lead_id = null;
		if (session != null && session.containsField("lead") && 
				session.get("lead") != null)
			lead_id = session.get("lead").toString();
//		add else block on 30th,DEC,2013
		else
			lead_id="\\N";

		String visit_id = null;
		if (session != null && session.containsField("visit") && session.get("visit") != null)
			visit_id = session.get("visit").toString();
//		add else block on 30th,DEC,2013
		else 
			visit_id="\\N";
		
		String order_id = null;
		if (session != null && session.containsField("order") && session.get("order") != null)
			order_id = session.get("order").toString();
//		add else block on 30th,DEC,2013
		else
			order_id="\\N";
		
		boolean ordered = false;
		if (order_id != null)
			ordered = true;
		
		
		StringBuilder quals  = new StringBuilder();
		if (session != null && session.containsField("quals") && session.get("quals") != null && 
				session.get("quals") instanceof ArrayList)
		{
			ArrayList<ObjectId> quals1 = (ArrayList<ObjectId>) session.get("quals");
			if (!quals1.isEmpty())
			{
				for (ObjectId qual : quals1)
					quals.append(qual.toString() + '\002');
				quals.deleteCharAt(quals.length() - 1);
			}
//			add else block on 30th,DEC,2013
			else
				quals.append("\\N");
		}
		
		StringBuilder errs  = new StringBuilder();
		if (session != null && session.containsField("errs") && session.get("errs") != null &&
				session.get("errs") instanceof ArrayList)
		{
			ArrayList<ObjectId> errs1 = (ArrayList<ObjectId>) session.get("errs");
			if (!errs1.isEmpty())
			{
				for (ObjectId err : errs1)
					errs.append(err.toString() + '\002');
				errs.deleteCharAt(errs.length() - 1);
			}
//			add else block on 30th,DEC,2013
			else
				errs.append("\\N");
		}
	
		
		
		
		DBObject agent = new BasicDBObject();
		String agent_id = null;
		String agent_firstName = null;
		String agent_lastName = null;
		String agent_callCenterName = null;
		StringBuilder agentStr = new StringBuilder();
		int callCenter = 0;
		if (session != null && session.containsField("agent") && session.get("agent") != null &&
				session.get("agent") instanceof DBObject)
		{
			agent = (DBObject) session.get("agent");
			callCenter = 1;
		}	
		if (agent.containsField("id") && agent.get("id") != null)
			agent_id = agent.get("id").toString(); 
		if (agent.containsField("firstName") && agent.get("firstName") != null)
			agent_firstName = agent.get("firstName").toString(); 
		if (agent.containsField("lastName") && agent.get("lastName") != null)
			agent_lastName = agent.get("lastName").toString(); 
		if (agent.containsField("callCenter") && agent.get("callCenter") != null)
			agent_callCenterName = agent.get("callCenter").toString(); 
		
		agentStr.append(agent_firstName + '_');
		agentStr.append(agent_lastName );
		agentStr.append("(" + agent_id + ")");
	
		
		output.append(userAgent_operatingSystem + '\001');
		output.append(userAgent_browser + '\001');
		output.append(userAgent_deviceType + '\001');
		output.append(userAgent_mobileDevice + '\001');
		output.append(apiCalls.toString() + '\001');
		output.append(date_time + '\001');
		output.append(type + '\001');
		output.append(session_id + '\001');
		output.append(lead_id + '\001');
		output.append(visit_id + '\001');
		output.append(ordered);
		output.append('\001');
		output.append(order_id + '\001');
		output.append(quals.toString() + '\001');
		output.append(errs.toString() + '\001');
		output.append(callCenter);
		output.append('\001');
		output.append(agentStr.toString() + '\001');
		output.append(agent_callCenterName);
		
	
		
		
		return output;
	}
	
	public static StringBuilder processCustomer(DBObject customer)
	{
		StringBuilder output = new StringBuilder();
		
		String type = "CUSTOMER";
		if (customer != null && customer.containsField("type") &&
				customer.get("type") != null)
			type = (String) customer.get("type");
		
		Date dateTime = null;
		if (customer != null && customer.containsField("dateTime") && 
				customer.get("dateTime") != null &&
				customer.get("dateTime") instanceof Date)
			dateTime = (Date) customer.get("dateTime");	
		
		String date_time = null;
		if (dateTime != null)
		{
			//dateTime.setHours(dateTime.getHours() - 8);
			date_time = formatter.format(dateTime);
			//System.out.println(dateTime.toString());
			//System.out.println(date_time + '\n');
		}
	
		String customer_id = null;
		if (customer != null && customer.containsField("_id") && 
				customer.get("_id") != null)
			customer_id = customer.get("_id").toString();
//		add else block on 30th,DEC
		else 
			customer_id="\\N";
		
		String firstName = null;
		if (customer != null && customer.containsField("firstName") && 
				customer.get("firstName") != null)
			firstName = (String) customer.get("firstName");
		
		String lastName = null;
		if (customer != null && customer.containsField("lastName") &&
				customer.get("lastName") != null)
			lastName = (String) customer.get("lastName");
		
		StringBuilder customerName = new StringBuilder();
		customerName.append(firstName + " " + lastName);
		
		String email = null;
		if (customer != null && customer.containsField("email") && 
				customer.get("email") != null)
			email = (String) customer.get("email");
		
		String phoneNumber = null;
		if (customer != null && customer.containsField("phoneNumber") &&
				customer.get("phoneNumber") != null)
			phoneNumber = (String) customer.get("phoneNumber");
		
		StringBuilder leads = new StringBuilder();
		if (customer != null && customer.containsField("leads") &&
				customer.get("leads") != null &&
				customer.get("leads") instanceof ArrayList)
		{
			ArrayList<ObjectId> leads1 = (ArrayList<ObjectId>) customer.get("leads");
			if (!leads1.isEmpty())
			{
				for (ObjectId lead : leads1)
					leads.append(lead.toString() + '\002');
				leads.deleteCharAt(leads.length() - 1);
			}
//			add else block on 30th,DEC,2013
			else
				leads.append("\\N");
		}
		
		

		output.append(type + '\001');
		output.append(date_time + '\001');
		output.append(customer_id + '\001');
		//output.append(firstName + '\001');
		//output.append(lastName + '\001');
		output.append(customerName.toString() + '\001');
		output.append(email + '\001');
		output.append(phoneNumber + '\001');
		output.append(leads.toString());
		
		
		
		
		return output;
	}
	
	public static StringBuilder processApiCall(DBObject apiCall)
	{
		StringBuilder output = new StringBuilder();
			
		Date dateTime = null;
		if (apiCall != null && apiCall.containsField("dateTime") && apiCall.get("dateTime") != null &&
				apiCall.get("dateTime") instanceof Date)
			dateTime = (Date) apiCall.get("dateTime");	
		
		String date_time = null;
		if (dateTime != null)
		{
			//dateTime.setHours(dateTime.getHours() - 8);
			date_time = formatter.format(dateTime);
			//System.out.println(dateTime.toString());
			//System.out.println(date_time + '\n');
		}
	
		
		String endpoint = null;
		if (apiCall != null && apiCall.containsField("endpoint") && 
				apiCall.get("endpoint") != null)
			endpoint = (String) apiCall.get("endpoint");
		
		Boolean isResponse = false;
		if (apiCall != null && apiCall.containsField("isResponse") && 
				apiCall.get("isResponse") != null 
				&& apiCall.get("isResponse") instanceof Boolean)
			isResponse = (Boolean) apiCall.get("isResponse");
		
		Boolean isRequest = false;
		if (apiCall != null && apiCall.containsField("isRequest") && 
				apiCall.get("isRequest") != null
				&& apiCall.get("isRequest") instanceof Boolean)
			isRequest = (Boolean) apiCall.get("isRequest");
		
		Integer duration = 0;
		if (apiCall != null && apiCall.containsField("duration") && 
				apiCall.get("duration") != null)
			duration = (Integer) apiCall.get("duration");
		
		int buckets = 0;
		int buckete = 0;
		if (duration != 0)
		{
			int tmp = (duration - 1) / 5;
			buckets = tmp * 5 + 1;
			buckete = (tmp + 1) * 5;
		}
		
		String apiCall_id = null;
		if (apiCall != null && apiCall.containsField("_id") && 
				apiCall.get("_id") != null)
			apiCall_id = apiCall.get("_id").toString();
//		add else block on 30th,DEC
		else
			apiCall_id="\\N";
		
		String session_id = null;
		if (apiCall != null && apiCall.containsField("session") &&
				apiCall.get("session") != null)
			session_id =  apiCall.get("session").toString();
//		add else block on 30th,DEC
		else
			session_id="\\N";
		
		Boolean isError = false;
		if (apiCall != null && apiCall.containsField("isError") &&
				apiCall.get("isError") != null &&
				apiCall.get("isError") instanceof Boolean)
			isError = (Boolean) apiCall.get("isError");
		
		boolean isTimedout = false;
		
		boolean isQual = false;
		boolean isQualResponse = false;
		boolean isQualRequest = false;
		boolean isPositiveQual = false;
	
		if (endpoint != null && endpoint.equalsIgnoreCase("ProviderPluginService-Qualify"))
		{
			isQual = true;
			if (isResponse)
				isQualResponse = true;
			if (isRequest)
				isQualRequest = true;
			
		}
		
		
		
		DBObject payload = null;
		if (isQual)
		{
		if (apiCall != null && apiCall.containsField("payload") && apiCall.get("payload") != null &&
				apiCall.get("payload") instanceof DBObject)
			payload = (DBObject) apiCall.get("payload");
		}
		
		DBObject serviceLocation = new BasicDBObject();
		//StringBuilder providers1 = new StringBuilder();
		StringBuilder products1 = new StringBuilder();		
		int numberOfProducts = 0;
		
		if (isQualResponse)
		{
		if (payload != null && payload.containsField("Providers") 
				&& payload.get("Providers") != null 
				&& payload.get("Providers") instanceof ArrayList)
		{
			ArrayList<DBObject> providers = (ArrayList<DBObject>) payload.get("Providers");
			if (!providers.isEmpty())
				isPositiveQual = true;
			if (!providers.isEmpty())
			{
				Iterator it1 = providers.iterator();
				while (it1.hasNext())
				{
					DBObject provider = (DBObject) it1.next();
					//BasicDBObject provider1 = new BasicDBObject();
					String providerCode = null;
					String providerName = null;
					if (provider.containsField("ProviderCode") && provider.get("ProviderCode") != null)
						providerCode = (String) provider.get("ProviderCode");					
					if (provider.containsField("ProviderName") && provider.get("ProviderName") != null)
						providerName = (String) provider.get("ProviderName");
					
					//provider1.append("providerCode", providerCode);
					//provider1.append("providerName", providerName);
					
				//StringBuilder products1 = new StringBuilder();
				if (provider.containsField("Products") && provider.get("Products") != null &&
						provider.get("Products") instanceof ArrayList)
				{
					ArrayList<DBObject> products = (ArrayList<DBObject>) provider.get("Products");
					if (!products.isEmpty())
					{
						Iterator it2 = products.iterator();
						
						while (it2.hasNext())
						{
							DBObject product = (DBObject) it2.next();
							StringBuilder product1 = new StringBuilder();
							String productCode = null;
							String productName = null;
							String productDescription = null;
							if (product.containsField("ProductCode") && product.get("ProductCode") != null)
								productCode = (String) product.get("ProductCode");
							if (product.containsField("ProductName") && product.get("ProductName") != null)
								productName = (String) product.get("ProductName");
							if (product.containsField("ProductDescription") && product.get("ProductDescription") != null)
								productDescription = (String) product.get("ProductDescription");
							
							product1.append("providerCode" + '\004' + providerCode + '\003');
							product1.append("providerName" + '\004' + providerName + '\003');
							product1.append("productCode" + '\004' + productCode + '\003');
							product1.append("productName" + '\004' + productName + '\003');
							product1.append("productDescription" + '\004' + productDescription);
							
						
							products1.append(product1.toString() + '\002');
							
						
						}
						numberOfProducts += products.size();
						
					}
				}
				//provider1.append("products", products1);
				//providers1.add(provider1);
				}
			}
			if (products1.length() > 0)
				products1.deleteCharAt(products1.length() - 1);
		
		}
		}
		
	
			if (isQualRequest && payload != null && payload.containsField("ServiceLocation") && 
					payload.get("ServiceLocation") != null &&
					payload.get("ServiceLocation") instanceof DBObject) 
			{
				serviceLocation = (DBObject) payload.get("ServiceLocation");
			}
	
		
		String ZipCode = null;
		String Zip4 = null;
		String TelephoneNumber = null;
		String StateCode = null;
		String Nxx = null;
		String Npa = null;
		String Country = null;
		String City = null;
		String ApartmentNumber = null;
		String AddressLine4 = null;
		String AddressLine3 = null;
		String AddressLine2 = null;
		String AddressLine1 = null;
		
		if (serviceLocation != null && serviceLocation.containsField("ZipCode") && 
				serviceLocation.get("ZipCode") != null)
			ZipCode = (String) serviceLocation.get("ZipCode");
		
		if (serviceLocation != null && serviceLocation.containsField("Zip4") && 
				serviceLocation.get("Zip4") != null)
			Zip4 = (String) serviceLocation.get("Zip4");
		
		if (serviceLocation != null && serviceLocation.containsField("TelephoneNumber") &&
				serviceLocation.get("TelephoneNumber") != null)
			TelephoneNumber = (String) serviceLocation.get("TelephoneNumber");
		
		if (serviceLocation != null && serviceLocation.containsField("StateCode") && 
				serviceLocation.get("StateCode") != null)
			StateCode = (String) serviceLocation.get("StateCode");
		
		if (serviceLocation != null && serviceLocation.containsField("Nxx") && 
				serviceLocation.get("Nxx") != null)
			Nxx = (String) serviceLocation.get("Nxx");
		
		if (serviceLocation != null && serviceLocation.containsField("Npa") && 
				serviceLocation.get("Npa") != null)
			Npa = (String) serviceLocation.get("Npa");
		
		if (serviceLocation != null && serviceLocation.containsField("Country") && 
				serviceLocation.get("Country") != null)
			Country = (String) serviceLocation.get("Country");
		
		if (serviceLocation != null && serviceLocation.containsField("City") && 
				serviceLocation.get("City") != null)
			City = (String) serviceLocation.get("City");
		
		if (serviceLocation != null && serviceLocation.containsField("ApartmentNumber") &&
				serviceLocation.get("ApartmentNumber") != null)
			ApartmentNumber = (String) serviceLocation.get("ApartmentNumber");
		
		if (serviceLocation != null && serviceLocation.containsField("AddressLine4") && 
				serviceLocation.get("AddressLine4") != null)
			AddressLine4 = (String) serviceLocation.get("AddressLine4");
		
		if (serviceLocation != null && serviceLocation.containsField("AddressLine3") &&
				serviceLocation.get("AddressLine3") != null)
			AddressLine3 = (String) serviceLocation.get("AddressLine3");
		
		if (serviceLocation != null && serviceLocation.containsField("AddressLine2") && 
				serviceLocation.get("AddressLine2") != null)
			AddressLine2 = (String) serviceLocation.get("AddressLine2");
		
		if (serviceLocation != null && serviceLocation.containsField("AddressLine1") && 
				serviceLocation.get("AddressLine1") != null)
			AddressLine1 = (String) serviceLocation.get("AddressLine1");
		
		StringBuilder serviceLocation1 = new StringBuilder();
		
		serviceLocation1.append("zipCode" + '\003' + ZipCode + '\002');
		serviceLocation1.append("zip4" + '\003' + Zip4 + '\002');
		serviceLocation1.append("telephoneNumber" + '\003' + TelephoneNumber + '\002');
		serviceLocation1.append("stateCode" + '\003' + StateCode + '\002');
		serviceLocation1.append("Nxx" + '\003' + Nxx + '\002');
		serviceLocation1.append("Npa" + '\003' + Npa + '\002');
		serviceLocation1.append("country" + '\003' + Country + '\002');
		serviceLocation1.append("city" + '\003' + City + '\002');
		serviceLocation1.append("apartmentNumber" + '\003' + ApartmentNumber + '\002');
		serviceLocation1.append("addressLine4" + '\003' + AddressLine4 + '\002');
		serviceLocation1.append("addressLine3" + '\003' + AddressLine3 + '\002');
		serviceLocation1.append("addressLine2" + '\003' + AddressLine2 + '\002');
		serviceLocation1.append("addressLine1" + '\003' + AddressLine1);
		
		
		
		String type = "APICALL";
		if (apiCall != null && apiCall.containsField("type") && apiCall.get("type") != null)
			type = (String) apiCall.get("type");
		
		String qual_id = null;
		if (apiCall != null && apiCall.containsField("qual") && apiCall.get("qual") != null)
			qual_id = (String) apiCall.get("qual");
//		add else block on 30th,DEC,2013
		else
			qual_id="\\N";
		
		output.append(type + '\001');
		output.append(date_time + '\001');
		output.append(endpoint + '\001');
		output.append(isResponse);
		output.append('\001');
		output.append(isRequest);
		output.append('\001');
		output.append(isError);
		output.append('\001');
		output.append(duration);
		output.append('\001');
		output.append(buckets);
		output.append('\001');
		output.append(buckete);
		output.append('\001');
		output.append(apiCall_id + '\001');
		output.append(session_id + '\001');
		output.append(isTimedout);
		output.append('\001');
		output.append(isQual);
		output.append('\001');
		output.append(qual_id + '\001');
		output.append(isQualResponse);
		output.append('\001');
		output.append(isQualRequest);
		output.append('\001');		
		output.append(isPositiveQual);
		output.append('\001');				
		output.append(numberOfProducts);
		output.append('\001');		
		output.append(products1.toString() + '\001');
		output.append(serviceLocation1.toString());

		/*BasicDBObject qualInfo = new BasicDBObject();
		qualInfo.append("isQualResponse", isQualResponse);
		qualInfo.append("isQualRequest", isQualRequest);
		qualInfo.append("isPositiveQual", isPositiveQual);
		qualInfo.append("numberOfProducts", numberOfProducts);
		qualInfo.append("providers", providers1);
		qualInfo.append("serviceLocation", serviceLocation);
		
		output.append("qualInfo", qualInfo);*/
		
		
		return output;
	}

	public static StringBuilder processQual(DBObject qual)
	{
		StringBuilder output = new StringBuilder();
		
		String type= "QUAL";
		if (qual != null && qual.containsField("type") && qual.get("type") != null)
			type = (String) qual.get("type");
		
		String qual_id= null;
		if (qual != null && qual.containsField("_id") && qual.get("_id") != null)
			qual_id = qual.get("_id").toString();
//		add else block on 30th,DEC,2013
		else
			qual_id="\\N";
		
		
		//System.out.println("qual_id = " + qual_id);
		Date dateTime = null;
		if (qual != null && qual.containsField("dateTime") && qual.get("dateTime") != null &&
				qual.get("dateTime") instanceof Date)
			dateTime = (Date) qual.get("dateTime");	
		
		String date_time = null;
		if (dateTime != null)
		{
			//dateTime.setHours(dateTime.getHours() - 8);
			date_time = formatter.format(dateTime);
			//System.out.println(dateTime.toString());
			//System.out.println(date_time + '\n');
		}
	
		
		String session_id = null;
		if (qual != null && qual.containsField("session") && qual.get("session") != null)
			session_id = qual.get("session").toString();
//		add else block on 30th,DEC,2013
		else
			session_id="\\N";
		
		Boolean wasSuccessful = true;
		if (qual != null && qual.containsField("wasSuccessful") && qual.get("wasSuccessful") != null
				&& qual.get("getSuccessful") instanceof Boolean)
			wasSuccessful = (Boolean) qual.get("wasSuccessful");
		
		
		DBObject response = null;
		if (qual != null && qual.containsField("response") && qual.get("response") != null &&
				qual.get("response") instanceof DBObject)
			response = (DBObject) qual.get("response");

		
		DBObject request = null;
		if (qual != null && qual.containsField("request") && qual.get("request") != null &&
				qual.get("request") instanceof DBObject)
			request = (DBObject) qual.get("request");
		
		DBObject standardized = null;
	
		boolean hasAddress = false;
		if (response != null && response.containsField("standardized") && response.get("standardized") != null &&
				response.get("standardized") instanceof DBObject)
		{
			//System.out.println(response.get("standardized").getClass());
			standardized = (DBObject) response.get("standardized");
		}
		
		
		
		int numberOfProducts = 0;
		if (standardized != null && standardized.containsField("offers") && 
				standardized.get("offers") != null && 
				standardized.get("offers") instanceof ArrayList)
			numberOfProducts = ((ArrayList<DBObject>) standardized.get("offers")).size(); 
		
		
		ArrayList<String> providers1 = null;
		if (response != null && response.containsField("providers") && 
				response.get("providers") != null &&
				response.get("providers") instanceof ArrayList)
			providers1 = (ArrayList<String>) response.get("providers");
		
		boolean isPositiveQual = true;
		if (providers1 != null && providers1.isEmpty())
			isPositiveQual = false;
		
		
		
		DBObject address = null;
		
		String address_city  = null;
		String address_state = null;
		String address_zip = null;
		String address_suite = null;
		String address_line = null;
		
		if (standardized != null && standardized.containsField("address") && 
				standardized.get("address") != null && 
				standardized.get("address") instanceof DBObject)
		{
			hasAddress = true;
			address = (DBObject) standardized.get("address");
			
			if (address != null && address.containsField("city") && 
					address.get("city") != null)
				address_city = (String) address.get("city");
			
			if (address != null && address.containsField("state") && address.get("state") != null)
				address_state = (String) address.get("state");
			if (address != null && address.containsField("zip") && address.get("zip") != null)
				address_zip = (String) address.get("zip");
			if (address != null && address.containsField("suite") && address.get("suite") != null)
				address_suite = (String) address.get("suite");
			if (address != null && address.containsField("line") && address.get("line") != null)
				address_line = (String) address.get("line");
			
		}
		
		
		
		if (!hasAddress && request != null && request.containsField("standardized") 
				&& request.get("standardized") != null && 
				request.get("standardized") instanceof DBObject)
		{
			standardized = (DBObject) request.get("standardized");
		
		if (standardized != null && standardized.containsField("zip") && 
				standardized.get("zip") != null)
			address_zip = (String) standardized.get("zip");
		if (standardized != null && standardized.containsField("suite") &&
				standardized.get("suite") != null)
			address_suite = (String) standardized.get("suite");
		if (standardized != null && standardized.containsField("line") && 
				standardized.get("line") != null)
			address_line = (String) standardized.get("line");
		}
	
		
		
		
		
		output.append(type + '\001');
		output.append(qual_id + '\001');
		output.append(date_time + '\001');
		output.append(session_id + '\001');
		output.append(wasSuccessful);
		output.append('\001');
		output.append(isPositiveQual);
		output.append('\001');
		output.append(numberOfProducts);
		output.append('\001');
        output.append(address_city + '\001');
        output.append(address_state + '\001');
        output.append(address_zip + '\001');
        output.append(address_suite + '\001');
        output.append(address_line);


		
		return output;
	}
	
	public static StringBuilder processError(DBObject error)
	{
		StringBuilder output = new StringBuilder();
		
		String type = "ERROR";
		if (error != null && error.containsField("type") && 
				error.get("type") != null)
			type = (String) error.get("type");
		
		String error_id = null;
		if (error != null && error.containsField("_id") && 
				error.get("_id") != null)
			error_id = error.get("_id").toString();
//		add else block on 30th,DEC,2013
		else
			error_id="\\N";
		
		
		Date dateTime = null;
		if (error != null && error.containsField("dateTime") && 
				error.get("dateTime") != null &&
				error.get("dateTime") instanceof Date)
			dateTime = (Date) error.get("dateTime");	
		
		String date_time = null;
		if (dateTime != null)
		{
			//dateTime.setHours(dateTime.getHours() - 8);
			date_time = formatter.format(dateTime);
			//System.out.println(dateTime.toString());
			//System.out.println(date_time + '\n');
		}
	
	
		
		String errorCode = null;
		if (error != null && error.containsField("errorCode") && 
				error.get("errorCode") != null)
			errorCode = (String) error.get("errorCode");
		
		String errorType = null;
		if (error != null && error.containsField("errorType") && 
				error.get("errorType") != null)
			errorType = (String) error.get("errorType");
		
		String errorMessage = null;
		if (error != null && error.containsField("errorMessage") && 
				error.get("errorMessage") != null)
		{
			errorMessage = (String) error.get("errorMessage");
			if (errorMessage.contains("\n"))
			{
				String[] strArray = errorMessage.split("\n");
				errorMessage = "";
				for (String str : strArray)
					errorMessage = errorMessage + '_' + str;
			
				
			}
		}
		String session_id = null;
		if (error != null && error.containsField("session") &&
				error.get("session") != null)
			session_id = error.get("session").toString();
//		add else block on 30th,DEC,2013
		else
			session_id="\\N";
		
		String providerCode = null;
		if (error != null && error.containsField("providerCode") && error.get("providerCode") != null)
			providerCode = (String) error.get("providerCode");
		
		String providerName = getProviderName(providerCode);
		
		String apicall_id = null;
		if (error != null && error.containsField("apicall") && 
				error.get("apicall") != null)
			apicall_id = (String) error.get("apicall");
//		add else block on 30th,DEC,2013
		else
			apicall_id="\\N";
		
		int severity = getSeverity(errorCode);
		
		output.append(type + '\001');
		output.append(date_time + '\001');
		output.append(error_id + '\001');
		output.append(session_id + '\001');
		output.append(errorType + '\001');
		output.append(errorCode + '\001');
		output.append(errorMessage + '\001');
		output.append(providerCode + '\001');
		output.append(providerName + '\001');
		output.append(severity);
		output.append('\001');
		output.append(apicall_id);
		
		
		return output;
	}
	
	public static StringBuilder processOrder(DBObject order)
	{
		StringBuilder output = new StringBuilder();
		
		String type = "ORDER";
		if (order != null && order.containsField("type") && 
				order.get("type") != null)
			type = (String) order.get("type");
		
		
		Date dateTime = null;
		if (order != null && order.containsField("dateTime") && 
				order.get("dateTime") != null &&
				order.get("dateTime") instanceof Date)
			dateTime = (Date) order.get("dateTime");	
		
		String date_time = null;
		if (dateTime != null)
		{
			//dateTime.setHours(dateTime.getHours() - 8);
			date_time = formatter.format(dateTime);
			//System.out.println(dateTime.toString());
			//System.out.println(date_time + '\n');
		}
	
		
		String order_id = null;
		if (order != null && order.containsField("_id") && 
				order.get("_id") != null)
			order_id = order.get("_id").toString();
//		add else block on 30th,DEC,2013
		else
			order_id="\\N";
		
		String qual_id = null;
		if (order != null && order.containsField("qual") && 
				order.get("qual") != null)
			qual_id = order.get("qual").toString();
//		add else block on 30th,DEC,2013
		else
			qual_id="\\N";
		
		String session_id= null;
		if (order != null && order.containsField("session") && 
				order.get("session") != null)
			session_id = order.get("session").toString();
//		add else block on 30th,DEC,2013
		else
			session_id="\\N";
		
		
		String providerId = null;
		if (order != null && order.containsField("providerId") &&
				order.get("providerId") != null)
			providerId = (String) order.get("providerId");
		
		String providerCode = null;
		if (order != null && order.containsField("providerCode") &&
				order.get("providerCode") != null)
			providerCode = (String) order.get("providerCode");
		
		String providerName = getProviderName(providerCode);
		
		String trackingId = null;
		if (order != null && order.containsField("trackingId") && 
				order.get("trackingId") != null)
			trackingId = (String) order.get("trackingId");
		
		String API =null;
		if(trackingId==null||trackingId.equalsIgnoreCase("null"))
			API="CPOS";
	    else
	    	API="G2B";
		
		ArrayList<DBObject> confirmations = null;
		if (order != null && order.containsField("confirmations") && 
				order.get("confirmations") != null &&
				order.get("confirmations") instanceof ArrayList)
			confirmations = (ArrayList<DBObject>) order.get("confirmations");
		StringBuilder orderConfirmationIds = new StringBuilder();
		if (confirmations != null && !confirmations.isEmpty())
		{
			for (DBObject confirmation : confirmations)
			{
				if (confirmation.containsField("orderConfirmationId") &&
						confirmation.get("orderConfirmationId") != null)
					orderConfirmationIds.append(confirmation.get("orderConfirmationId").toString() 
							+ '_');
			}
			if (orderConfirmationIds.length() > 0)
				orderConfirmationIds.deleteCharAt(orderConfirmationIds.length() - 1);
		}
		
		
		
		ArrayList<DBObject> products = new ArrayList<DBObject>();
		if (order != null && order.containsField("products") && order.get("products") != null && 
				order.get("products") instanceof ArrayList)
			products = (ArrayList<DBObject>) order.get("products");
		
		if (products != null && !products.isEmpty())
		{
			Iterator it = products.iterator();
			while (it.hasNext())
			{
				DBObject product = (DBObject) it.next();
				StringBuilder product1 = new StringBuilder();
				
				
				product1.append(type + '\001');
				product1.append(date_time + '\001');
				product1.append(order_id + '\001');
				product1.append(qual_id + '\001');
				product1.append(session_id + '\001');
				product1.append(providerId + '\001');
				product1.append(providerCode + '\001');
				product1.append(providerName + '\001');
				product1.append(trackingId + '\001');
				product1.append(API + '\001');
				product1.append(orderConfirmationIds.toString() + '\001');
				
				
				String term = null;
				String voiceServiceExtension = "false";
				String videoServiceExtension = "false";
				String dataServiceExtension = "false";
				String price = null;
				String description = null;
				StringBuilder lineItemDescription = new StringBuilder();
				
				if (product.containsField("voiceServiceExtension") && 
						product.get("voiceServiceExtension") != null &&
						product.get("voiceServiceExtension") instanceof String)
					voiceServiceExtension = (String) product.get("voiceServiceExtension");
				
				if (product.containsField("videoServiceExtension") && 
						product.get("videoServiceExtension") != null && 
						product.get("videoServiceExtension") instanceof String)
					videoServiceExtension = (String) product.get("videoServiceExtension");
				
				if (product.containsField("dataServiceExtension") && 
						product.get("dataServiceExtension") != null &&
						product.get("dataServiceExtension") instanceof String)
					dataServiceExtension = (String) product.get("dataServiceExtension");
				if (voiceServiceExtension.equalsIgnoreCase("true"))
					lineItemDescription.append("Voice");
				if (videoServiceExtension.equalsIgnoreCase("true"))
					lineItemDescription.append("Video");
				if (dataServiceExtension.equalsIgnoreCase("true"))
					lineItemDescription.append("Data");
				
				
				
				if (product.containsField("price") && product.get("price") != null &&
						product.get("price") instanceof DBObject)
				{
					DBObject price1 = null;
					price1 = (DBObject) product.get("price");
					if (price1.containsField("promo") && price1.get("promo") != null && 
							price1.get("promo") instanceof DBObject)
					{
						DBObject promo = null;
						promo = (DBObject) price1.get("promo");
						if (promo.containsField("price") && promo.get("price") != null)
							price = promo.get("price").toString();
						if (promo.containsField("term") && promo.get("term") != null)
							term = promo.get("term").toString();
						if (promo.containsField("description") && promo.get("description") != null)
						{
							description = promo.get("description").toString();
							description.replace('\n', '.');
							if (description.contains("\n"))
							{
								String[] strArray = description.split("\n");
								description = "";
								for (String str : strArray)
									description = description + "_" + str;
							}
						}
					}
				}
				
				String lineItemCount = null;
				if (product.containsField("lineItemCount") && product.get("lineItemCount") != null)
					lineItemCount = (String) product.get("lineItemCount");
		
				String verticalCode = null;
				if (product.containsField("verticalCode") && product.get("verticalCode") != null)
					verticalCode = (String) product.get("verticalCode");
				
				String productName = null;
				if (product.containsField("name") && product.get("name") != null)
					productName = (String) product.get("name");
				
				
		
				//System.out.print("VoiceServiceExtension " + voiceServiceExtension);
				//System.out.print(" VideoServiceExtension " + videoServiceExtension);
				//System.out.println(" dataServiceExtension " + dataServiceExtension + "\n");


				product1.append(lineItemDescription.toString() + '\001');
				product1.append(price + '\001');
				product1.append(term + '\001');
				product1.append(description + '\001');
				product1.append(verticalCode + '\001');
				product1.append(lineItemCount + '\001');
				product1.append(productName + '\n');
				output.append(product1);
		
				
			}
			if (output.length() > 0)
				output.deleteCharAt(output.length() - 1);
			
		}
	
		/*output.append(type + '\001');
		output.append(date_time + '\001');
		output.append(order_id + '\001');
		output.append(qual_id + '\001');
		output.append(session_id + '\001');
		output.append(providerId + '\001');
		output.append(providerCode + '\001');
		output.append(providerName + '\001');
		output.append(trackingId + '\001');
		output.append(orderConfirmationIds.toString() + '\001');
		output.append(products1.toString());*/
		
		
		return output;
		
	}
	
	public static StringBuilder processLead(DBObject lead)
	{
		StringBuilder output = new StringBuilder();
		
	    
	    Date dateTime = null;
		if (lead != null && lead.containsField("dateTime") && 
				lead.get("dateTime") != null &&
				lead.get("dateTime") instanceof Date)
			dateTime = (Date) lead.get("dateTime");	
		
		String date_time = null;
		if (dateTime != null)
		{
			//dateTime.setHours(dateTime.getHours() - 8);
			date_time = formatter.format(dateTime);
			//System.out.println(dateTime.toString());
			//System.out.println(date_time + '\n');
		}
	
		
	    String type = "LEAD";
	    if (lead != null && lead.containsField("type") && 
	    		lead.get("type") != null)
	    	type = (String) lead.get("type");
	    

	    String lead_id = null;
	    if (lead != null && lead.containsField("_id") && lead.get("_id") != null)
	    	lead_id = lead.get("_id").toString();
//		add else block on 30th,DEC,2013
		else
			lead_id="\\N";

	    StringBuilder sessions = new StringBuilder();	   
	    if (lead != null && lead.containsField("sessions") && lead.get("sessions") != null &&
	    		lead.get("sessions") instanceof ArrayList)
	    {
	    	 ArrayList<ObjectId> sessions1 = null;
	    	sessions1 = (ArrayList<ObjectId>) lead.get("sessions");
	    	if (!sessions1.isEmpty())
	    	{
	    		for (ObjectId session : sessions1)
	    			sessions.append(session.toString() + '\002');
	    		sessions.deleteCharAt(sessions.length() - 1);
	    	}
//			add else block on 30th,DEC,2013
			else
				sessions.append("\\N");
	    }
	    
	    StringBuilder visits = new StringBuilder();	   
	    if (lead != null && lead.containsField("visits") && lead.get("visits") != null &&
	    		lead.get("visits") instanceof ArrayList)
	    {
	    	 ArrayList<ObjectId> visits1 = null;
	    	 visits1 = (ArrayList<ObjectId>) lead.get("visits");
	    	if (!visits1.isEmpty())
	    	{
	    		for (ObjectId visit : visits1)
	    			visits.append(visit.toString() + '\002');
	    		visits.deleteCharAt(visits.length() - 1);
	    	}
//			add else block on 30th,DEC,2013
			else
				visits.append("\\N");
	    }
	    
	    
	    
	    DBObject address = new BasicDBObject();
	    if (lead != null && lead.containsField("address") && lead.get("address") != null &&
	    		lead.get("address") instanceof DBObject)
	    	address = (DBObject) lead.get("address");
	    String address_city = null;
	    String address_line = null;
	    String address_state = null;
	    String address_suite = null;
	    String address_zip = null;
	    
	    if (address.containsField("city") && address.get("city") != null)
	    	address_city = (String) address.get("city");
	    
	    if (address.containsField("line") && address.get("line") != null)
	    	address_line = (String) address.get("line");
	    if (address.containsField("state") && address.get("state") != null)
	    	address_state = (String) address.get("state");
	    if (address.containsField("suite") && address.get("suite") != null)
	    	address_suite = (String) address.get("suite");
	    if (address.containsField("zip") && address.get("zip") != null)
	    	address_zip = (String) address.get("zip");

	    
	    

	    String leadSource = null;
	    if (lead != null && lead.containsField("leadSource") && 
	    		lead.get("leadSource") != null)
	    	leadSource = (String) lead.get("leadSource");
	    
	    String leadSourceName = getLeadSourceName(leadSource);

	    String providerPhone = null;
	    if (lead != null && lead.containsField("providerPhone") &&
	    		lead.get("providerPhone") != null)
	    	providerPhone = (String) lead.get("providerPhone");
	   
	    String providerCode = null;
	    if (lead != null && lead.containsField("providerCode") && 
	    		lead.get("providerCode") != null)
	    	providerCode = (String) lead.get("providerCode");
	    

	    DBObject semTracking = new BasicDBObject();
	    if (lead != null && lead.containsField("semTracking") && 
	    		lead.get("semTracking") != null &&
	    		lead.get("semTracking") instanceof DBObject)
	    	semTracking = (DBObject) lead.get("semTracking");
	    
		String semTracking_inboundQueryString = null;
		String semTracking_adId = null;
		String semTracking_naturalTerms = null;
		String semTracking_searchEngine = null;
		String semTracking_naturalSearchId = null;
		String semTracking_searchId = null;
		String semTracking_referrer = null;
		String semTracking_adGroup = null;
		String semTracking_campaign = null;
		String semTracking_terms = null;
		
		if (semTracking.containsField("inboundQueryString") && 
				semTracking.get("inboundQueryString") != null)
			semTracking_inboundQueryString = (String) semTracking.get("inboundQueryString");
		
		if (semTracking.containsField("adId") && semTracking.get("adId") != null)
			semTracking_adId = (String) semTracking.get("adId");
		
		if (semTracking.containsField("naturalTerms") && semTracking.get("naturalTerms") != null)
			semTracking_naturalTerms = (String) semTracking.get("naturalTerms");
		
		if (semTracking.containsField("searchEngine") && semTracking.get("searchEngine") != null)
			semTracking_searchEngine = (String) semTracking.get("searchEngine");
		
		if (semTracking.containsField("naturalSearchId") && semTracking.get("naturalSearchId") != null)
			semTracking_naturalSearchId = (String) semTracking.get("naturalSearchId");
		
		if (semTracking.containsField("searchId") && semTracking.get("searchId") != null)
			semTracking_searchId = (String) semTracking.get("searchId");
		
		if (semTracking.containsField("referrer") && semTracking.get("referrer") != null)
			semTracking_referrer = (String) semTracking.get("referrer");
		
		if (semTracking.containsField("adGroup") && semTracking.get("adGroup") != null)
			semTracking_adGroup = (String) semTracking.get("adGroup");
		
		if (semTracking.containsField("campaign") && semTracking.get("campaign") != null)
			semTracking_campaign = (String) semTracking.get("campaign");
		
		
		if (semTracking.containsField("terms") && semTracking.get("terms") != null)
			semTracking_terms = (String) semTracking.get("terms");
		
		
		output.append(type + '\001');
		output.append(lead_id + '\001');
		output.append(date_time + '\001');
		output.append(sessions.toString() + '\001');
		output.append(visits.toString() + '\001');

		output.append(address_city + '\001');
		output.append(address_line + '\001');
		output.append(address_state + '\001');
		output.append(address_suite + '\001');
		output.append(address_zip + '\001');

		output.append(leadSource+ '\001');
		output.append(leadSourceName + '\001');
		output.append(providerPhone + '\001');
		output.append(providerCode + '\001');
		
		output.append(semTracking_inboundQueryString + '\001');
		output.append(semTracking_adId + '\001');
		output.append(semTracking_naturalTerms + '\001');
		output.append(semTracking_searchEngine + '\001');
		output.append(semTracking_naturalSearchId + '\001');
		output.append(semTracking_searchId + '\001');
		output.append(semTracking_referrer + '\001');
		output.append(semTracking_adGroup + '\001');
		output.append(semTracking_campaign + '\001');
		output.append(semTracking_terms);
		
		
		return output;
	}
	
	public static StringBuilder processVisit(DBObject visit)
	{
		StringBuilder output = new StringBuilder();
		
		 Date dateTime = null;
			if (visit != null && visit.containsField("dateTime") && 
					visit.get("dateTime") != null &&
					visit.get("dateTime") instanceof Date)
				dateTime = (Date) visit.get("dateTime");	
			
			String date_time = null;
			if (dateTime != null)
			{
				//dateTime.setHours(dateTime.getHours() - 8);
				date_time = formatter.format(dateTime);
				//System.out.println(dateTime.toString());
				//System.out.println(date_time + '\n');
			}
		
			
		    String type = "VISIT";
		    if (visit != null && visit.containsField("type") && 
		    		visit.get("type") != null)
		    	type = (String) visit.get("type");
		    
			
		    String visit_id = null;
		    if (visit != null && visit.containsField("_id") && visit.get("_id") != null)
		    	visit_id = visit.get("_id").toString();
//			add else block on 30th,DEC,2013
			else
				visit_id="\\N";
		    
		    
		    String lead_id = null;
		    if (visit != null && visit.containsField("lead") && visit.get("lead") != null)
		    	lead_id = visit.get("lead").toString();
		    
//		    add else block on 30th,DEC
		    else
		    	lead_id="\\N";
		    
		    String session_id=null;
		    if(visit!=null&&visit.containsField("session")&&visit.get("session")!=null)
		    	session_id=visit.get("session").toString();
		    
//		    add else block on 30th,DEC 
		    else
		    	session_id="\\N";
			
		    output.append(type+'\001');
		    output.append(visit_id+'\001');
		    output.append(date_time+'\001');
		    output.append(lead_id+'\001');
		    output.append(session_id);
		
		return output;
	}
	
	public static BasicDBObject parseUserAgent(String userAgentString) 
	{
		String osName = null;
		String browserName = null;
		String deviceTypeName = null;
		String mobileDevice = null;
		BasicDBObject outputKey = new BasicDBObject();
		
		if (userAgentString != null) 
		{
			UserAgent userAgent = UserAgent.parseUserAgentString(userAgentString);
			
			if (userAgent != null) {
		    OperatingSystem os = userAgent.getOperatingSystem();
		    Browser browser = userAgent.getBrowser();
		    DeviceType deviceType = os.getDeviceType();
		    osName = os.getName();
		    browserName = browser.getName();
			if (deviceType == DeviceType.COMPUTER) 
			{
				deviceTypeName = "desktop";
			} else if (deviceType == DeviceType.MOBILE || deviceType == DeviceType.TABLET) 
			{
				deviceTypeName = "mobile";
				OperatingSystem osGroup = os.getGroup();
		        if (osGroup.equals(OperatingSystem.ANDROID)) 
		        {
		        	mobileDevice = "android device";
		        } else if (osGroup.equals(OperatingSystem.KINDLE)) 
		        {
		        	mobileDevice = "kindle";
		        } else if (osGroup.equals(OperatingSystem.WINDOWS)) 
		        {
		        	mobileDevice = "windows mobile device";
		        } else if (osGroup.equals(OperatingSystem.IOS)) 
		        {
		        	if (os.equals(OperatingSystem.iOS4_IPHONE) || os.equals(OperatingSystem.iOS5_IPHONE) ||
		        			os.equals(OperatingSystem.iOS6_IPHONE) || os.equals(OperatingSystem.MAC_OS_X_IPHONE) ||
		        			os.equals(OperatingSystem.IOS)) 
		        	{
		        		mobileDevice = "iphone";
		        	} else if (os.equals(OperatingSystem.MAC_OS_X_IPAD)) 
		        	{
		        		mobileDevice = "ipad";
		        	} else {
		        		mobileDevice = "other";
		        	}
		        } else if (osGroup.equals(OperatingSystem.BLACKBERRY)) 
		        {
		        	mobileDevice = "blackBerry phone";
		        } else if (osGroup.equals(OperatingSystem.BLACKBERRY_TABLET)) 
		        {
		        	mobileDevice = "blackBerry tablet";
		        } else {
		        	mobileDevice = "other";
		        }
			} else {
					deviceTypeName = "other";
			}
			}
		}

		
        outputKey.append("operatingSystem", osName);
		outputKey.append("browser", browserName);
		outputKey.append("deviceType", deviceTypeName);
		outputKey.append("mobileDevice", mobileDevice);
		//outputKey.append("userAgent", userAgentString);
       
		
        return outputKey;
		
	}
	public static String getProviderName(String providerCode)
	{
		String providerName = null;
		
		if (providerCode != null && providerCode.equalsIgnoreCase("pro53"))
			providerName = "comcast";
	    return providerName;
	}
	public static String getLeadSourceName(String leadSource)
	{
		String leadSourceName = null;
		
		
		return leadSourceName;
	}
	public static int getSeverity(String errorCode)
	{
		int severity;
	    severity = 0;
	    String key = null;
	    if (errorCode != null)
	    {
	    	key = errorCode;
	    	if (errorCodes.containsKey(key))
	    		severity = Integer.valueOf(errorCodes.get(key)).intValue();
	    }
		
		
		return severity;
		
		
		
	}
	
	public static Date buildDate(String dateString )
	{
		Date date = null;
		
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
		
		try {
			date = formatter.parse(dateString);
		} catch (Exception e)
		{
			e.printStackTrace();
			System.out.println("exception on SimpleDateFormat parse date string");
		}
		
		return date;
	}
	public static void prepareFiles()
	{
		
		try {

			
			 sessionsFile = new File(filePrefix +  "_sesssion.tmp");
		    leadsFile = new File(filePrefix + "_leads.tmp");
			ordersFile = new File(filePrefix + "_orders.tmp");
			apiCallsFile = new File(filePrefix + "_apiCalls.tmp");
			 errorsFile = new File(filePrefix + "_errors.tmp");
		    qualsFile = new File(filePrefix + "_quals.tmp");
			customersFile = new File(filePrefix + "_customers.tmp");
			visitsFile = new File(filePrefix + "_visits.tmp");
			
			
			if (!sessionsFile.exists())
				sessionsFile.createNewFile();
			
			if (!leadsFile.exists())
				leadsFile.createNewFile();
		
			if (!ordersFile.exists())
				ordersFile.createNewFile();
		
			if (!apiCallsFile.exists())
				apiCallsFile.createNewFile();
		
			if (!errorsFile.exists())
				errorsFile.createNewFile();
		
			if (!qualsFile.exists())
				qualsFile.createNewFile();
		
			if (!customersFile.exists())
				customersFile.createNewFile();
			
			if (!visitsFile.exists())
				visitsFile.createNewFile();
			
		
		sessionsOutput = new BufferedWriter(new FileWriter(sessionsFile));
		leadsOutput = new BufferedWriter(new FileWriter(leadsFile));
		ordersOutput = new BufferedWriter(new FileWriter(ordersFile));
		apiCallsOutput = new BufferedWriter(new FileWriter(apiCallsFile));
		errorsOutput = new BufferedWriter(new FileWriter(errorsFile));
		qualsOutput = new BufferedWriter(new FileWriter(qualsFile));
		customersOutput = new BufferedWriter(new FileWriter(customersFile));
		visitsOutput = new BufferedWriter(new FileWriter(visitsFile));
		
		
		
		
		} catch (Exception e) {
			System.out.println("file not found");
		}


	}
	public static void finishFiles()
	{
		sessionsFile.renameTo(new File(filePrefix + "_sessions.new"));
		leadsFile.renameTo(new File(filePrefix + "_leads.new"));
		ordersFile.renameTo(new File(filePrefix + "_orders.new"));
		apiCallsFile.renameTo(new File(filePrefix + "_apiCalls.new"));
		errorsFile.renameTo(new File(filePrefix + "_errors.new"));
		qualsFile.renameTo(new File(filePrefix + "_quals.new"));
		customersFile.renameTo(new File(filePrefix + "_customers.new"));
		visitsFile.renameTo(new File(filePrefix + "_visits.new"));
		
		
	}
	public static String getDateString(Date date)
	{
	
		SimpleDateFormat formatter = new SimpleDateFormat("YYYY'y'MM'm'dd'd'HH'h'mm'm'ss's'");
		

		
		
		
		return formatter.format(date);
	}
	public static void getErrorCodes()
	{
		//DBCursor result = errorCodesColl.find(query);
		DBCursor result = errorCodesColl.find();
		DBObject errorCodeObject;
		String errorCodeStr = null, severity = null, API_call = null;
		errorCodes = new HashMap<String, String>();
				
				try {
					int count = 0;
					while (result.hasNext())
					{
						count++;
						errorCodeObject = result.next();
						if (errorCodeObject != null && errorCodeObject.containsField("ErrorCd") && 
								errorCodeObject.get("ErrorCd") != null)
							 errorCodeStr = (String) errorCodeObject.get("ErrorCd");
						
						if (errorCodeObject != null && errorCodeObject.containsField("Severity") && 
								errorCodeObject.get("Severity") != null)
							 severity = errorCodeObject.get("Severity").toString();
						if (errorCodeObject != null && errorCodeObject.containsField("API Call") && 
								errorCodeObject.get("API Call") != null)
							API_call = (String) errorCodeObject.get("API Call");
						
						
						//System.out.println(errorCodeStr + " = " + severity);
						//System.out.println(errorCodeObject.toString());
						errorCodes.put(errorCodeStr, severity);
							
						
					}
					//System.out.println(count);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					result.close();
				}
			
	}
	
	public static void prepareDate()
	{
		String regex = "(20[0-9]{2}-[0-1][0-9]-[0-3][0-9]_[0-2][0-9]:[0-5][0-9]:[0-5][0-9])";
		
		if ( startString == null || endString == null ||
				!(Pattern.matches(regex, startString) && Pattern.matches(regex, endString)))
		{
			System.out.println("Start time string or end time string is not right!");
			System.out.println("Please comform to this format yyyy-MM-dd HH:mm:ss!");
			System.out.println("An example is 2013-11-21_13:23:00");
			System.exit(0);
		}
		
		start = buildDate(startString);
		end = buildDate(endString);
		startString = getDateString(start);
		endString = getDateString(end);
		
		BasicDBObject condition = new BasicDBObject("$gte", start);
		condition.append("$lt", end);
		query = new BasicDBObject("dateTime", condition);
		
		fileName = startString + '_' + endString;
		//fileName = "";
		filePrefix = outputFileDest + fileName;
		formatter = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
		
	
		
	}
	
	public static void main(String[] args)
	{
		String propPath = null;
		if (args.length == 2)
		{
			startString = args[0];
			endString = args[1];
			propPath = "E://JAVA workspace//mongodb_data_synchronization//conf//testprop.properties";
			
		} else if (args.length == 3)
		{
			startString = args[0];
			endString = args[1];
			propPath = args[2];
		 
		} else {
			System.out.println("please at least set start time and end endTime");
			System.exit(0);
		}
		//startString = "2013-09-04_04:00:00";
	    //endString = "2013-09-04_07:00:00";
		
		 //String propPath = "/home/zengmingyu/scripts/testprop.properties";
		
		
		//System.out.println(buildDate(startString).toString());
		//System.out.println(buildDate(endString).toString());
		
	    prepareConnections(propPath);
		prepareDate();
		getErrorCodes();
		prepareFiles();
		getFromSessions();
		getFromCustomers();
		getFromApiCalls();
		getFromQuals();
		getFromErrors();
	    getFromOrders();
		getFromLeads();
		getFromVisits();
		finishFiles();
		

		
		
		
		
		}
}








