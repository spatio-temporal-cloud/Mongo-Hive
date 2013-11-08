package synchronization;

import java.util.*;
import java.io.*;

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
	public static DBCollection responseAndErrorColl = null;
	public static DBCollection aggregatedResponsesColl = null;
	public static DBCollection aggregatedErrorsColl = null;
	public static BufferedWriter sessionsOutput = null;
	public static BufferedWriter leadsOutput = null;
	public static BufferedWriter ordersOutput = null;
	public static BufferedWriter apiCallsOutput = null;
	public static BufferedWriter errorsOutput = null;
	public static BufferedWriter qualsOutput = null;
	public static BufferedWriter customersOutput = null;
	public static Date start = null;
	public static Date end = null;
	
	public static Random rand = null;
	
	static {
		
		rand = new Random();
		
		String mongodbServerIP = null;
		int port = -1;
		String leadsCollectionName = null;
		String ordersCollectionName = null;
		String sessionsCollectionName = null;
		String apicallsCollectionName = null;
		String errorsCollectionName = null;
		String qualsCollectionName = null;
		String customersCollectionName = null;
		
		String databaseName = null;
		String responseAndErrorCollectionName = null;
		String aggregatedResponsesCollectionName = null;
		String aggregatedErrorsCollectionName = null;
		
		/*try { 
			FileInputStream fis = new FileInputStream("/home/zengmingyu/resources/onlineOrderConversion.properties");

			Properties properties = new Properties();
			properties.load(fis);
			mongodbServerIP = properties.getProperty("mongodbServerIP", "localhost");
			port = Integer.valueOf(properties.getProperty("port", "27017"));
			databaseName = properties.getProperty("databaseName", "online_500");
			leadsCollectionName = properties.getProperty("leadsCollectionName", "leads");
			ordersCollectionName = properties.getProperty("ordersCollectionName", "orders");
			sessionsCollectionName = properties.getProperty("sessionsCollectionName", "sessions");
			onlineOrderConversionCollectionName = properties.getProperty("onlineOrderConversionCollectionName", "onlineOrderConversion");
			
			
			
			
		fis.close();
		} catch (FileNotFoundException e) 
		{
			System.out.println("properties file not found");
			System.exit(0);
		} catch (IOException e) 
		{
			System.out.println("FileiInputStream close exception");
		}*/
			
		
	    mongodbServerIP = "localhost";
		port = 27017;
		databaseName = "maize";
		sessionsCollectionName = "sessions";
		leadsCollectionName = "leads";
		ordersCollectionName = "orders";
		apicallsCollectionName = "apicalls";
		errorsCollectionName = "errors";
		qualsCollectionName = "quals";
		customersCollectionName = "customers";
		responseAndErrorCollectionName = "responseAndError";
		aggregatedErrorsCollectionName = "aggregatedErrors";
		aggregatedResponsesCollectionName = "aggregatedResponse";
		

		





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
		 System.out.println(" apicalls collection doesn't exist");
			System.exit(0);
	 }
	 
		//responseAndErrorColl = db.getCollection(responseAndErrorCollectionName);
		//aggregatedErrorsColl = db.getCollection(aggregatedErrorsCollectionName);
		//aggregatedResponsesColl = db.getCollection(aggregatedResponsesCollectionName);
		
		try {

			File sessionsFile = new File("sessions.txt");
			File leadsFile = new File("leads.txt");
			File ordersFile = new File("orders.txt");
			File apiCallsFile = new File("apiCalls.txt");
			File errorsFile = new File("errors.txt");
			File qualsFile = new File("quals.txt");
			File customersFile = new File("customers.txt");
			
			sessionsOutput = null;
			leadsOutput = null;
			ordersOutput = null;
			apiCallsOutput = null;
			errorsOutput = null;
			qualsOutput = null;
			customersOutput = null;

			
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
		
		sessionsOutput = new BufferedWriter(new FileWriter(sessionsFile));
		leadsOutput = new BufferedWriter(new FileWriter(leadsFile));
		ordersOutput = new BufferedWriter(new FileWriter(ordersFile));
		apiCallsOutput = new BufferedWriter(new FileWriter(apiCallsFile));
		errorsOutput = new BufferedWriter(new FileWriter(errorsFile));
		qualsOutput = new BufferedWriter(new FileWriter(qualsFile));
		customersOutput = new BufferedWriter(new FileWriter(customersFile));
		
		
		
		
		} catch (Exception e) {
			System.out.println("file not found");
		}

		
		
		
	    end = new Date();
		start = (Date) end.clone();
		start.setMinutes(start.getMinutes() - 1);
		

	}

	public static void getFromSessions()
	{
		BasicDBObject conditions = new BasicDBObject("$gt", start);
		conditions.append("$lt", end);
		BasicDBObject query = new BasicDBObject("dateTime", conditions);
		DBCursor result = sessionsColl.find();
		//DBObject sessionTmp = sessionsColl.findOne();
		//System.out.println(processSession(sessionTmp).toString());
	
		
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				if (count == 10)
					break;
				BasicDBObject sessionDBObject = processSession(result.next());
				sessionsOutput.write(sessionDBObject.toString() + "\n");
				System.out.print(sessionDBObject.toString() + "\n");
			}
			
			sessionsOutput.flush();
			sessionsOutput.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			result.close();
		}
		
		
		
		
	}
	
	public static void getFromCustomers()
	{
		BasicDBObject conditions = new BasicDBObject("$gt", start);
		conditions.append("$lt", end);
		BasicDBObject query = new BasicDBObject("dateTime", conditions);
		DBCursor result = customersColl.find();
		
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				if (count == 10)
					break;
				BasicDBObject customerDBObject = processCustomer(result.next());
				customersOutput.write(customerDBObject.toString() + "\n");
				System.out.print(customerDBObject.toString() + "\n");
				
			}
			customersOutput.flush();
			customersOutput.close();
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
		BasicDBObject conditions = new BasicDBObject("$gt", start);
		conditions.append("$lt", end);
		BasicDBObject query = new BasicDBObject("dateTime", conditions);
		DBCursor result = apiCallsColl.find();
		
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				if (count == 10)
					break;
				BasicDBObject apiCallsDBObject = processApiCall(result.next());
				apiCallsOutput.write(apiCallsDBObject.toString() + "\n");
				System.out.print(apiCallsDBObject.toString() + "\n");
			}
			apiCallsOutput.flush();
			apiCallsOutput.close();
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
		BasicDBObject conditions = new BasicDBObject("$gt", start);
		conditions.append("$lt", end);
		BasicDBObject query = new BasicDBObject("dateTime", conditions);
		DBCursor result = qualsColl.find();
		
		try {
		    int count = 0;
			while (result.hasNext())
			{
				count++;
				if (count == 10)
					break;
				BasicDBObject qualDBObject = processQual(result.next());
				qualsOutput.write(qualDBObject.toString() + "\n");
				System.out.print(qualDBObject.toString() + "\n");
				
			}
			qualsOutput.flush();
			qualsOutput.close();
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
		BasicDBObject conditions = new BasicDBObject("$gt", start);
		conditions.append("$lt", end);
		BasicDBObject query = new BasicDBObject("dateTime", conditions);
		DBCursor result = errorsColl.find();
		
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				if (count == 10) 
					break;
				BasicDBObject errorDBObject = processError(result.next());
				errorsOutput.write(errorDBObject.toString() + "\n");
				System.out.print(errorDBObject.toString() + "\n");
			}
			errorsOutput.flush();
			errorsOutput.close();
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
		BasicDBObject conditions = new BasicDBObject("$gt", start);
		conditions.append("$lt", end);
		BasicDBObject query = new BasicDBObject("dateTime", conditions);
		DBCursor result = ordersColl.find();
		
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				if (count == 10)
					break;
				BasicDBObject orderDBObject = processOrder(result.next());
				ordersOutput.write(orderDBObject.toString() + "\n");
				System.out.print(orderDBObject.toString() + "\n");
			}
			ordersOutput.flush();
			ordersOutput.close();
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
		BasicDBObject conditions = new BasicDBObject("$gt", start);
		conditions.append("$lt", end);
		BasicDBObject query = new BasicDBObject("dateTime", conditions);
		DBCursor result = leadsColl.find();
		
		try {
			int count = 0;
			while (result.hasNext())
			{
				count++;
				if (count == 10)
					break;
				BasicDBObject leadDBObject = processLead(result.next());
				leadsOutput.write(leadDBObject.toString() + "\n");
				System.out.print(leadDBObject.toString() + "\n");
			}
			leadsOutput.flush();
			leadsOutput.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			result.close();
		}
	
		
	
	
		//DBObject leadTmp = leadsColl.findOne();
		//System.out.println(processLead(leadTmp).toString());
		
	}



	
	
	public static BasicDBObject processSession(DBObject session)
	{
		BasicDBObject output = new BasicDBObject();
		
		String userAgentString = null;
		if (session.containsField("userAgent") && session.get("userAgent") != null)
			userAgentString = (String) session.get("userAgent");
		BasicDBObject userAgentDBObject = parseUserAgent(userAgentString);
		
		ArrayList<String> apiCalls  = new ArrayList<String>();
		if (session.containsField("apiCalls") && session.get("apiCalls") != null)
		{
			ArrayList<ObjectId> apiCalls1 = (ArrayList<ObjectId>) session.get("apiCalls");
			if (!apiCalls1.isEmpty())
			{
				for (ObjectId apiCall : apiCalls1)
				apiCalls.add(apiCall.toString());
			}
		}
		
		
		String dateTime = "NULL";
		if (session.containsField("dateTime") && session.get("dateTime") != null)
			dateTime = session.get("dateTime").toString();
		
		String type = "SESSION";
		if (session.containsField("type") && session.get("type") != null)
			type = (String) session.get("type");
		
		String session_id = "NULL"; 
		if (session.containsField("_id") && session.get("_id") != null)
			session_id = session.get("_id").toString();
		
		String lead_id = "NULL";
		if (session.containsField("lead") && session.get("lead") != null)
			lead_id = session.get("lead").toString();

		String visit_id = "NULL";
		if (session.containsField("visit") && session.get("visit") != null)
			visit_id = session.get("visit").toString();
		
		String order_id = "NULL";
		if (session.containsField("order") && session.get("order") != null)
			order_id = session.get("order").toString();
		
		boolean ordered = false;
		if (order_id != "NULL")
			ordered = true;
		
		
		ArrayList<String> quals  = new ArrayList<String>();
		if (session.containsField("quals") && session.get("quals") != null)
		{
			ArrayList<ObjectId> quals1 = (ArrayList<ObjectId>) session.get("quals");
			if (!quals1.isEmpty())
			{
				for (ObjectId qual : quals1)
				quals.add(qual.toString());
			}
		}
		
		ArrayList<String> errs  = new ArrayList<String>();
		if (session.containsField("errs") && session.get("errs") != null)
		{
			ArrayList<ObjectId> errs1 = (ArrayList<ObjectId>) session.get("errs");
			if (!errs1.isEmpty())
			{
				for (ObjectId err : errs1)
					errs.add(err.toString());
			}
		}
	
		
		
		
		DBObject agent = new BasicDBObject();
		if (session.containsField("agent") && session.get("agent") != null)
			agent = (DBObject) session.get("agent");
		
		output.append("userAgent", userAgentDBObject);
		output.append("apiCalls", apiCalls);
		output.append("dateTime", dateTime);
		output.append("type", type);
		output.append("id", session_id);
		output.append("lead", lead_id);
		output.append("visit", visit_id);
		output.append("ordered", ordered);
		output.append("order", order_id);
		output.append("quals", quals);
		output.append("errs", errs);
		output.append("agent", agent);
	
		
		
		return output;
	}
	
	public static BasicDBObject processCustomer(DBObject customer)
	{
		BasicDBObject output = new BasicDBObject();
		
		String type = "CUSTOMER";
		if (customer.containsField("type") && customer.get("type") != null)
			type = (String) customer.get("type");
		
		String dateTime = "NULL";
		if (customer.containsField("dateTime") && customer.get("dateTime") != null)
			dateTime = customer.get("dateTime").toString();
		
		String customer_id = "NULL";
		if (customer.containsField("_id") && customer.get("_id") != null)
			customer_id = customer.get("_id").toString();
		
		String firstName = "NULL";
		if (customer.containsField("firstName") && customer.get("firstName") != null)
			firstName = (String) customer.get("firstName");
		
		String lastName = "NULL";
		if (customer.containsField("lastName") && customer.get("lastName") != null)
			lastName = (String) customer.get("lastName");
		
		String email = "NULL";
		if (customer.containsField("email") && customer.get("email") != null)
			email = (String) customer.get("email");
		
		String phoneNumber = "NULL";
		if (customer.containsField("phoneNumber") && customer.get("phoneNumber") != null)
			phoneNumber = (String) customer.get("phoneNumber");
		
		ArrayList<String> leads = new ArrayList<String>();
		if (customer.containsField("leads") && customer.get("leads") != null)
		{
			ArrayList<ObjectId> leads1 = (ArrayList<ObjectId>) customer.get("leads");
			if (!leads.isEmpty())
			{
				for (ObjectId lead : leads1)
					leads.add(lead.toString());
			}
		}
		
		

		output.append("type", type);
		output.append("dateTime", dateTime);
		output.append("id", customer_id);
		output.append("firstName", firstName);
		output.append("lastName", lastName);
		output.append("email", email);
		output.append("phoneNumber", phoneNumber);
		output.append("leads", leads);
		
		
		
		
		return output;
	}
	
	public static BasicDBObject processApiCall(DBObject apiCall)
	{
		BasicDBObject output = new BasicDBObject();
		
		String dateTime = "NULL";
		if (apiCall.containsField("dateTime") && apiCall.get("dateTime") != null)
			dateTime = apiCall.get("dateTime").toString();
		
		String endpoint = "NULL";
		if (apiCall.containsField("endpoint") && apiCall.get("endpoint") != null)
			endpoint = (String) apiCall.get("endpoint");
		
		boolean isResponse = false;
		if (apiCall.containsField("isResponse") && apiCall.get("isResponse") != null)
			isResponse = (boolean) apiCall.get("isResponse");
		
		boolean isRequest = false;
		if (apiCall.containsField("isRequest") && apiCall.get("isRequest") != null)
			isRequest = (boolean) apiCall.get("isRequest");
		
		int duration = 0;
		if (apiCall.containsField("duration") && apiCall.get("duration") != null)
			duration = (int) apiCall.get("duration");
		
		int buckets = 0;
		int buckete = 0;
		if (duration != 0)
		{
			int tmp = (duration - 1) / 5;
			buckets = tmp * 5 + 1;
			buckete = (tmp + 1) * 5;
		}
		
		String apiCall_id = "NULL";
		if (apiCall.containsField("_id") && apiCall.get("_id") != null)
			apiCall_id = apiCall.get("_id").toString();
		
		String session_id = "NULL";
		if (apiCall.containsField("session") && apiCall.get("session") != null)
			session_id =  apiCall.get("session").toString();
		
		boolean isError = false;
		if (apiCall.containsField("isError") && apiCall.get("isError") != null)
			isError = (boolean) apiCall.get("isError");
		
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
		if (apiCall.containsField("payload") && apiCall.get("payload") != null)
			payload = (DBObject) apiCall.get("payload");
		}
		
		DBObject serviceLocation = new BasicDBObject();
		ArrayList<BasicDBObject> providers1 = new ArrayList<BasicDBObject>();
		int numberOfProducts = 0;
		
		if (isQualResponse)
		{
		if (payload.containsField("Providers") && payload.get("Providers") != null)
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
					BasicDBObject provider1 = new BasicDBObject();
					String providerCode = "NULL";
					String providerName = "NULL";
					if (provider.containsField("ProviderCode") && provider.get("ProviderCode") != null)
						providerCode = (String) provider.get("ProviderCode");					
					if (provider.containsField("ProviderName") && provider.get("ProviderName") != null)
						providerName = (String) provider.get("ProviderName");
					
					provider1.append("providerCode", providerCode);
					provider1.append("providerName", providerName);
					
					ArrayList<BasicDBObject> products1 = new ArrayList<BasicDBObject>();
				if (provider.containsField("Products") && provider.get("Products") != null)
				{
					ArrayList<BSONObject> products = (ArrayList<BSONObject>) provider.get("Products");
					if (!products.isEmpty())
					{
						Iterator it2 = products.iterator();
						
						while (it2.hasNext())
						{
							BSONObject product = (BSONObject) it2.next();
							BasicDBObject product1 = new BasicDBObject();
							String productCode = "NULL";
							String productName = "NULL";
							String productDescription = "NULL";
							if (product.containsField("ProductCode") && product.get("ProductCode") != null)
								productCode = (String) product.get("ProductCode");
							if (product.containsField("ProductName") && product.get("ProductName") != null)
								productName = (String) product.get("ProductName");
							if (product.containsField("ProductDescription") && product.get("ProductDescription") != null)
								productDescription = (String) product.get("ProductDescription");
							
							product1.append("productCode", productCode);
							product1.append("productName", productName);
							product1.append("productDescription", productDescription);
							products1.add(product1);
							
						
						}
						numberOfProducts += products.size();
						
					}
				}
				provider1.append("products", products1);
				providers1.add(provider1);
				}
			}
		
		}
		}
		
		if (isQualRequest)
		{
			if (payload.containsField("ServiceLocation") && payload.get("ServiceLocation") != null) 
			{
				serviceLocation = (DBObject) payload.get("ServiceLocation");
			}
		}
		
		String type = "APICALL";
		if (apiCall.containsField("type") && apiCall.get("type") != null)
			type = (String) apiCall.get("type");
		
		String qual = "NULL";
		if (apiCall.containsField("qual") && apiCall.get("qual") != null)
			qual = (String) apiCall.get("qual");
		
		
		output.append("type", type);
		output.append("dateTime", dateTime);
		output.append("endpoint", endpoint);
		output.append("isResponse", isResponse);
		output.append("isRequest", isRequest);
		output.append("isError", isError);
		output.append("duration", duration);
		output.append("buckets", buckets);
		output.append("buckete", buckete);
		output.append("id", apiCall_id);
		output.append("session", session_id);
		output.append("isTimedout", isTimedout);
		output.append("isQual", isQual);
		output.append("qual", qual);
		output.append("isPositiveQual", isPositiveQual);
		
		BasicDBObject qualInfo = new BasicDBObject();
		qualInfo.append("isQualResponse", isQualResponse);
		qualInfo.append("isQualRequest", isQualRequest);
		qualInfo.append("isPositiveQual", isPositiveQual);
		qualInfo.append("numberOfProducts", numberOfProducts);
		qualInfo.append("providers", providers1);
		qualInfo.append("serviceLocation", serviceLocation);
		
		//output.append("qualInfo", qualInfo);
		
		
		return output;
	}

	public static BasicDBObject processQual(DBObject qual)
	{
		BasicDBObject output = new BasicDBObject();
		
		String type= "QUAL";
		if (qual.containsField("type") && qual.get("type") != null)
			type = (String) qual.get("type");
		
		String qual_id= "NULL";
		if (qual.containsField("_id") && qual.get("_id") != null)
			qual_id = qual.get("_id").toString();
		
		String dateTime = "NULL";
		if (qual.containsField("dateTime") && qual.get("dateTime") != null)
			dateTime = qual.get("dateTime").toString();
		
		String session_id = "NULL";
		if (qual.containsField("session") && qual.get("session") != null)
			session_id = qual.get("session").toString();
		
		boolean wasSuccessful = true;
		if (qual.containsField("wasSuccessful") && qual.get("wasSuccessful") != null)
			wasSuccessful = (boolean) qual.get("wasSuccessful");
		
		DBObject response = null;
		if (qual.containsField("response") && qual.get("response") != null)
			response = (DBObject) qual.get("response");
		
		DBObject request = null;
		if (qual.containsField("request") && qual.get("request") != null)
			request = (DBObject) qual.get("request");
		
		ArrayList<String> providers1 = null;
		if (response.containsField("providers") && response.get("providers") != null)
			providers1 = (ArrayList<String>) response.get("providers");
		
		boolean isPositiveQual = true;
		if (providers1.isEmpty())
			isPositiveQual = false;
		
		DBObject address = null;
		ArrayList<BasicDBObject> providers = null;
		int numberOfProducts = 0;
		
		output.append("type", type);
		output.append("id", qual_id);
		output.append("dateTime", dateTime);
		output.append("session", session_id);
		output.append("wasSuccessful", wasSuccessful);
		output.append("isPositiveQual", isPositiveQual);
		
		return output;
	}
	
	public static BasicDBObject processError(DBObject error)
	{
		BasicDBObject output = new BasicDBObject();
		
		String type = "ERROR";
		if (error.containsField("type") && error.get("type") != null)
			type = (String) error.get("type");
		
		String error_id = "NULL";
		if (error.containsField("_id") && error.get("_id") != null)
			error_id = error.get("_id").toString();
		
		String dateTime = "NULL";
		if (error.containsField("dateTime") && error.get("dateTime") != null)
			dateTime = error.get("dateTime").toString();
		
		String errorCode = "NULL";
		if (error.containsField("errorCode") && error.get("errorCode") != null)
			errorCode = (String) error.get("errorCode");
		
		String errorType = "NULL";
		if (error.containsField("errorType") && error.get("errorType") != null)
			errorType = (String) error.get("errorType");
		
		String errorMessage = "NULL";
		if (error.containsField("errorMessage") && error.get("errorMessage") != null)
			errorMessage = (String) error.get("errorMessage");
		
		String session_id = "NULL";
		if (error.containsField("session") && error.get("session") != null)
			session_id = error.get("session").toString();
		
		String providerCode = "NULL";
		if (error.containsField("providerCode") && error.get("providerCode") != null)
			providerCode = (String) error.get("providerCode");
		
		String providerName = "comcast";
		
		String apicall = "NULL";
		if (error.containsField("apicall") && error.get("apicall") != null)
			apicall = (String) error.get("apicall");
		
		int severity = rand.nextInt(5) + 1;
		
		output.append("type", type);
		output.append("dateTime", dateTime);
		output.append("id", error_id);
		output.append("session", session_id);
		output.append("errorType", errorType);
		output.append("errorCode", errorCode);
		output.append("errorMessage", errorMessage);
		output.append("providerCode", providerCode);
		output.append("providerName", providerName);
		output.append("severity", severity);
		output.append("apicall", apicall);
		
		
		return output;
	}
	
	public static BasicDBObject processOrder(DBObject order)
	{
		BasicDBObject output = new BasicDBObject();
		
		String type = "ORDER";
		if (order.containsField("type") && order.get("type") != null)
			type = (String) order.get("type");
		
		String dateTime = "NULL";
		if (order.containsField("dateTime") && order.get("dateTime") != null)
			dateTime = order.get("dateTime").toString();
		
		String order_id = "NULL";
		if (order.containsField("_id") && order.get("_id") != null)
			order_id = order.get("_id").toString();
		
		String qual_id = "NULL";
		if (order.containsField("qual") && order.get("qual") != null)
			qual_id = order.get("qual").toString();
		
		String session_id= "NULL";
		if (order.containsField("session") && order.get("session") != null)
			session_id = order.get("session").toString();
		
		
		String providerId = "NULL";
		if (order.containsField("providerId") && order.get("providerId") != null)
			providerId = (String) order.get("providerId");
		
		String providerCode = "NULL";
		if (order.containsField("providerCode") && order.get("providerCode") != null)
			providerCode = (String) order.get("providerCode");
		
		String providerName = "comcast";
		
		String trackingId = "NULL";
		if (order.containsField("trackingId") && order.get("trackingId") != null)
			trackingId = (String) order.get("trackingId");
		
		ArrayList<DBObject> confirmations = null;
		if (order.containsField("confirmations") && order.get("confirmations") != null)
			confirmations = (ArrayList<DBObject>) order.get("confirmations");
		String orderConfirmationId = "NULL";
		if (confirmations != null && !confirmations.isEmpty())
			if (confirmations.get(0).containsField("orderConfirmationId") && 
					confirmations.get(0).get("orderConfirmationId") != null )
				orderConfirmationId = (String) confirmations.get(0).get("orderConfirmationId"); 
		
		ArrayList<DBObject> products = null;
		ArrayList<DBObject> products1 = new ArrayList<DBObject>();
		if (order.containsField("products") && order.get("products") != null)
			products = (ArrayList<DBObject>) order.get("products");
		
		if (products != null && !products.isEmpty())
		{
			Iterator it = products.iterator();
			while (it.hasNext())
			{
				DBObject product = (DBObject) it.next();
				BasicDBObject product1 = new BasicDBObject();
				String term = "NULL";
				String voiceServiceExtension = "false";
				String videoServiceExtension = "false";
				String dataServiceExtension = "false";
				String price = "NULL";
				String description = "NULL";
				
				if (product.containsField("voiceServiceExtension") && 
						product.get("voiceServiceExtension") != null)
					voiceServiceExtension = (String) product.get("voiceServiceExtension");
				
				if (product.containsField("videoServiceExtension") && 
						product.get("videoServiceExtension") != null)
					videoServiceExtension = (String) product.get("videoServiceExtension");
				
				if (product.containsField("dataServiceExtension") && 
						product.get("dataServiceExtension") != null)
					dataServiceExtension = (String) product.get("dataServiceExtension");
				
				if (product.containsField("price") && product.get("price") != null)
				{
					DBObject price1 = null;
					price1 = (DBObject) product.get("price");
					if (price1.containsField("promo") && price1.get("promo") != null)
					{
						DBObject promo = null;
						promo = (DBObject) price1.get("promo");
						if (promo.containsField("price") && promo.get("price") != null)
							price = promo.get("price").toString();
						if (promo.containsField("term") && promo.get("term") != null)
							term = promo.get("term").toString();
						if (promo.containsField("description") && promo.get("description") != null)
							description = promo.get("description").toString();
						
					}
				}
				
				String lineItemCount = "NULL";
				if (product.containsField("lineItemCount") && product.get("lineItemCount") != null)
					lineItemCount = (String) product.get("lineItemCount");
		
				String verticalCode = "NULL";
				if (product.containsField("verticalCode") && product.get("verticalCode") != null)
					verticalCode = (String) product.get("verticalCode");
		
				product1.append("voiceServiceExtension", voiceServiceExtension);
				product1.append("videoServiceExtension", videoServiceExtension);
				product1.append("dataServiceExtension", dataServiceExtension);
				product1.append("price", price);
				product1.append("term", term);
				product1.append("description", description);
				product1.append("vertivalCode", verticalCode);
				product1.append("lineItemCount", lineItemCount);
				products1.add(product1);
		
				
			}
		}
	
		output.append("type", type);
		output.append("dateTime", dateTime);
		output.append("id", order_id);
		output.append("qual", qual_id);
		output.append("session", session_id);
		output.append("providerId", providerId);
		output.append("providerCode", providerCode);
		output.append("providerName", providerName);
		output.append("trackingId", trackingId);
		output.append("orderConfirmationId", orderConfirmationId);
		output.append("products", products1);
		
		
		return output;
		
	}
	
	public static BasicDBObject processLead(DBObject lead)
	{
		BasicDBObject output = new BasicDBObject();
		
	    String dateTime = "NULL";
	    if (lead.containsField("dateTime") && lead.get("dateTime") != null)
	    	dateTime = lead.get("dateTime").toString();
		
	    String type = "LEAD";
	    if (lead.containsField("type") && lead.get("type") != null)
	    	type = (String) lead.get("type");
	    

	    String lead_id = "NULL";
	    if (lead.containsField("_id") && lead.get("_id") != null)
	    	lead_id = lead.get("_id").toString();

	    ArrayList<String> sessions = new ArrayList<String>();	   
	    if (lead.containsField("sessions") && lead.get("sessions") != null)
	    {
	    	 ArrayList<ObjectId> sessions1 = null;
	    	sessions1 = (ArrayList<ObjectId>) lead.get("sessions");
	    	if (!sessions1.isEmpty())
	    	{
	    		for (ObjectId session : sessions1)
	    			sessions.add(session.toString());
	    	}
	    }
	    
	    ArrayList<String> visits = new ArrayList<String>();	   
	    if (lead.containsField("visits") && lead.get("visits") != null)
	    {
	    	 ArrayList<ObjectId> visits1 = null;
	    	 visits1 = (ArrayList<ObjectId>) lead.get("visits");
	    	if (!visits1.isEmpty())
	    	{
	    		for (ObjectId visit : visits1)
	    			visits.add(visit.toString());
	    	}
	    }
	    
	    

	    DBObject address = new BasicDBObject();
	    if (lead.containsField("address") && lead.get("address") != null)
	    	address = (DBObject) lead.get("address");
	    

	    String leadSource = "NULL";
	    if (lead.containsField("leadSource") && lead.get("leadSource") != null)
	    	leadSource = (String) lead.get("leadSource");
	    
	    String leadSourceName = "NULL";

	    String providerPhone = "NULL";
	    if (lead.containsField("providerPhone") && lead.get("providerPhone") != null)
	    	providerPhone = (String) lead.get("providerPhone");
	   
	    String providerCode = "NULL";
	    if (lead.containsField("providerCode") && lead.get("providerCode") != null)
	    	providerCode = (String) lead.get("providerCode");
	    

	    DBObject semTracking = new BasicDBObject();
	    if (lead.containsField("semTracking") && lead.get("semTracking") != null)
	    	semTracking = (DBObject) lead.get("semTracking");
	    
		
		
		
		
		output.append("type", type);
		output.append("id", lead_id);
		output.append("dateTime", dateTime);
		output.append("sessions", sessions);
		output.append("visits", visits);
		output.append("address", address);
		output.append("leadSource", leadSource);
		output.append("leadSourceName", leadSourceName);
		output.append("providerPhone", providerPhone);
		output.append("providerCode", providerCode);
		output.append("semTracking", semTracking);
		
		
		
		return output;
	}
	
	public static BasicDBObject parseUserAgent(String userAgentString) 
	{
		String osName = "NULL";
		String browserName = "NULL";
		String deviceTypeName = "NULL";
		String mobileDevice = "NULL";
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

	public static void main(String[] args)
	{
		getFromSessions();
		getFromCustomers();
		getFromApiCalls();
		getFromQuals();
		getFromErrors();
		getFromOrders();
		getFromLeads();
		
	}
}
