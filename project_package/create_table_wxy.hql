drop database if exists maize cascade;
create database maize;
use maize;
create table leads (
       type string,
       id string,
       date_Time timestamp,
       sessions array<string>,
       visits array<string>,
       address_city string,
       address_line string,
       address_state string,
       address_suite string,
       address_zip string,
       leadSource string,
       leadSourceName string,
       providerPhone string,
       providerCode string,
       semTracking_inboundQueryString string,
       semTracking_adId string,
       semTracking_naturalTerms string,
       semTracking_searchEngine string,
       semTracking_naturalSearchId string,
       semTracking_searchId string,
       semTracking_referrer string,
       semTracking_adGroup string,
       semTracking_campaign string,
       semTracking_terms string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table sessions (
        userAgent_operatingSystem string,
        userAgent_browser string,
        userAgent_deviceType string,
        userAgent_mobileDevice string,
        apiCalls array<string>,
        date_time timestamp,
        type string,
        id string,
        lead string,
        visit string,
        ordered boolean,
        order1 string,
        quals array<string>,
        errs array<string>,
        callCenter boolean,
        agent string,
        agent_callCenterName string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table orders (
       type string,
       date_time timestamp,
       id string,
       qual string,
       session string, 
       providerId string,
       providerCode string,
       providerName string,
       trackingId string,
       API string,
       orderConfirmationId string,
       lineItemDescription string,
       price string,
       term string,
       description string,
       verticalCode string,
       lineItemCount string,
       productName string	   
      )
partitioned by (day string)
STORED AS TEXTFILE;

create table visits(
        type string,
	id string,
	date_Time timestamp,
        lead string,
	session string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table customers (
        type string,
        date_time timestamp,
        id string,
        customerName string,
        email string,
        phoneNumber string,
        leads array<string>
)
partitioned by (day string)
STORED AS TEXTFILE;


create table leadexplode (
       leadid string,
       sessionid string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table leadsession(
       leadid string,
       sessionid string,
       date_time timestamp,
       userAgent_operatingSystem string,
       userAgent_browser string,
       userAgent_deviceType string,
       userAgent_mobileDevice string,
       orderid string,
       agent string,
       callCenter boolean,
       agent_callCenterName string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table leaddistinct(
	leadid string,
	sessionid string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table leadsession2(
       leadid string,
       sessionid string,
       userAgent_operatingSystem string,
       userAgent_browser string,
       userAgent_deviceType string,
       userAgent_mobileDevice string,
       orderid string,
       agent string,
       callCenter boolean,
       agent_callCenterName string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table leadsession3(
       leadid string,
       sessionid string,
       date_time timestamp,
       address_city string,
       address_line string,
       address_state string,
       address_suite string,
       address_zip string,
       leadSource string,
       leadSourceName string,
       providerPhone string,
       semTracking_naturalTerms string,
       semTracking_searchEngine string,
       semTracking_adGroup string,
       semTracking_campaign string,
       semTracking_terms string,
       userAgent_operatingSystem string,
       userAgent_browser string,
       userAgent_deviceType string,
       userAgent_mobileDevice string,
       orderid string,
       agent string,
       callCenter boolean,
       agent_callCenterName string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table leaddetail(
       leadid string,
       sessionid string,
       date_time timestamp,
       address_city string,
       address_line string,
       address_state string,
       address_suite string,
       address_zip string,
       leadSource string,
       leadSourceName string,
       providerPhone string,
       semTracking_naturalTerms string,
       semTracking_searchEngine string,
       semTracking_adGroup string,
       semTracking_campaign string,
       semTracking_terms string,
       userAgent_operatingSystem string,
       userAgent_browser string,
       userAgent_deviceType string,
       userAgent_mobileDevice string,
       agent string,
       callCenter boolean,
       agent_callCentername string,
       orderid string,
       providerId string,
       providerCode string,
       providerName string,
       trackingId string,
       API string,
       lineitemdescription string,
       price string,
       term string,
       description string,
       verticalCode string,
       lineItemCount string,
       productname string	
)
partitioned by (day string)
STORED AS TEXTFILE;


create table sessionlead(
        date_time timestamp,
        sessionid string,
        leadid string,
        userAgent_operatingSystem string,
        userAgent_browser string,
        userAgent_deviceType string,
        userAgent_mobileDevice string,
        orderid string,
        agent string,
        callCenter boolean,
        agent_callCenterName string,
        address_city string,
        address_line string,
        address_state string,
        address_suite string,
        address_zip string,
        leadSource string,
        leadSourceName string,
        providerPhone string,
        semTracking_naturalTerms string,
        semTracking_searchEngine string,
        semTracking_adGroup string,
        semTracking_campaign string,
        semTracking_terms string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table sessiondetail(
        date_time timestamp,
        sessionid string,
        leadid string,
        userAgent_operatingSystem string,
        userAgent_browser string,
        userAgent_deviceType string,
        userAgent_mobileDevice string,
        agent string,
        callCenter boolean,
        agent_callCenterName string,
        address_city string,
        address_line string,
        address_state string,
        address_suite string,
        address_zip string,
        leadSource string,
        leadSourceName string,
        providerPhone string,
        semTracking_naturalTerms string,
        semTracking_searchEngine string,
        semTracking_adGroup string,
        semTracking_campaign string,
        semTracking_terms string,
        orderid string,
        providerId string,
        providerCode string,
        providerName string,
        trackingId string,
        API string,
        orderConfirmationId string,
        lineItemDescription string,
        price string,
        term string,
	description string,
        verticalCode string,
        lineItemCount string,
        productName string    
)
partitioned by (day string)
STORED AS TEXTFILE;


create table ordersession(
       date_time timestamp,
       orderid string,
       providerId string,
       providerCode string,
       providerName string,
       trackingId string,
       API string,
       orderConfirmationId string,
       lineItemDescription string,
       price string,
       term string,
       Description string,
       verticalCode string,
       lineItemCount string ,
       productName string,   
       userAgent_operatingSystem string,
       userAgent_browser string,
       userAgent_deviceType string,
       userAgent_mobileDevice string,
       sessionid string,
       leadid string,
       agent string,
       callCenter boolean,
       agent_callCenterName string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table ordersessionlead(
       date_time timestamp,
       orderid string,
       providerId string,
       providerCode string,
       providerName string,
       trackingId string,
       API string,
       orderConfirmationId string,
       lineItemDescription string,
       price string,
       term string,
       Description string,
       verticalCode string,
       lineItemCount string ,
       productName string,   
       userAgent_operatingSystem string,
       userAgent_browser string,
       userAgent_deviceType string,
       userAgent_mobileDevice string,
       sessionid string,
       agent string,
       callCenter boolean,
       agent_callCenterName string,
       leadid string,
       address_city string,
       address_line string,
       address_state string,
       address_suite string,
       address_zip string,
       leadSource string,
       leadSourceName string,
       providerPhone string,
       semTracking_naturalTerms string,
       semTracking_searchEngine string,
       semTracking_adGroup string,
       semTracking_campaign string,
       semTracking_terms string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table customerexplode(
        customerid string,
        customerName string,
        email string,
        phoneNumber string,
        leadid string
)
partitioned by (day string)
stored as textfile;

create table orderdetail(   
       date_time timestamp,
       orderid string,
       providerId string,
       providerCode string,
       providerName string,
       trackingId string,
       API string,
       orderConfirmationId string,
       lineItemDescription string,
       price string,
       term string,
       Description string,
       verticalCode string,
       lineItemCount string ,
       productName string,   
       userAgent_operatingSystem string,
       userAgent_browser string,
       userAgent_deviceType string,
       userAgent_mobileDevice string,
       sessionid string,
       agent string,
       callCenter boolean,
       agent_callCenterName string,
       leadid string,
       address_city string,
       address_line string,
       address_state string,
       address_suite string,
       address_zip string,
       leadSource string,
       leadSourceName string,
       providerPhone string,
       semTracking_naturalTerms string,
       semTracking_searchEngine string,
       semTracking_adGroup string,
       semTracking_campaign string,
       semTracking_terms string,
       customerid string,
       customerName string,
       email string,
       phoneNumber string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table visitlead(
        visitid string,
	date_Time timestamp,
	leadid string,
	sessionid string,
        semTracking_campaign string,
        semTracking_adGroup string,
        semTracking_terms string,
        semTracking_searchEngine string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table visitratio(
	visitid string,
	date_Time timestamp,
	leadid string,
	sessionid string,
	orderid string,
        semTracking_campaign string,
        semTracking_adGroup string,
        semTracking_terms string,
        semTracking_searchEngine string
)
partitioned by (day string)
STORED AS TEXTFILE;


create table apicalls (
        type string,
        date_time timestamp,
        endpoint string,
        isResponse boolean,
        isRequest boolean,
        isError boolean,
        duration int,
        buckets1 int,
        buckete1 int,
        id string,
        session string,
        isTimedout boolean,
        isQual boolean,
        qual string,
        isQualResponse boolean,
        isQualRequest boolean,
        isPositiveQual boolean,
        numberOfProducts int,
        products array<map<string, string>>,
        serviceLocation map<string, string>
)   
partitioned by (day string)
STORED AS TEXTFILE;

create table quals (
        type string,
        id string,
        date_time timestamp,
        session string,
        wasSuccessful boolean,
        isPositiveQual boolean,
        numberOfProducts int,
        address_city string,
        address_state string,
        address_zip string,
        address_suite string,
        address_line string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table errors (
        type string,
        date_time timestamp,
        id string,
        session string,
        errorType string,
        errorCode string,
        errorMessage string,
        providerCode string, 
        providerName string,
        severity int,
        apicall string
)
partitioned by (day string)
STORED AS TEXTFILE;

create table joined_apicalls (
        type string,
        date_time timestamp,
        endpoint string,
        isResponse boolean,
        isRequest boolean,
        isError boolean,
        duration int,
        buckets1 int,
        buckete1 int,
        id string,
        session string,
        isTimedout boolean,
        isQual boolean,
        qual string,
        isQualResponse boolean,
        isQualRequest boolean,
        isPositiveQual boolean,
        numberOfProducts int,
        products array<map<string, string>>,
        serviceLocation map<string, string>,
        userAgent_operatingSystem string, 
        userAgent_browser string,
        userAgent_deviceType string,
        userAgent_mobileDevice string,
        lead string, 
        visit string, 
        ordered boolean, 
        order1 string,
        callCenter int,
        agent string, 
        agent_callCenterName string,  
        address_city string,  
        address_line string,  
        address_state string,
        address_suite string, 
        address_zip string,   
        leadSource string, 
        leadSourceName string, 
        providerPhone string, 
        providerCode string,
        semTracking_inboundQueryString string,
        semTracking_adId string,
        semTracking_naturalTerms string,
        semTracking_searchEngine string,
        semTracking_naturalSearchId string,
        semTracking_searchId string,
        semTracking_referrer string,
        semTracking_adGroup string,
        semTracking_campaign string,
        semTracking_terms string
 
)   
partitioned by (day string)
STORED AS TEXTFILE;
create table joined_errors (
        type string,
        date_time timestamp,
        id string,
        session string,
        errorType string,
        errorCode string,
        errorMessage string,
        providerCode string, 
        providerName string,
        severity int,
        apicall string,
        userAgent_operatingSystem string, 
        userAgent_browser string,
        userAgent_deviceType string,
        userAgent_mobileDevice string,
        lead string, 
        visit string, 
        ordered boolean, 
        order1 string,
        callCenter int, 
        agent string, 
        agent_callCenterName string,
        address_city string,  
        address_line string,  
        address_state string,
        address_suite string, 
        address_zip string,   
        leadSource string, 
        leadSourceName string, 
        providerPhone string,
        semTracking_inboundQueryString string,
        semTracking_adId string,
        semTracking_naturalTerms string,
        semTracking_searchEngine string,
        semTracking_naturalSearchId string,
        semTracking_searchId string,
        semTracking_referrer string,
        semTracking_adGroup string,
        semTracking_campaign string,
        semTracking_terms string
  
)
partitioned by (day string)
STORED AS TEXTFILE;






