package com.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class DistinctLead extends UDAF{
	public static class DistinctLeadEvaluator implements UDAFEvaluator{
		public static class PartialResult{
			String sessionid;
			String datetime;
			String orderid;
		}
		private PartialResult partial;

		public void init(){
			partial=null;
		}
		public boolean iterate(String session_id,String date_time,String order_id){
			
			if(session_id==null||session_id.equalsIgnoreCase("null")){
				return true;
			}
			else{
				if(partial==null){
					partial=new PartialResult();
					partial.sessionid=new String(session_id);
					partial.datetime=new String(date_time);
					if(order_id!=null){
						partial.orderid=new String(order_id);
					}
					return true;
				}
				else{
					if(partial.orderid==null||partial.orderid.equalsIgnoreCase("null")){
						if(order_id==null||order_id.equalsIgnoreCase("null")){
							if(partial.sessionid==null||partial.sessionid.equalsIgnoreCase("null")){
								partial.sessionid=session_id;
								partial.datetime=date_time;
								partial.orderid=order_id;
								return true;
							}
							else{
								if(partial.datetime.compareTo(date_time)<0){
									return true;
								}
								else{
									partial.sessionid=session_id;
									partial.datetime=date_time;
									partial.orderid=order_id;
									return true;
								}
							}
						}
						else{
							partial.sessionid=session_id;
							partial.datetime=date_time;
							partial.orderid=order_id;
							return true;
						}
					}
					else{
						return true;
					}
				}
			}
			
			
		}
		
		public PartialResult terminatePartial(){
			return partial;
		}
		
		public boolean merge(PartialResult other){
			if(other==null){
				return true;
			}	
			else{	
				if(other.sessionid==null||other.sessionid.equalsIgnoreCase("null")){
					return true;
				}
				else{
					if(partial==null){
						partial=new PartialResult();
						partial.sessionid=new String(other.sessionid);
						partial.datetime=new String(other.datetime);
						if(other.orderid!=null){
							partial.orderid=new String(other.orderid);
						}
						return true;
					}
					else{
						if(partial.orderid==null||partial.orderid.equalsIgnoreCase("null")){
							if(other.orderid==null||other.orderid.equalsIgnoreCase("null")){
								if(partial.sessionid==null||partial.sessionid.equalsIgnoreCase("null")){
									partial.sessionid=other.sessionid;
									partial.datetime=other.datetime;
									partial.orderid=other.orderid;
									return true;
								}
								else{
									if(partial.datetime.compareTo(other.datetime)<0){
										return true;
									}
									else{
										partial.sessionid=other.sessionid;
										partial.datetime=other.datetime;
										partial.orderid=other.orderid;
										return true;
									}
								}
							}
							else{
								partial.sessionid=other.sessionid;
								partial.datetime=other.datetime;
								partial.orderid=other.orderid;
								return true;
							}
						}
						else{
							return true;
						}
					}
				}
			}
		}
		
		public String terminate(){
			if(partial==null||partial.sessionid==null){
				return null;
			}
			else{
				return new String(partial.sessionid);
			}
		}
		
	}
}