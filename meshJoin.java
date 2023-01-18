package DWH_Project;

import java.sql.*;
import java.io.FileWriter;
import java.util.*;
import org.apache.commons.collections4.multimap.*;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.io.BufferedReader;
import org.apache.commons.collections4.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileReader;


public class My_DWH_Project {

	
	//for getting sqlworkbench username
	static String user() throws SQLException
	{
		Scanner myObj = new Scanner(System.in);  // Create a Scanner object
	    System.out.println("Enter username:");

	    String username = myObj.nextLine();  // Read user input
		
	    return(username);
	}
	
	//for getting sqlworkbench password
	static String pass() throws SQLException
	{
		Scanner myObj = new Scanner(System.in);  
		System.out.println("Enter password:");
	    String pw = myObj.nextLine();  
		
	    return(pw);
	}
	
	
	
	//establishing connections
	static Connection dw_sir(String username,String password) throws SQLException{
		
		
		Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/dwh_test", username,
                password);
        System.out.println("Connected With the Transaction Master database successfully");
        System.out.println("Please wait as data is being input into warehouse. It will show 10000 once all have been entered into the warehouse.");
        return(connection);
	
	}

	public static Connection dw_mine(String username, String password) throws SQLException{
		Connection starware = DriverManager.getConnection("jdbc:mysql://localhost:3306/unon", username,
                password);
        System.out.println("Connected With the My Warehouse database successfully");

        return(starware);
	}
	
	//insertion functions
	public static void insertprod(String pid, String prodname, Statement starwr) throws SQLException{
		try
        {
	      	String x = "insert into product (PRODUCT_ID, PRODUCT_NAME) values ('" + pid + "','" + prodname + "')";
        	starwr.executeUpdate(x);
        } 
		
		catch (SQLException e)
        {

        }
	}
	
	public static void insertsupp(String suppid, String suppname, Statement starwr) throws SQLException{
		try
        {
		
			String x3 = "insert into supplier (SUPPLIER_ID, SUPPLIER_NAME) values ('" + suppid + "','" + suppname.replace("'", "") + "')";
        	starwr.executeUpdate(x3);
        } 
		
		catch (SQLException e)
        {

        }
		}
	
	public static void insertdate(String t_id, String tdate, Statement starwr) throws SQLException{
		String[] date = tdate.split("-");
		
    	
    	try
        {
			String x4 = "insert into trandate (TIME_ID, FDATE, DAYS, MONTHS, QUARTERS, YEARS) values ('" +  t_id + "','" + tdate + "','" + date[2] + "','" + date[1] + "','" + (((Integer.valueOf(date[1])-1)/3)+1) + "','" + date[0] + "')";
        	starwr.executeUpdate(x4);
        } 
		
		catch (SQLException e)
        {

        }
		}
	
	public static void insertstore(String storeid, String storename, Statement starwr) throws SQLException{
		try
        {
			
			String x2 = "insert into store (STORE_ID, STORE_NAME) values ('" + storeid + "','" + storename + "')";
        	starwr.executeUpdate(x2);
        } 
		
		catch (SQLException e)
        {

        }
	}
	
	public static void insertcust(String custid, String custname, Statement starwr) throws SQLException{
		try
        {
			String x1 = "insert into customer (CUSTOMER_ID, CUSTOMER_NAME) values ('" + custid + "','" + custname + "')";
        	starwr.executeUpdate(x1);
        } 
		
		catch (SQLException e)
        {

        }
	}
	
	public static void insertfact(String pid, String suppid, String t_id, String storeid, String custid, float quant, float price, int tot, Statement starwr) throws SQLException{
		try
        {
	      	String x7 = "insert into fact (prod, supp, date_id, store, cust, quant, price, totsale) values ('" + pid + "','" + suppid + "','" + t_id + "','" + storeid + "','" + custid + "','" + quant + "','" + price + "','" + tot + "')";
        	starwr.executeUpdate(x7);
        } 
		
		catch (SQLException e)
        {
        }
	}

	public static void main(String[] args) throws SQLException{
		
		        
        int transdata = 0;
        int masterdata = 0;
        
        MultiValuedMap<String, Map<String, String>> multi = new ArrayListValuedHashMap<>();
        ArrayBlockingQueue<List<Map<String,String>>> abq = new ArrayBlockingQueue<List<Map<String,String>>>(10);
		
        int count = 0;
        String username=user();
        String password=pass();

        
        Connection mine=dw_mine(username,password);
        Connection sir=dw_sir(username,password);

       
        while(true)
        {
        	if (count == 10000)
        	{
        		break;
        	}

        	if(masterdata == 100)
        	{
        		masterdata = 0;
        	}

        	//for execution of queries
        	Statement smt = sir.createStatement();
        	Statement smt1 = sir.createStatement();
        	
        	Statement starwr = mine.createStatement();
        	Statement starwr1 = mine.createStatement();
       
        	String maststr = "select * from products limit " + masterdata + ",10";
        	String transstr = "select * from transactions limit " + transdata + ",50";
        	
        	//data from transactions
            ResultSet transacdata=smt.executeQuery(transstr);
            
            
            //data from master
            ResultSet mastdata=smt1.executeQuery(maststr);
        	
            
            List<Map<String, String>> list = new ArrayList<Map<String, String>>(); 
            
        	while(transacdata.next())
        	{
            	//mapping data into simple hash map
        		Map<String, String> transmap = new HashMap<String, String>();
            	
            	//putting into map
            	String transid = transacdata.getString("TRANSACTION_ID");
            	transmap.put("TRANSACTION_ID" , transid);
            	
            	String pid = transacdata.getString("PRODUCT_ID");
            	transmap.put("PRODUCT_ID" , pid);
            	
            	String custid = transacdata.getString("CUSTOMER_ID");
            	transmap.put("CUSTOMER_ID" , custid);
            	
            	String storeid = transacdata.getString("STORE_ID");
            	transmap.put("STORE_ID" , storeid);

            	String storename = transacdata.getString("STORE_NAME");
            	transmap.put("STORE_NAME" , storename);

				String tid = transacdata.getString("TIME_ID");
            	transmap.put("TIME_ID" , tid);
            	
            	String tdate = transacdata.getString("T_DATE");
            	transmap.put("T_DATE" , tdate);

            	String quantity = transacdata.getString("QUANTITY");
            	transmap.put("QUANTITY" , quantity);
           	
            	list.add(transmap);
            	
            	multi.put(transmap.get("PRODUCT_ID"), transmap);
				//multi2.put(transmap.get("CUSTOMER_ID"), transmap);
        	}
        	
        	//removing from map if abq size exceeds 10 which is coming from master
        	if (abq.size()>=10)
            {
            	for(Map<String,String> Maps: abq.poll())
            	{
            		multi.removeMapping(Maps.get("PRODUCT_ID"), Maps);
            	}
            }

            	abq.add(list);
            
        	
        	while(mastdata.next())
        	{
        		for(Map<String, String> getter: multi.get(mastdata.getString("PRODUCT_ID")))
        		{
        			String prodid = mastdata.getString("PRODUCT_ID");
        			String prodname = mastdata.getString("PRODUCT_NAME");
        			String suppid = mastdata.getString("SUPPLIER_ID");
        			String suppname = mastdata.getString("SUPPLIER_NAME");
        			float price = mastdata.getFloat("PRICE");
                    float quant = Float.parseFloat(getter.get("QUANTITY"));
        			
        			//for joining
        			String transid = getter.get("TRANSACTION_ID");
		        	String pid = getter.get("PRODUCT_ID");
		        	String custid = getter.get("CUSTOMER_ID");
		        	//String custname = getter.get("CUSTOMER_NAME");
		        	String storeid = getter.get("STORE_ID");
					String storename = getter.get("STORE_NAME");
		        	String t_date = getter.get("T_DATE");
		        	String t_id = getter.get("TIME_ID");
		        	String custname = "Abdullah";

        			
		        	//calculating the total price
		        	int tot = (int)(price * quant);
		        	count++;
		        	
		        	//calling insertion functions
		    		insertcust(custid, custname, starwr);
		    		insertstore(storeid, storename, starwr);
		    		insertdate(t_id, t_date, starwr);
		    		insertsupp(suppid, suppname, starwr);
		    		insertprod(pid, prodname, starwr);
		            insertfact(pid, suppid, t_id, storeid, custid, quant, price, tot, starwr);    			        	
        		}
        		
        	}
        	
            //"relevant iteration"
        	masterdata = masterdata + 10;
	        transdata = transdata + 50; 
        }
    	System.out.println(count);

	}
	
}