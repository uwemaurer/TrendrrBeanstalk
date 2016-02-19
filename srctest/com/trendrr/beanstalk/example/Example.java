package com.trendrr.beanstalk.example;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.trendrr.beanstalk.BeanstalkClient;
import com.trendrr.beanstalk.BeanstalkException;
import com.trendrr.beanstalk.BeanstalkJob;
import com.trendrr.beanstalk.BeanstalkPool;



/**
 * @author Dustin Norlander
 * @created Nov 15, 2010
 * 
 */
public class Example {

	private static final int PORT = 11300;
    protected static Log log = LogFactory.getLog(Example.class);

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			//clientExample();
		    pooledExample2();
		} catch (BeanstalkException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Example for using an unpooled client
	 * @throws BeanstalkException 
	 */
	public static void clientExample() throws BeanstalkException {
		BeanstalkClient client = new BeanstalkClient("localhost", PORT, "example");
		log.info("Putting a job");
		client.put(1l, 0, 5000, "this is some data".getBytes());
		BeanstalkJob job = client.reserve(60);
		log.info("GOt job: " + job);
		client.deleteJob(job);
		client.close(); //closes the connection
	}
	
	
	public static void pooledExample()  throws BeanstalkException {
		BeanstalkPool pool = new BeanstalkPool("localhost", PORT, 
				30, //poolsize 
			"example" //tube to use
		);
		
		for (int i = 0; i < 100; i++) {
    		BeanstalkClient client = pool.getClient();
    		//log.info("Putting a job");
    		client.put(1l, 0, 5000, ("this is some data " + i).getBytes());
    		BeanstalkJob job = client.reserve(60);
    		log.info("GOt job: " + new String(job.getData()));
    		client.deleteJob(job);
    		client.close();  // returns the connection to the pool
		}
	}

    public static void pooledExample2() throws BeanstalkException {
        BeanstalkPool pool = new BeanstalkPool("localhost", PORT, 30, // poolsize
            "example" // tube to use
        );

        BeanstalkClient client = pool.getClient();
        // log.info("Putting a job");
        log.info(client.tubeStats());
        for (int i = 0; i < 5; i++) {
            byte[] data = new byte[200];
            client.put(1l, 0, 5000, data);
        }
        BeanstalkJob job = client.reserve(60);
        log.info("GOt job: " + new String(job.getData()));
        client.deleteJob(job);
        client.close(); // returns the connection to the pool

    }
}
