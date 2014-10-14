/**
 * 
 */
package com.trendrr.beanstalk;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//import com.trendrr.common.DynMap;

/**
 * @author dustin norlander
 *
 */
public class BeanstalkClient {

	protected Log log = LogFactory.getLog(BeanstalkClient.class);
	
	protected BeanstalkConnection con;
	private boolean inited = false;
	boolean reap = false; //this will tell the pool to reap it when returned
	
	protected String addr;
	protected int port;
	protected String tube;
	
	
	public BeanstalkClient(BeanstalkConnection con) {
		this.con = con;
		this.inited = true;
	}
	
	public BeanstalkClient(String addr, int port) {
		this(addr, port, null);
	}
	
	public BeanstalkClient(String addr, int port, String tube) {
		this.addr = addr;
		this.port = port;
		this.tube = tube;
	}
	
	/**
	 * will return the connection to the pool, or close the underlying socket if this
	 * did not come from a pool
	 */
	public void close() {
		if (this.con != null) {
			this.con.close();
		}
	}
	
	private void init() throws BeanstalkException{
		if (inited) {
			return;
		}
		
		try {
			this.inited = true;
			this.con = new BeanstalkConnection();
			this.con.connect(addr, port);
			if (this.tube != null) {
				this.useTube(tube);
				this.watchTube(tube);
				this.ignoreTube("default"); //remove the default tube from watchlist
			}
		} catch (BeanstalkException x) {
			throw x;
		} 
	}
	
	public void useTube(String tube) throws BeanstalkException{
		try {			
			this.init();
			con.write("use " + tube + "\r\n");
			String line = con.readControlResponse();
			log.debug(line);
			if (line.startsWith("USING")) {
				return;
			}
			throw new BeanstalkException(line);
		} catch (BeanstalkDisconnectedException x) {
			this.reap = true; //reap that shit..
			throw x;
		} catch (BeanstalkException x) {
			throw x;
		}
	}
	
	public void watchTube(String tube) throws BeanstalkException{
		try {			
			this.init();
			con.write("watch " + tube + "\r\n");
			
			String line = con.readControlResponse();
			log.debug(line);
			
			if (line.startsWith("WATCHING")) {
				return;
			}
			throw new BeanstalkException(line);
		} catch (BeanstalkDisconnectedException x) {
			this.reap = true; //reap that shit..
			throw x;
		} catch (BeanstalkException x) {
			throw x;
		}
	}
	
	public void ignoreTube(String tube) throws BeanstalkException{
		try {			
			this.init();
			con.write("ignore " + tube + "\r\n");
			String line = con.readControlResponse();
			log.debug(line);
			
			if (line.startsWith("WATCHING")) {
				return;
			}
			throw new BeanstalkException(line);
		} catch (BeanstalkDisconnectedException x) {
			this.reap = true; //reap that shit..
			throw x;
		} 
	}
	
	/**
	 * stats for the current tube
	 * @throws BeanstalkException
	*/ 
	public String tubeStats() throws BeanstalkException {
		return this.tubeStats(this.tube);
	}
	 
    public List<String> listTubes() throws BeanstalkException {
        try {
            this.init();
            String command = "list-tubes\r\n";
            con.write(command);
            String line = con.readControlResponse();
            if (!line.startsWith("OK")) {
                throw new BeanstalkException(line);
            }
            int numBytes = Integer.parseInt(line.split(" ")[1]);
            String response = new String(con.readBytes(numBytes));
            // log.info(response);
            String lines[] = response.split("\\n");
            List<String> result = new ArrayList<String>();
            // remove "- "
            for (String tube : lines) {
                if (tube.startsWith("- ")) {
                    result.add(tube.substring(2));
                }
            }
            return result;
        } catch (BeanstalkDisconnectedException x) {
            this.reap = true; // reap that shit..
            throw x;
        }
    }
	 
	public String tubeStats(String tube) throws BeanstalkException {
		try {			
			this.init();
			String command = "stats-tube " + tube + "\r\n";
//			log.info(command);
			con.write(command);
			
			String line = con.readControlResponse();
			
			
//			log.info(line);
			
			if (!line.startsWith("OK")) {
				throw new BeanstalkException(line);
			}
			int numBytes = Integer.parseInt(line.split(" ")[1]);
			String response = new String(con.readBytes(numBytes));
			
			
			log.info(response);
			
			return response;
		} catch (BeanstalkDisconnectedException x) {
			this.reap = true; //reap that shit..
			throw x;
		} 
	}
	/**
	 * Puts a task into the currently used queue (see {@link #useTube(String)}.
	 * @param priority The job priority, from 0 to 2^32. Most urgent = 0, least urgent = 4294967295.
	 * @param delay The time the server will wait before putting the job on the ready queue.
	 * @param ttr The job time-to-run. The server will automatically release the job after this TTR (in seconds)
	 *   after a client reserves it.
	 * @param data The job data.
	 * @return The id of the inserted job.
	 * @throws BeanstalkException If an unexpected response is received from the server, or other unexpected
	 * 	 problem occurs.
	 */
	public long put(long priority, int delay, int ttr, byte[] data) throws BeanstalkException{
		try {			
			this.init();
			String command = "put " + priority + " " + delay + " " + ttr + " " + data.length + "\r\n";
//			log.info(command);
			
			ByteArrayOutputStream buf = new ByteArrayOutputStream();
			buf.write(command.getBytes());
			buf.write(data);
			buf.write("\r\n".getBytes());
			
			con.write(buf.toByteArray());

//			String line = in.readLine();
			String line = con.readControlResponse();
//			log.info("INPUT: " + line);
			
//			log.info("READ RESPONSE IN : " + (new Date().getTime() - start.getTime()) );
			
			if (line.startsWith("INSERTED")) {
				long id = Long.parseLong(line.replaceAll("[^0-9]", ""));
				return id;
			}
			
			//there was an error.
			throw new BeanstalkException(line);
			
		} catch (BeanstalkDisconnectedException x) {
			this.reap = true; //reap that shit..
			throw x;
		} catch (BeanstalkException x) {
			throw x;
		} catch (Exception x) {
			throw new BeanstalkException(x);
		}
	}
	
	public void deleteJob(BeanstalkJob job) throws BeanstalkException {
		deleteJob(job.getId());
	}
	
	public void deleteJob(long id) throws BeanstalkException {
		try {			
			this.init();
			String command = "delete " + id + "\r\n";
			log.debug(this);
			log.debug(command);
			con.write(command);
			String line = con.readControlResponse();
			log.debug(line);
			
			if (line.startsWith("DELETED")) {
				return;	
			}
			throw new BeanstalkException(line);			
		} catch (BeanstalkDisconnectedException x) {
			this.reap = true; //reap that shit..
			throw x;
		}
	}

	/**
	 * Reserves a job from the queue.
	 * @param timeoutSeconds The number of seconds to wait for a job. Null if it should wait until a job is available
	 * @return The head of the queue, or null if the specified timeout elapses before a job is available.
	 * @throws BeanstalkException If an unexpected response is received from the server, or other unexpected
	 * 	 problem occurs.
	 */
	public BeanstalkJob reserve(Integer timeoutSeconds) throws BeanstalkException{
		try {			
			this.init();
			String command = "reserve\r\n";
			if (timeoutSeconds != null) {
				command = "reserve-with-timeout " + timeoutSeconds + "\r\n";
			}
			
			log.debug(this);
			log.debug(command);
			con.write(command);
			String line = con.readControlResponse();
			log.debug(line);
			
			if (line.startsWith("TIMED_OUT")) {
				return null;
			}
			
			if (!line.startsWith("RESERVED")) {
				throw new BeanstalkException(line);
			}

			String[] tmp = line.split("\\s+");
			long id = Long.parseLong(tmp[1]);
			
			int numBytes= Integer.parseInt(tmp[2]);
			
			log.debug("ID : " + id);
			log.debug("numbytes: " + numBytes);
				
			byte[] bytes = con.readBytes(numBytes);
//			log.info("GOT TASK: " + new String(bytes));
			
			return new BeanstalkJob(id, bytes);
		} catch (BeanstalkDisconnectedException x) {
			this.reap = true; //reap that shit..
			throw x;
		} catch (BeanstalkException x) {
			throw x;
		} catch (Exception x) {
			throw new BeanstalkException(x);
		}
	}

    public void release(long id, int priority, int delay) throws BeanstalkException {
        try {
            this.init();
            String command = "release " + id + " " + priority + " " + delay + "\r\n";

            log.debug(this);
            log.debug(command);
            con.write(command);
            String line = con.readControlResponse();
            log.debug(line);

            if (!line.startsWith("RELEASED")) {
                throw new BeanstalkException(line);
            }

        } catch (BeanstalkDisconnectedException x) {
            this.reap = true; //reap that shit..
            throw x;
        } catch (BeanstalkException x) {
            throw x;
        } catch (Exception x) {
            throw new BeanstalkException(x);
        }        
    }
	/**
	 * Releases a job (places it back onto the queue).
	 * @param job The job to release. This job must previously have been reserved.
	 * @param priority The new priority to assign to the released job.
	 * @param delay The number of seconds the server should wait before placing the job onto the ready queue.
	 * @throws BeanstalkException If an unexpected response is received from the server, or other unexpected
	 * 	 problem occurs.
	 */
	public void release(BeanstalkJob job, int priority, int delay) throws BeanstalkException {
	    release(job.getId(), priority, delay);
	}

	/**
	 * Buries a job ("buried" state means the job will not be touched by the server again until "kicked").
	 * @throws BeanstalkException If an unexpected response is received from the server, or other unexpected
	 * 	 problem occurs.
	 * @param job The job to bury. This job must previously have been reserved.
	 * @param priority The new priority to assign to the job.
	 */
	public void bury(BeanstalkJob job, int priority) throws BeanstalkException {
		try {
			this.init();
			String command = "bury " + job.getId() + " " + priority + "\r\n";

			log.debug(this);
			log.debug(command);
			con.write(command);
			String line = con.readControlResponse();
			log.debug(line);

			if (!line.startsWith("BURIED")) {
				throw new BeanstalkException(line);
			}

		} catch (BeanstalkDisconnectedException x) {
			this.reap = true; //reap that shit..
			throw x;
		} catch (BeanstalkException x) {
			throw x;
		} catch (Exception x) {
			throw new BeanstalkException(x);
		}
	}

    /**
     * Returns statistical information for the specified job.
     * @throws BeanstalkException If an unexpected response is received from the server, or other unexpected
     *   problem occurs.
     * @param job The job for which to return the corresponding statistics.
     */
    public Map<String, String> getJobStats(BeanstalkJob job) throws BeanstalkException {
        return getJobStats(job.getId());
    }

    /**
     * Returns statistical information for the job with the specified job id.
     * @throws BeanstalkException If an unexpected response is received from the server, or other unexpected
     *   problem occurs.
     * @param id The job id for which to return the corresponding statistics.
     */
    public Map<String, String> getJobStats(long id) throws BeanstalkException {
        try {
            this.init();
            String command = "stats-job " + id + "\r\n";
            log.debug(this);
            log.debug(command);
            con.write(command);
            String controlResponse = con.readControlResponse();
            log.debug(controlResponse);

            if (!controlResponse.startsWith("NOT_FOUND")) {
                throw new BeanstalkException(controlResponse);
            }

            int numBytes = Integer.parseInt(controlResponse.split(" ")[1]);
            String response = new String(con.readBytes(numBytes));
            log.info(response);
            return buildStatsMap(response);
        } catch (BeanstalkDisconnectedException x) {
            this.reap = true; //reap that shit..
            throw x;
        }
    }

    private Map<String, String> buildStatsMap(String response) throws BeanstalkException {
        Map<String, String> statsMap = new HashMap<String, String>();
        BufferedReader reader = new BufferedReader(new StringReader(response));
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                if (line.contains("---")) { //skip the first line
                    continue;
                }
                int colonIndex = line.indexOf(':');
                String key = line.substring(0, colonIndex);
                String value = line.substring(colonIndex + 2);
                statsMap.put(key, value);
            }
        } catch (IOException e) {
            throw new BeanstalkException(e);
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                log.error(e);
            }
        }
        return statsMap;
    }
}


