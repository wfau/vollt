package uws.service.wait;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.servlet.http.HttpServletRequest;

import uws.job.UWSJob;
import uws.job.user.JobOwner;

/*
 * This file is part of UWSLibrary.
 * 
 * UWSLibrary is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * UWSLibrary is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with UWSLibrary.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Copyright 2015 - Astronomisches Rechen Institut (ARI)
 */

/**
 * <p>This {@link BlockingPolicy} extends the {@link LimitedBlockingPolicy}. So, it proposes to limit the blocking duration
 * but it especially limits the number of blocked threads for a given job and user.</p>
 * 
 * <h3>Blocked by Job AND User</h3>
 * 
 * <p>
 * 	As said, the limit on the number of threads is set ONLY for a given job AND a given user. Thus, an attempt to
 * 	access the same job with the blocking behavior is limited but even if the limit is reached the same user can
 * 	benefit from the blocking behavior with another job. Similarly a different user can access to the same job
 * 	even if the limit is reached for another user.
 * </p>
 * 
 * <p><i>Note:
 * 	If no user is identified, the IP address will be used instead.
 * </i></p>
 * 
 * <h3>What happens when the limit is reached?</h3>
 * 
 * <p>In a such case, 2 strategies are proposed:</p>
 * <ul>
 * 	<li>unblock the oldest blocked thread and accept the new blocking</li>
 * 	<li>do not block for the new asked blocking (then {@link #block(Thread, long, UWSJob, JobOwner, HttpServletRequest)} will return 0)</li>
 * </ul>
 * 
 * <p>
 * 	The strategy to use MUST be specified at creation using {@link #UserLimitedBlockingPolicy(long, int, boolean)}
 * 	with a third parameter set to <code>true</code> to unblock the oldest thread if needed, or <code>false</code>
 * 	to prevent blocking if the limit is reached.
 * </p>
 * 
 * @author Gr&eacute;gory Mantelet (ARI)
 * @version 4.2 (05/2015)
 * @since 4.2
 */
public class UserLimitedBlockingPolicy extends LimitedBlockingPolicy {
	
	/** Default number of allowed blocked threads. */
	public final static int DEFAULT_NB_MAX_BLOCKED = 3;
	
	/** The maximum number of blocked thread for a given job and user. */
	protected final int maxBlockedThreadsByUser;
	
	/** List of all blocked threads.
	 * Keys are an ID identifying a given job AND a given user
	 * (basically: jobId+";"+userId ;  see {@link #buildKey(UWSJob, JobOwner, HttpServletRequest)} for more details).
	 * Values are fixed-length queues of blocked threads. */
	protected final Map<String, BlockingQueue<Thread>> blockedThreads;
	
	/** Indicate what should happen when the maximum number of threads for a given job and user is reached.
	 * <code>true</code> to unblock the oldest blocked thread in order to allow the new blocking.
	 * <code>false</code> to forbid new blocking. */
	protected final boolean unblockOld;
	
	/**
	 * <p>Build a default {@link UserLimitedBlockingPolicy}.</p>
	 * 
	 * <p>
	 * 	This instance will limit the number of blocked threads by user and job to the default
	 * 	value (i.e. {@value #DEFAULT_NB_MAX_BLOCKED}) and will limit the blocking duration to the default timeout
	 * 	(see {@link LimitedBlockingPolicy#DEFAULT_TIMEOUT}).
	 * </p>
	 * 
	 * <p>When the limit of threads is reached, the oldest thread is unblocked in order to allow the new incoming blocking.</p>
	 */
	public UserLimitedBlockingPolicy() {
		this(DEFAULT_TIMEOUT, DEFAULT_NB_MAX_BLOCKED);
	}

	/**
	 * <p>Build a {@link UserLimitedBlockingPolicy} which will limits the blocking duration to the given value
	 * and will limit the number of blocked threads by job and user to the default value
	 * (i.e. {@value #DEFAULT_NB_MAX_BLOCKED}).</p>
	 * 
	 * <p>When the limit of threads is reached, the oldest thread is unblocked in order to allow the new incoming blocking.</p>
	 * 
	 * @param timeout	Maximum blocking duration (in seconds).
	 *               	<i>If &lt; 0, the default timeout (see {@link LimitedBlockingPolicy#DEFAULT_TIMEOUT}) will be set.</i>
	 * 
	 * @see LimitedBlockingPolicy#LimitedBlockingPolicy(long)
	 */
	public UserLimitedBlockingPolicy(final long timeout) {
		this(timeout, DEFAULT_NB_MAX_BLOCKED);
	}

	/**
	 * <p>Build a {@link UserLimitedBlockingPolicy} which will limits the blocking duration to the given value
	 * and will limit the number of blocked threads by job and user to the given value.</p>
	 * 
	 * <p>When the limit of threads is reached, the oldest thread is unblocked in order to allow the new incoming blocking.</p>
	 * 
	 * @param timeout		Maximum blocking duration (in seconds).
	 *               		<i>If &lt; 0, the default timeout (see {@link LimitedBlockingPolicy#DEFAULT_TIMEOUT}) will be set.</i>
	 * @param maxNbBlocked	Maximum number of blocked threads allowed for a given job and a given user.
	 *                    	<i>If &le; 0, this parameter will be ignored and the default value
	 *                    	(i.e. {@value #DEFAULT_NB_MAX_BLOCKED}) will be set instead.</i>
	 */
	public UserLimitedBlockingPolicy(final long timeout, final int maxNbBlocked) {
		this(timeout, maxNbBlocked, true);
	}

	/**
	 * <p>Build a {@link UserLimitedBlockingPolicy} which will limits the blocking duration to the given value
	 * and will limit the number of blocked threads by job and user to the given value.</p>
	 * 
	 * <p>
	 * 	When the limit of threads is reached, the oldest thread is unblocked if the 3rd parameter is <code>true</code>,
	 * 	or new incoming blocking will be forbidden if this parameter is <code>false</code>.
	 * </p>
	 * 
	 * @param timeout		Maximum blocking duration (in seconds).
	 *               		<i>If &lt; 0, the default timeout (see {@link LimitedBlockingPolicy#DEFAULT_TIMEOUT}) will be set.</i>
	 * @param maxNbBlocked	Maximum number of blocked threads allowed for a given job and a given user.
	 *                    	<i>If &le; 0, this parameter will be ignored and the default value
	 *                    	(i.e. {@value #DEFAULT_NB_MAX_BLOCKED}) will be set instead.</i>
	 * @param unblockOld	Set the behavior to adopt when the maximum number of threads is reached for a given job and user.
	 *                  	<code>true</code> to unblock the oldest thread in order to allow the new incoming blocking,
	 *                  	<code>false</code> to forbid the new incoming blocking.
	 */
	public UserLimitedBlockingPolicy(final long timeout, final int maxNbBlocked, final boolean unblockOld) {
		super(timeout);
		maxBlockedThreadsByUser = (maxNbBlocked <= 0) ? DEFAULT_NB_MAX_BLOCKED : maxNbBlocked;
		blockedThreads = Collections.synchronizedMap(new HashMap<String, BlockingQueue<Thread>>());
		this.unblockOld = unblockOld;
	}
	
	/**
	 * <p>Build the key for the map {@link #blockedThreads}.</p>
	 * 
	 * <p>The built key is basically: jobId + ";" + userId.</p>
	 * 
	 * <p><i>Note:
	 * 	If no user is logged in or if the user is not specified here or if it does not have any ID, the IP address
	 * 	of the HTTP client will be used instead.
	 * </i></p>
	 * 
	 * @param job		Job associated with the request to block.
	 *           		<b>MUST NOT be NULL.</b>
	 * @param user		User who asked the blocking behavior.
	 *            		<i>If NULL (or it has a NULL ID), the IP address of the HTTP client will be used.</i>
	 * @param request	HTTP request which should be blocked.
	 *               	<i>SHOULD NOT be NULL.</i>
	 * 
	 * @return	The corresponding map key.
	 *        	<i>NEVER NULL.</i>
	 */
	protected final String buildKey(final UWSJob job, final JobOwner user, final HttpServletRequest request){
		if (user == null || user.getID() == null){
			if (request == null)
				return job.getJobId()+";???";
			else
				return job.getJobId()+";"+request.getRemoteAddr();
		}else
			return job.getJobId()+";"+user.getID();
	}
	
	@Override
	public long block(final Thread thread, final long userDuration, final UWSJob job, final JobOwner user, final HttpServletRequest request) {
		// Get the ID of the blocking (job+user):
		String id = buildKey(job, user, request);
		
		// Get the corresponding queue (if any):
		BlockingQueue<Thread> queue = blockedThreads.get(id);
		if (queue == null)
			queue = new ArrayBlockingQueue<Thread>(maxBlockedThreadsByUser);
		
		// Try to add the recently blocked thread:
		if (!queue.offer(thread)){
			/* If it fails, 2 strategies are possible: */
			// 1/ Unblock the oldest blocked thread and add the given thread into the queue:
			if (unblockOld){
				// Get the oldest blocked thread:
				Thread old = queue.poll();
				// Wake it up // Unblock it:
				if (old != null){
					synchronized(old){
						old.notifyAll();
					}
				}
				// Add the thread into the queue:
				queue.offer(thread);
			}
			// 2/ The given thread CAN NOT be blocked because too many threads for this job and user are already blocked => unblock it!
			else
				return 0;
		}
		
		// Add the queue into the map:
		blockedThreads.put(id, queue);
		
		// Return the eventually limited duration to wait:
		return super.block(thread, userDuration, job, user, request);
		
	}

	@Override
	public void unblocked(final Thread unblockedThread, final UWSJob job, final JobOwner user, final HttpServletRequest request) {
		// Get the ID of the blocking (job+user):
		String id = buildKey(job, user, request);
		
		// Get the corresponding queue (if any):
		BlockingQueue<Thread> queue = blockedThreads.get(id);
		
		if (queue != null){
			Iterator<Thread> it = queue.iterator();
			// Search for the corresponding item inside the queue:
			while(it.hasNext()){
				// When found...
				if (it.next().equals(unblockedThread)){
					// ...remove it from the queue:
					it.remove();
					// If the queue is now empty, remove the queue from the map:
					if (queue.isEmpty())
						blockedThreads.remove(id);
					return;
				}
			}
		}
	}
	
	
}
