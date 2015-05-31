package uws.service.actions;

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
 * Copyright 2012-2015 - UDS/Centre de Données astronomiques de Strasbourg (CDS),
 *                       Astronomisches Rechen Institut (ARI)
 */

import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import uws.UWSException;
import uws.job.ExecutionPhase;
import uws.job.UWSJob;
import uws.job.serializer.UWSSerializer;
import uws.job.user.JobOwner;
import uws.service.UWSService;
import uws.service.UWSUrl;
import uws.service.log.UWSLog.LogLevel;
import uws.service.wait.BlockingPolicy;
import uws.service.wait.WaitObserver;

/**
 * <p>The "Get Job" action of a UWS.</p>
 * 
 * <p><i><u>Note:</u> The corresponding name is {@link UWSAction#JOB_SUMMARY}.</i></p>
 * 
 * <p>This action returns the summary of the job specified in the given UWS URL.
 * This summary is serialized by the {@link UWSSerializer} choosed in function of the HTTP Accept header.</p>
 * 
 * @author Gr&eacute;gory Mantelet (CDS;ARI)
 * @version 4.2 (05/2015)
 */
public class JobSummary extends UWSAction {
	private static final long serialVersionUID = 1L;
	
	/** Name of the parameter which allows the blocking behavior (for a specified or unlimited duration)
	 * of a JobSummary request.
	 * @since 4.2 */
	public final static String WAIT_PARAMETER = "WAIT";

	public JobSummary(UWSService u){
		super(u);
	}

	/**
	 * @see UWSAction#JOB_SUMMARY
	 * @see uws.service.actions.UWSAction#getName()
	 */
	@Override
	public String getName(){
		return JOB_SUMMARY;
	}

	@Override
	public String getDescription(){
		return "Lets getting a summary of the specified job. (URL: {baseUWS_URL}/{jobListName}/{job-id}, Method: HTTP-GET, No parameter)";
	}

	/**
	 * Checks whether:
	 * <ul>
	 * 	<li>a job list name is specified in the given UWS URL <i>(<u>note:</u> the existence of the jobs list is not checked)</i>,</li>
	 * 	<li>a job ID is given in the UWS URL <i>(<u>note:</u> the existence of the job is not checked)</i>,</li>
	 * 	<li>there is no job attribute,</li>
	 * 	<li>the HTTP method is HTTP-GET.</li>
	 * </ul>
	 * 
	 * @see uws.service.actions.UWSAction#match(uws.service.UWSUrl, java.lang.String, javax.servlet.http.HttpServletRequest)
	 */
	@Override
	public boolean match(UWSUrl urlInterpreter, JobOwner user, HttpServletRequest request) throws UWSException{
		return (urlInterpreter.hasJobList() && urlInterpreter.hasJob() && !urlInterpreter.hasAttribute() && request.getMethod().equalsIgnoreCase("get"));
	}

	/**
	 * Gets the specified job <i>(and throw an error if not found)</i>,
	 * chooses the serializer and write the serialization of the job in the given response.
	 * 
	 * @see #getJob(UWSUrl, String)
	 * @see UWSService#getSerializer(String)
	 * @see UWSJob#serialize(ServletOutputStream, UWSSerializer)
	 * 
	 * @see uws.service.actions.UWSAction#apply(uws.service.UWSUrl, java.lang.String, javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	@Override
	public boolean apply(UWSUrl urlInterpreter, JobOwner user, HttpServletRequest request, HttpServletResponse response) throws UWSException, IOException{
		// Get the job:
		UWSJob job = getJob(urlInterpreter);

		// Block if necessary:
		JobSummary.block(uws.getWaitPolicy(), request, job, user);

		// Write the job summary:
		UWSSerializer serializer = uws.getSerializer(request.getHeader("Accept"));
		response.setContentType(serializer.getMimeType());
		try{
			job.serialize(response.getOutputStream(), serializer, user);
		}catch(Exception e){
			if (!(e instanceof UWSException)){
				getLogger().logUWS(LogLevel.ERROR, urlInterpreter, "SERIALIZE", "Can not serialize the job \"" + job.getJobId() + "\"!", e);
				throw new UWSException(UWSException.INTERNAL_SERVER_ERROR, e, "Can not format properly the job \"" + job.getJobId() + "\"!");
			}else
				throw (UWSException)e;
		}

		return true;
	}
	
	/**
	 * <p>Block the current thread until the specified duration is elapsed or
	 * if the execution phase of the target job changes.</p>
	 * 
	 * <p><i>Note:
	 * 	This function will have no effect if the given thread, the given HTTP request or the given job is NULL.
	 * </i></p>
	 * 
	 * @param policy		Strategy to adopt for the blocking behavior.
	 *              		<i>If NULL, the standard blocking behavior will be performed:
	 *              		block the duration (eventually unlimited) specified by the user.</i>
	 * @param req			The HTTP request which asked for the blocking.
	 *           			<b>MUST NOT be NULL.</b> 
	 * @param job			The job associate with the HTTP request.
	 *           			<b>MUST NOT be NULL.</b>
	 * @param user			The user who asked for the blocking behavior.
	 *            			<i>NULL if no user is logged in.</i>
	 * 
	 * @since 4.2
	 */
	public static void block(final BlockingPolicy policy, final HttpServletRequest req, final UWSJob job, final JobOwner user){
		if (req == null || job == null)
			return;
		
		/* BLOCKING/WAIT BEHAVIOR:
		 * this HTTP-GET request should block until either the specified time (or the timeout) is reached
		 * or if the job phase changed: */
		if (req.getParameter("WAIT") != null && (job.getPhase() == ExecutionPhase.PENDING || job.getPhase() == ExecutionPhase.QUEUED || job.getPhase() == ExecutionPhase.EXECUTING)){
			Thread threadToBlock = Thread.currentThread();
			WaitObserver observer = null;
			long waitingTime = 0;
			
			// Fetch the WAIT parameter:
			try{
				String str = req.getParameter("WAIT");
				/* note: if the parameter is missing or if no value is set, it should be understood as an unlimited duration */
				if (str == null || str.trim().length() == 0)
					waitingTime = -1;
				else
					waitingTime = Long.parseLong(req.getParameter("WAIT"));
			}catch(NumberFormatException nfe){}
			
			// Eventually limit the waiting time in function of the chosen policy:
			if (policy != null)
				waitingTime = policy.block(threadToBlock, waitingTime, job, user, req);
			
			// Blocking ONLY IF the duration is NOT NULL (i.e. wait during 0 seconds):
			if (waitingTime != 0){
				try{
					// Watch the job in order to detect an execution phase modification:
					observer = new WaitObserver(threadToBlock);
					job.addObserver(observer);

					// If the job is still processing, then wait the specified time:
					if (job.getPhase() == ExecutionPhase.PENDING || job.getPhase() == ExecutionPhase.QUEUED || job.getPhase() == ExecutionPhase.EXECUTING){
						synchronized(threadToBlock){
							// Limited duration:
							if (waitingTime > 0)
								threadToBlock.wait(waitingTime*1000);
							// "Unlimited" duration (the wait will stop only if the job phase changes):
							else
								threadToBlock.wait();
						}
					}
							
				}catch(InterruptedException ie){
					/* If the WAIT has been interrupted, the blocking
					 * is stopped and nothing special should happen. */
				}
				/* Clear all retained resources. */
				finally{
					// Do not observe any more the job:
					if (observer != null)
						job.removeObserver(observer);
					
					// Notify the BlockingPolicy that this Thread is no longer blocked:
					if (policy != null)
						policy.unblocked(threadToBlock, job, user, req);
				}
			}
		}
	}

}
