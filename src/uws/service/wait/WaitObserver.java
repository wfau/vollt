package uws.service.wait;

import uws.UWSException;
import uws.job.ExecutionPhase;
import uws.job.JobObserver;
import uws.job.UWSJob;

/**
 * 
 * @author Gr&eacute;gory Mantelet (ARI)
 * @version 4.2 (05/2015)
 * @since 4.2
 */
public class WaitObserver implements JobObserver {
	private static final long serialVersionUID = 1L;
	
	/** Thread to notify in case an execution phase occurs. */
	private final Thread waitingThread;
	
	/**
	 * Build a {@link JobObserver} which will wake up the given thread when the execution phase of watched jobs changes.
	 * 
	 * @param thread	Thread to notify.
	 */
	public WaitObserver(final Thread thread){
		waitingThread = thread;
	}
	
	@Override
	public void update(final UWSJob job, final ExecutionPhase oldPhase, final ExecutionPhase newPhase) throws UWSException {
		if (oldPhase != null && newPhase != null && oldPhase != newPhase){
			synchronized(waitingThread){
				waitingThread.notify();
			}
		}
	}
	
}