package uws.service.wait;

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
 * In this {@link BlockingPolicy}, the blocking may be limited by a maximum waiting time.
 * Thus, unlimited blocking may be prevented. If no timeout is specified at creation, a
 * default one is set: {@link #DEFAULT_TIMEOUT} (= {@value #DEFAULT_TIMEOUT} seconds).
 * 
 * @author Gr&eacute;gory Mantelet (ARI)
 * @version 4.2 (05/2015)
 * @since 4.2
 */
public class LimitedBlockingPolicy implements BlockingPolicy {
	
	/** Default timeout (in seconds) set by this policy at creation if none is specified. */
	public final static long DEFAULT_TIMEOUT = 60;
	
	/** Maximum blocking duration (in seconds).
	 * <i>This attribute is set by default to {@link #DEFAULT_TIMEOUT}. */
	protected long timeout = DEFAULT_TIMEOUT;

	/**
	 * Build a blocking policy with the default timeout (= {@value #DEFAULT_TIMEOUT} seconds).
	 */
	public LimitedBlockingPolicy() {}
	
	/**
	 * Build a {@link BlockingPolicy} which will limit blocking duration to the given value.
	 * 
	 * @param timeout	Maximum blocking duration (in seconds).
	 *               	<i>If &lt; 0, the default timeout (i.e. {@value #DEFAULT_TIMEOUT}) will be set.</i>
	 */
	public LimitedBlockingPolicy(final long timeout) {
		this.timeout = (timeout < 0) ? DEFAULT_TIMEOUT : timeout;
	}

	@Override
	public long block(final Thread thread, final long userDuration, final UWSJob job, final JobOwner user, final HttpServletRequest request) {
		return (userDuration < 0 || userDuration > timeout) ? timeout : userDuration;
	}

	@Override
	public void unblocked(final Thread unblockedThread, final UWSJob job, final JobOwner user, final HttpServletRequest request) {}

}
