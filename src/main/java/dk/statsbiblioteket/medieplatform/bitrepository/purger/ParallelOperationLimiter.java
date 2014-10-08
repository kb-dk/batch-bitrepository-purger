package dk.statsbiblioteket.medieplatform.bitrepository.purger;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.statsbiblioteket.medieplatform.bitrepository.purger.DeleteJob.JobStatus;

/**
 * Provides functionality for limiting the number of operations by providing a addJob method which
 * will block if a specified limit is reached.
 */
 public class ParallelOperationLimiter {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final ResultHandler resultHandler;
    private final BlockingQueue<DeleteJob> activeOperations;
    private final int secondsToWaitForFinish;

    ParallelOperationLimiter(ResultHandler resultHandler, int limit, int timeToWaitForFinish) {
        this.resultHandler = resultHandler;
        activeOperations = new LinkedBlockingQueue<>(limit);
        this.secondsToWaitForFinish = timeToWaitForFinish;
    }

    /**
     * Will block until the if the activeOperations queue limit is exceeded and unblock when a job is removed.
     * @param job The job in the queue.
     */
    void addJob(DeleteJob job) {
        try {
            activeOperations.put(job);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Gets the PutJob for fileID
     * @param fileID The fileID to get the job for
     * @return PutJob the PutJob with relevant info for the job. 
     */
    DeleteJob getJob(String fileID) {
        Iterator<DeleteJob> iter = activeOperations.iterator();
        DeleteJob job = null;
        while(iter.hasNext()) {
            job = iter.next();
            if(job.getFileID().equals(fileID)) {
                break;
            }
        }
        return job;
    }

    /**
     * Removes a job from the queue
     * @param job the PutJob to remove 
     */
    void removeJob(DeleteJob job) {
        activeOperations.remove(job);
    }

    /**
     * Wait until there's no more jobs to be processed, or until timeout occurs 
     */
    public void waitForFinish() {
        int secondsWaiting = 0;
        while (!activeOperations.isEmpty() && (secondsWaiting++ < secondsToWaitForFinish)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                //No problem
            }
        }
        if (secondsWaiting > secondsToWaitForFinish) {
            String message = "Timeout(" + secondsToWaitForFinish+ "s) waiting for last files (" + Arrays.toString(activeOperations.toArray()) + ") to complete.";
            log.warn(message);
            for (DeleteJob job : activeOperations.toArray(new DeleteJob[activeOperations.size()])) {
                job.setStatus(JobStatus.TIMEOUT);
                resultHandler.addFailure(job);
            }
        }
    }
}