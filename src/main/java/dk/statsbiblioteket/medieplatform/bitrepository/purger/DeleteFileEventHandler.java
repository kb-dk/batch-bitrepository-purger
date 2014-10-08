package dk.statsbiblioteket.medieplatform.bitrepository.purger;

import org.bitrepository.client.eventhandler.EventHandler;
import org.bitrepository.client.eventhandler.OperationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.statsbiblioteket.medieplatform.bitrepository.purger.DeleteJob.JobStatus;

/**
 * Event handler class to handle the outcome of the operations. 
 * Only two types of events are handled: COMPLETE and FAILED
 * COMPLETE is handled by just removing the job from the operationLimiter
 * FAILURE is handled by removing the job from the operationLimiter and reporting it as failed.  
 */
public class DeleteFileEventHandler implements EventHandler {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final ParallelOperationLimiter operationLimiter;
    private final ResultHandler resultHandler;
    
    /**
     * Create the event handler
     * @param operationLimiter The operation limiter for access to the job queue
     * @param resultHandler ResultHandler to report failed jobs to.
     */
    DeleteFileEventHandler(ParallelOperationLimiter operationLimiter, ResultHandler resultHandler) {
        this.operationLimiter = operationLimiter;
        this.resultHandler = resultHandler;
    }
    
    @Override
    public void handleEvent(OperationEvent event) {
        if (event.getEventType().equals(OperationEvent.OperationEventType.COMPLETE)) {
            DeleteJob job = getJob(event);
            log.info("Completed deleting file '{}' with checksum '{}'", job.getFileID(), job.getChecksum());
            operationLimiter.removeJob(job);
        } else if (event.getEventType().equals(OperationEvent.OperationEventType.FAILED)) {
            DeleteJob job = getJob(event);
            log.info("Failed deleting file'{}' with checksum '{}'", job.getFileID(), job.getChecksum());
            job.setStatus(JobStatus.FAILED);
            resultHandler.addFailure(job);
            operationLimiter.removeJob(job);
        }
    }

    /**
     * Wrapper method to encapsulate the task of obtaining a DeleteJob by it's event.  
     */
    private DeleteJob getJob(OperationEvent event) {
        DeleteJob job = null;
        job = operationLimiter.getJob(event.getFileID());
        return job;
    }
    
}
