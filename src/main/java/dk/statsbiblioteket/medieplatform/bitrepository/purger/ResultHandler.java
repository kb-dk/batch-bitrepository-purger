package dk.statsbiblioteket.medieplatform.bitrepository.purger;

import java.util.ArrayList;
import java.util.List;

/**
 * Class to handle collection of results. 
 * Currently two types of results are colleted:
 * - Failed jobs, i.e. jobs that have failed for some reason.
 * - Dry runs, i.e. jobs that was really not started.  
 */
public class ResultHandler {
    List<DeleteJob> failedJobs;
    List<DeleteJob> dryRuns;
    
    /**
     * Create the result handler. 
     */
    public ResultHandler() {
        failedJobs = new ArrayList<>();
        dryRuns = new ArrayList<>();
    }
    
    /**
     * Add a DeleteJob to the list of failed jobs 
     */
    public void addFailure(DeleteJob job) {
        failedJobs.add(job);
    }
    
    /**
     * Get the list of failed jobs 
     */
    public List<DeleteJob> getFailedJobs() {
        return failedJobs;
    }
    
    /**
     * Add a DeleteJob to the list of dry runs 
     */
    public void addDryRun(DeleteJob job) {
        dryRuns.add(job);
    }
    
    /**
     * Get the list of dry runs 
     */
    public List<DeleteJob> getDryRuns() {
        return dryRuns;
    }
}
