package dk.statsbiblioteket.medieplatform.bitrepository.purger;

/**
 * Class representing a DeleteJob
 * The class carries the basic information about a job i.e. fileID, checksum and status 
 */
public class DeleteJob {

    /**
     * Enum to indicate the status of jobs 
     */
    public enum JobStatus {
        FAILED,
        TIMEOUT, 
        DRYRUN, 
        CREATED;
    }
    
    final String fileID;
    final String checksum;
    JobStatus status;
    
    /**
     * Constructor for creating a DeleteJob, the job's status is initialized with the CREATED status.
     * @param fileID The ID of the file that the job is about
     * @param checksum The checksum for the file 
     */
    DeleteJob(String fileID, String checksum) {
        this.fileID = fileID;
        this.checksum = checksum;
        this.status = JobStatus.CREATED;
    }
    
    /**
     * Get the status of the job. 
     * Initially the job status will be CREATED 
     */
    JobStatus getStatus() {
        return status;
    }
    
    /**
     * Set the status of the job 
     */
    void setStatus(JobStatus status) {
        this.status = status;
    }
    
    /**
     * Get the ID of the file that the job is about 
     */
    String getFileID() {
        return fileID;
    } 
    
    /**
     * Get the checksum for the file that the job is about 
     */
    String getChecksum() {
        return checksum;
    }
    
    
    @Override
    public String toString() {
        return "DeleteJob [fileID=" + fileID + ", checksum=" + checksum
                + ", status=" + status + "]";
    }
}
