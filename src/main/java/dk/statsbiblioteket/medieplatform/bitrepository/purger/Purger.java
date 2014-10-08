package dk.statsbiblioteket.medieplatform.bitrepository.purger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import javax.jms.JMSException;

import org.bitrepository.bitrepositoryelements.ChecksumDataForFileTYPE;
import org.bitrepository.bitrepositoryelements.ChecksumSpecTYPE;
import org.bitrepository.bitrepositoryelements.ChecksumType;
import org.bitrepository.client.eventhandler.EventHandler;
import org.bitrepository.common.utils.Base16Utils;
import org.bitrepository.common.utils.CalendarUtils;
import org.bitrepository.modify.deletefile.DeleteFileClient;
import org.bitrepository.protocol.messagebus.MessageBus;
import org.bitrepository.protocol.messagebus.MessageBusManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.statsbiblioteket.medieplatform.bitrepository.purger.DeleteJob.JobStatus;

/**
 * Class to handle purging of a set of files in a collection on a given pillar. 
 * The class supports making a dryrun (showing what was intended to be done, without deleting any files).
 * 
 * In the event of a non-dryrun, the files are deleted asynchronously to speed the process up.
 * Failed files, or files that were in the process of being deleted when a timeout occurs will be listed on STDOUT.
 */
public class Purger {
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final DeleteFileClient deleteClient;
    private final String pillarID;
    private final String collectionID;
    private final ParallelOperationLimiter operationLimiter;
    private final ResultHandler resultHandler;
    private final EventHandler eventHandler;
    private final String deleteMessage;
    
    /**
     * Create the purger
     * @param deleteClient The client used to delete files 
     * @param collectionID The collection in which the files should be deleted from
     * @param pillarID The pillar from which the files should be deleted
     * @param maxAsync The maximum number of asynchronous deletes 
     * @param maxRuntime The maximum number of seconds that will be waited before shutting down the purger 
     */
    Purger(DeleteFileClient deleteClient, String collectionID, String pillarID, String deleteMessage, int maxAsync, int maxRuntime) {
        this.deleteClient = deleteClient;
        this.collectionID = collectionID;
        this.pillarID = pillarID;
        this.deleteMessage = deleteMessage;
        resultHandler = new ResultHandler();
        operationLimiter = new ParallelOperationLimiter(resultHandler, maxAsync, maxRuntime);
        eventHandler = new DeleteFileEventHandler(operationLimiter, resultHandler);
    }
    
    /**
     * Perform the purge of the files contained in the supplied file. 
     * The file format should be <fileID>\t<checksum>
     * 
     * @param fileList The file containing the list of files to be deleted, along with their checksum
     * @param dryRun Boolean to indicate if the purge should be a dry run. If true, no files will be deleted
     */
    public void purge(File fileList, boolean dryRun) {
        try(BufferedReader br = new BufferedReader(new FileReader(fileList));) {
            String line;
            while((line = br.readLine()) != null) {
                String[] tokens = line.trim().split("\\s");
                if(tokens.length == 2) {
                    String fileID = tokens[0];
                    String checksum = tokens[1];
                    DeleteJob job = new DeleteJob(fileID, checksum);
                    if(dryRun) {
                        job.setStatus(JobStatus.DRYRUN);
                        resultHandler.addDryRun(job);
                    } else {
                        deleteFile(job);    
                    }
                } else {
                    log.warn("Malformed line encountered. Line was: '" + line + "'. Line was ignored");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        operationLimiter.waitForFinish();
        
        if(dryRun) {
            reportResults(resultHandler.getDryRuns());
        } else {
            reportResults(resultHandler.getFailedJobs());
        }
    }
    
    /**
     * Perform the actual delete of a file
     * @param fileID The ID of the file to delete
     * @param checksum The checksum of the file to be deleted 
     */
    private void deleteFile(DeleteJob job) {
        operationLimiter.addJob(job);
        log.info("Added delete job for file {}", job.getFileID());
        deleteClient.deleteFile(collectionID, job.getFileID(), pillarID, getChecksumData(job.getChecksum()), null, 
                eventHandler, deleteMessage);
    }
    
    /**
     * Make the data structure needed for supplying the checksum for deleting a file
     * The current implementation assumes that MD5 checksums are used. 
     * @param checksum The checksum to put into the data structure  
     */
    private ChecksumDataForFileTYPE getChecksumData(String checksum) {
        ChecksumDataForFileTYPE res = new ChecksumDataForFileTYPE();
        res.setCalculationTimestamp(CalendarUtils.getNow());
        ChecksumSpecTYPE checksumSpec = new ChecksumSpecTYPE();
        checksumSpec.setChecksumType(ChecksumType.MD5);
        res.setChecksumSpec(checksumSpec);
        res.setChecksumValue(Base16Utils.encodeBase16(checksum));
        return res;
    }
 
    /**
     * Handle the reporting of results. Results are reported on STDOUT in the form
     * STATUS: FileID Checksum
     * @param jobs The list of jobs to be reported.   
     */
    protected void reportResults(List<DeleteJob> jobs) {
        if(jobs != null && !jobs.isEmpty()) {
            System.out.println("STATUS: FileID Checksum");
            for(DeleteJob job : jobs) {
                StringBuilder sb = new StringBuilder();
                sb.append(job.getStatus()).append(": ").append(job.getFileID()).append(" ").append(job.getChecksum());
                System.out.println(sb.toString());
            }
        }
    }
    
    /**
     * Method to shutdown the client properly.
     */
    public void shutdown() {
        try {
            MessageBus messageBus = MessageBusManager.getMessageBus();
            if (messageBus != null) {
                MessageBusManager.getMessageBus().close();
            }
        } catch (JMSException e) {
            log.warn("Failed to shutdown messagebus connection", e);
        } catch (Exception e) {
            log.warn("Caught unexpected exception while closing messagebus down", e);
        }
    }
}
