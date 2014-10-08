package dk.statsbiblioteket.medieplatform.bitrepository.purger;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.util.List;

import org.bitrepository.bitrepositoryelements.ChecksumDataForFileTYPE;
import org.bitrepository.bitrepositoryelements.ChecksumSpecTYPE;
import org.bitrepository.client.eventhandler.CompleteEvent;
import org.bitrepository.client.eventhandler.EventHandler;
import org.bitrepository.client.eventhandler.OperationEvent;
import org.bitrepository.client.eventhandler.OperationFailedEvent;
import org.bitrepository.modify.deletefile.DeleteFileClient;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import dk.statsbiblioteket.medieplatform.bitrepository.purger.DeleteJob.JobStatus;


public class BitrepositoryPurgerTest {

    final String TEST_COLLECTION = "test-collection";
    final String TEST_PILLAR_ID = "test-pillar";

    /**
     * Tests the good case scenario where the bitrepository client completes the requests without failures
     */
    @Test(groups = "regressionTest")
    public void smallPurgeSuccessTest() {
        boolean PERFORM = false;
        DeleteFileClient client = new DeleteFileClient() {
            @Override
            public void deleteFile(String collectionID, String fileId, String pillarId, 
                    ChecksumDataForFileTYPE checksumForPillar, ChecksumSpecTYPE checksumRequested,
                    EventHandler eventHandler, String auditTrailInformation) {
                OperationEvent event = new CompleteEvent(TEST_COLLECTION, null);
                eventHandler.handleEvent(event);
            }
        };
        Purger purger = spy(new Purger(client, TEST_COLLECTION, TEST_PILLAR_ID, "delete message", 8, 3600));
        File testFile = new File("src/test/resources/small-test-input-file");
        
        purger.purge(testFile, PERFORM);
        
        ArgumentCaptor<List> arguments = ArgumentCaptor.forClass(List.class);
        verify(purger).reportResults(arguments.capture());
        List<DeleteJob> reportedJobs =  (List<DeleteJob>) arguments.getAllValues().get(0);
        assertTrue(reportedJobs.isEmpty(), "No DeleteJobs should be reported as there is no failures from the bitrepository client");
    }
    
    /**
     * Tests the good case scenario where the purger is running in dry run mode. 
     * DeleteJobs should be reported as dry runs and no interaction should happen with the bitrepository client
     */
    @Test(groups = "regressionTest")
    public void smallDryrunTest() {
        boolean DRYRUN = true;
        DeleteFileClient client = mock(DeleteFileClient.class);
        
        Purger purger = spy(new Purger(client, TEST_COLLECTION, TEST_PILLAR_ID, "delete message", 8, 3600));
        File testFile = new File("src/test/resources/small-test-input-file");
        
        purger.purge(testFile, DRYRUN);
        
        ArgumentCaptor<List> arguments = ArgumentCaptor.forClass(List.class);
        verify(purger).reportResults(arguments.capture());
        List<DeleteJob> reportedJobs =  (List<DeleteJob>) arguments.getAllValues().get(0);
        assertEquals(reportedJobs.size(), 2, "Two DeleteJobs should be reported here");
        assertEquals(reportedJobs.get(0).getFileID(), "testfile1");
        assertEquals(reportedJobs.get(0).getStatus(), JobStatus.DRYRUN, "The JobStatus should be DRYRUN");
        assertEquals(reportedJobs.get(1).getFileID(), "testfile2");
        assertEquals(reportedJobs.get(1).getStatus(), JobStatus.DRYRUN, "The JobStatus should be DRYRUN");
        
        verifyNoMoreInteractions(client);
    }
    
    /**
     * Tests the failure scenario where the bitrepository client fails the requests
     */
    @Test(groups = "regressionTest")
    public void smallPurgeFailureTest() {
        boolean PERFORM = false;
        DeleteFileClient client = new DeleteFileClient() {
            @Override
            public void deleteFile(String collectionID, String fileId, String pillarId, 
                    ChecksumDataForFileTYPE checksumForPillar, ChecksumSpecTYPE checksumRequested,
                    EventHandler eventHandler, String auditTrailInformation) {
                OperationEvent event = new OperationFailedEvent(TEST_COLLECTION, "Failed", null);
                eventHandler.handleEvent(event);
            }
        };
        Purger purger = spy(new Purger(client, TEST_COLLECTION, TEST_PILLAR_ID, "delete message", 8, 3600));
        File testFile = new File("src/test/resources/small-test-input-file");
        
        purger.purge(testFile, PERFORM);
        
        ArgumentCaptor<List> arguments = ArgumentCaptor.forClass(List.class);
        verify(purger).reportResults(arguments.capture());
        List<DeleteJob> reportedJobs =  (List<DeleteJob>) arguments.getAllValues().get(0);
        assertEquals(reportedJobs.size(), 2, "Two DeleteJobs should be reported as the bitrepository client reports failures");
        assertEquals(reportedJobs.get(0).getFileID(), "testfile1");
        assertEquals(reportedJobs.get(0).getStatus(), JobStatus.FAILED, "The JobStatus should be FAILED");
        assertEquals(reportedJobs.get(1).getFileID(), "testfile2");
        assertEquals(reportedJobs.get(1).getStatus(), JobStatus.FAILED, "The JobStatus should be FAILED");
    }
    
    /**
     * Tests the failure scenario where theres no answer from the bitrepository client before the component is set to timeout
     */
    @Test(groups = "regressionTest")
    public void smallPurgeTimeoutTest() {
        boolean PERFORM = false;
        DeleteFileClient client = new DeleteFileClient() {
            @Override
            public void deleteFile(String collectionID, String fileId, String pillarId, 
                    ChecksumDataForFileTYPE checksumForPillar, ChecksumSpecTYPE checksumRequested,
                    EventHandler eventHandler, String auditTrailInformation) {
            }
        };
        Purger purger = spy(new Purger(client, TEST_COLLECTION, TEST_PILLAR_ID, "delete message", 8, 1));
        File testFile = new File("src/test/resources/small-test-input-file");
        
        purger.purge(testFile, PERFORM);
        
        ArgumentCaptor<List> arguments = ArgumentCaptor.forClass(List.class);
        verify(purger).reportResults(arguments.capture());
        List<DeleteJob> reportedJobs =  (List<DeleteJob>) arguments.getAllValues().get(0);
        assertEquals(reportedJobs.size(), 2, "Two DeleteJobs should be reported as the bitrepository client reports failures");
        assertEquals(reportedJobs.get(0).getFileID(), "testfile1");
        assertEquals(reportedJobs.get(0).getStatus(), JobStatus.TIMEOUT, "The JobStatus should be TIMEOUT");
        assertEquals(reportedJobs.get(1).getFileID(), "testfile2");
        assertEquals(reportedJobs.get(1).getStatus(), JobStatus.TIMEOUT, "The JobStatus should be TIMEOUT");
    }
}
