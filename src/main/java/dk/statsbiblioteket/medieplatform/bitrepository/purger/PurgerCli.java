package dk.statsbiblioteket.medieplatform.bitrepository.purger;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.bitrepository.common.settings.Settings;
import org.bitrepository.common.settings.SettingsProvider;
import org.bitrepository.common.settings.XMLFileSettingsLoader;
import org.bitrepository.modify.ModifyComponentFactory;
import org.bitrepository.modify.deletefile.DeleteFileClient;
import org.bitrepository.protocol.security.BasicMessageAuthenticator;
import org.bitrepository.protocol.security.BasicMessageSigner;
import org.bitrepository.protocol.security.BasicOperationAuthorizor;
import org.bitrepository.protocol.security.BasicSecurityManager;
import org.bitrepository.protocol.security.SecurityManager;
import org.bitrepository.protocol.security.MessageAuthenticator;
import org.bitrepository.protocol.security.MessageSigner;
import org.bitrepository.protocol.security.OperationAuthorizor;
import org.bitrepository.protocol.security.PermissionStore;

/**
 * Class for invoking the purger component 
 */
public class PurgerCli {
    private final static String COLLECTION_ID_PROPERTY = "bitrepository.purger.collectionid";
    private final static String PILLAR_ID_PROPERTY = "bitrepository.purger.pillarid";
    private final static String CLIENT_ID_PROPERTY = "bitrepository.purger.componentid";
    private final static String SETTINGS_DIR_PROPERTY = "bitrepository.purger.settingsdir";
    private final static String CERTIFICATE_FILE_PROPERTY = "bitrepository.purger.certificate";
    private final static String MAX_ASYNC_PROPERTY = "bitrepository.purger.numberofasyncdeletes";
    private final static String MAX_RUNTIME_PROPERTY = "bitrepository.purger.maxruntime";
    
    private final static String PERFORM_DELETE_OPT = "performdelete";
    private final static String FILELIST_OPT = "filelist";
        
    /** The client for performing the DeleteFile operations.*/
    private DeleteFileClient client;
    private Purger purger;
    /**  */
    boolean dryRun = true;
    File filesForDeletion;
    Properties properties;
    Settings settings;
    
    public static void main(String[] args) {
        PurgerCli purgerCli = new PurgerCli();
        try {
            purgerCli.initialize(args);    
        } catch(ParseException | IOException e) {
            throw new RuntimeException("Failed to initialize client", e);
        }
        
        purgerCli.purge();
    }
    
    public PurgerCli() {
        
    }
    
    /**
     * Initializes the purger, i.e. loading settings and creating the needed objects.  
     */
    private void initialize(String[] args) throws ParseException, IOException {
        parseArgs(args);
        loadSettings();
        createDeleteClient();
        int maxAsync = Integer.parseInt(properties.getProperty(MAX_ASYNC_PROPERTY));
        int maxRuntime = Integer.parseInt(properties.getProperty(MAX_RUNTIME_PROPERTY));
        
        purger = new Purger(client, properties.getProperty(COLLECTION_ID_PROPERTY),
                properties.getProperty(PILLAR_ID_PROPERTY), maxAsync, maxRuntime);
    }
    
    /**
     * Parse the arguments needed for the purger 
     * @throws ParseException if commandline options cannot be parsed.
     */
    private void parseArgs(String[] args) throws ParseException {
        CommandLineParser parser = new PosixParser();
        Options options = new Options();
        CommandLine cmd;
        
        Option filelistOpt = new Option(FILELIST_OPT, true, "File containing the list of files to delete");
        filelistOpt.setRequired(true);
        Option performOpt = new Option(PERFORM_DELETE_OPT, false, "Actually do perform the deletion");
        options.addOption(filelistOpt);
        options.addOption(performOpt);
        
        cmd = parser.parse(options, args, false);
        
        if(cmd.hasOption(PERFORM_DELETE_OPT)) {
            dryRun = false;
        }
        filesForDeletion = new File(options.getOption(FILELIST_OPT).getValue());
        if(!filesForDeletion.exists()) {
            System.err.println("File '" + filesForDeletion + "' does not exist.");
            System.exit(1);
        }
    }
    
    /**
     * Method to load the needed settings. 
     * Settings loaded is the 'config.properties' file located on classpath and those specific to the Bitrepository client 
     */
    private void loadSettings() throws IOException {
        properties = new Properties();
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties"));
        SettingsProvider settingsLoader = new SettingsProvider(new XMLFileSettingsLoader(properties.getProperty(SETTINGS_DIR_PROPERTY)), 
                properties.getProperty(CLIENT_ID_PROPERTY));
        settings = settingsLoader.getSettings();
    }
    
    /**
     * Construct the needed Bitrepository delete client 
     */
    private void createDeleteClient() {
        PermissionStore permissionStore = new PermissionStore();
        MessageAuthenticator authenticator = new BasicMessageAuthenticator(permissionStore);
        MessageSigner signer = new BasicMessageSigner();
        OperationAuthorizor authorizer = new BasicOperationAuthorizor(permissionStore);
        SecurityManager securityManager = new BasicSecurityManager(settings.getRepositorySettings(),
                properties.getProperty(CERTIFICATE_FILE_PROPERTY),
                authenticator, signer, authorizer, permissionStore, properties.getProperty(CLIENT_ID_PROPERTY));
        client = ModifyComponentFactory.getInstance().retrieveDeleteFileClient(
                settings, securityManager, properties.getProperty(CLIENT_ID_PROPERTY));
    }
    
    /**
     * Method to delegate the actual performance of purging to the purger 
     */
    public void purge() {
        purger.purge(filesForDeletion, dryRun);
    }

}
