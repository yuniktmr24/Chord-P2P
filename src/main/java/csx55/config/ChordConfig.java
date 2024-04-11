package csx55.config;

/***
 * Static Config for the Chord Overlay
 */
public class ChordConfig {

    public static final int NUM_PEERS = 32; //to get k value = log(numberOfPeers)

    public static final int MAINTENANCE_INTERVAL = 15; //in seconds

    public static final String FILE_STORAGE_ROOT = "tmp";

    public static final String FILE_DOWNLOAD_ROOT = "download";

    public static final boolean DEBUG_MODE = false;
}
