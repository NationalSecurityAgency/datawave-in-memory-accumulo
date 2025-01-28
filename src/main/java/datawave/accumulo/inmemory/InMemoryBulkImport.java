package datawave.accumulo.inmemory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executor;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations.ImportDestinationArguments;
import org.apache.accumulo.core.client.admin.TableOperations.ImportMappingOptions;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.metadata.UnreferencedTabletFile;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.util.Validators;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class InMemoryBulkImport implements ImportDestinationArguments, ImportMappingOptions {
    private static final Logger log = LoggerFactory.getLogger(InMemoryBulkImport.class);
    private boolean setTime = false;
    private boolean ignoreEmptyDir = false;
    private Executor executor = null;
    private final String dir;
    private InMemoryAccumulo acu;
    private String username;
    private int numThreads = -1;
    private String tableName;
    private LoadPlan plan = null;
    private static final byte[] byte0 = new byte[] {0};
    
    InMemoryBulkImport(InMemoryAccumulo acu, String directory) {
        this.dir = Objects.requireNonNull(directory);
        this.acu = Objects.requireNonNull(acu);
    }
    
    public ImportMappingOptions tableTime(boolean value) {
        this.setTime = value;
        return this;
    }
    
    public ImportMappingOptions ignoreEmptyDir(boolean ignore) {
        this.ignoreEmptyDir = ignore;
        return this;
    }
    
    @Override
    public ImportMappingOptions to(String tableName) {
        this.tableName = Validators.EXISTING_TABLE_NAME.validate(tableName);
        return this;
    }
    
    @Override
    public ImportMappingOptions plan(LoadPlan plan) {
        this.plan = Objects.requireNonNull(plan);
        return this;
    }
    
    @Override
    public ImportMappingOptions executor(Executor service) {
        this.executor = Objects.requireNonNull(service);
        return this;
    }
    
    @Override
    public ImportMappingOptions threads(int numThreads) {
        Preconditions.checkArgument(numThreads > 0, "Non positive number of threads given : %s", numThreads);
        this.numThreads = numThreads;
        return this;
    }
    
    @Override
    public void load() throws TableNotFoundException, IOException, AccumuloException, AccumuloSecurityException {
        long time = System.currentTimeMillis();
        InMemoryTable table = acu.tables.get(this.tableName);
        if (table == null) {
            throw new TableNotFoundException(null, tableName, "The table was not found");
        }
        
        FileSystem fs = acu.getFileSystem();
        Path srcPath = this.checkPath(fs, this.dir);
        /*
         * Begin the import - iterate the files in the path
         */
        for (FileStatus importStatus : fs.listStatus(srcPath)) {
            try {
                CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE, table.settings);
                FileSKVIterator importIterator = FileOperations.getInstance().newReaderBuilder()
                                .forFile(UnreferencedTabletFile.of(fs, importStatus.getPath()), fs, fs.getConf(), cs)
                                .withTableConfiguration(DefaultConfiguration.getInstance()).seekToBeginning().build();
                while (importIterator.hasTop()) {
                    Key key = importIterator.getTopKey();
                    Value value = importIterator.getTopValue();
                    if (setTime) {
                        key.setTimestamp(time);
                    }
                    Mutation mutation = new Mutation(key.getRow());
                    if (!key.isDeleted()) {
                        mutation.put(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibilityData().toArray()),
                                        key.getTimestamp(), value);
                    } else {
                        mutation.putDelete(key.getColumnFamily(), key.getColumnQualifier(), new ColumnVisibility(key.getColumnVisibilityData().toArray()),
                                        key.getTimestamp());
                    }
                    table.addMutation(mutation);
                    importIterator.next();
                }
            } catch (Exception e) {
                log.error("Bulk Import Failed");
            }
            fs.delete(importStatus.getPath(), true);
        }
    }
    
    private Path checkPath(FileSystem fs, String dir) throws IOException, AccumuloException {
        Path ret = dir.contains(":") ? new Path(dir) : fs.makeQualified(new Path(dir));
        
        try {
            if (!fs.getFileStatus(ret).isDirectory()) {
                throw new AccumuloException("Bulk import directory " + dir + " is not a directory!");
            } else {
                Path tmpFile = new Path(ret, "isWritable");
                if (fs.createNewFile(tmpFile)) {
                    fs.delete(tmpFile, true);
                    return ret;
                } else {
                    throw new AccumuloException("Bulk import directory " + dir + " is not writable.");
                }
            }
        } catch (FileNotFoundException var5) {
            FileNotFoundException fnf = var5;
            throw new AccumuloException("Bulk import directory " + dir + " does not exist or has bad permissions", fnf);
        }
    }
}
