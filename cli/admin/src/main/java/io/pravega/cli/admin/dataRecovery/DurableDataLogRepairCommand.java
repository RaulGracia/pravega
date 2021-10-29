/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.cli.admin.dataRecovery;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.util.CompositeByteArraySegment;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.logs.DataFrameBuilder;
import io.pravega.segmentstore.server.logs.DataFrameRecord;
import io.pravega.segmentstore.server.logs.DebugRecoveryProcessor;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.Cleanup;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * This command provides an administrator with the basic primitives to manipulate a DurableLog with damaged entries.
 * The workflow of this command is as follows:
 * 1. Disable the original DurableLog (if not done yet).
 * 2. Reads the original damaged DurableLog and creates a backup copy of it for safety reasons.
 * 3. Validate and buffer all the edits from admin to be done on the original DurableLog data (i.e., skip, delete,
 * replace operations). All these changes are then written on a Repair Log (i.e., original DurableLog data + admin changes).
 * 4. With the desired updates written in the Repair Log, the admin can replace the original DurableLog metadata by the
 * Repair Log's one. This will make the DurableLog for the Segment Container under repair to point to the Repair Log data.
 * 5. The backed-up data for the originally damaged DurableLog can be reused to create a new Repair Log or discarded if
 * the Segment Container recovers as expected.
 */
public class DurableDataLogRepairCommand extends DataRecoveryCommand {

    private final static Duration TIMEOUT = Duration.ofSeconds(10);
    @VisibleForTesting
    private List<LogEditOperation> durableLogEdits;
    private boolean testMode = false;

    /**
     * Creates a new instance of the DurableLogRepairCommand class.
     *
     * @param args The arguments for the command.
     */
    public DurableDataLogRepairCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Constructor for testing purposes. We can provide predefined edit commands as input and validate the results
     * without having to provide console input.
     *
     * @param args The arguments for the command.
     * @param durableLogEdits List of LogEditOperation to be done on the original log.
     */
    @VisibleForTesting
    DurableDataLogRepairCommand(CommandArgs args, List<LogEditOperation> durableLogEdits) {
        super(args);
        this.durableLogEdits = durableLogEdits;
        this.testMode = true;
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        int containerId = getIntArg(0);
        val bkConfig = getCommandArgs().getState().getConfigBuilder()
                .include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, getServiceConfig().getZkURL()))
                .build().getConfig(BookKeeperConfig::builder);
        @Cleanup
        val zkClient = createZKClient();
        @Cleanup
        val dataLogFactory = new BookKeeperLogFactory(bkConfig, zkClient, this.executorService);
        dataLogFactory.initialize();

        // Open the Original Log in read-only mode.
        @Cleanup
        val originalDataLog = dataLogFactory.createDebugLogWrapper(containerId);
        @Cleanup
        val originalDataLogReadOnly = originalDataLog.asReadOnly();

        // Disable the Original Log, if not disabled already.
        output("Original DurableLog is enabled. You are about to disable it.");
        if (!this.testMode && !confirmContinue()) {
            output("Not disabling Original Log this time.");
            return;
        }
        originalDataLog.disable();

        // Get user input of operations to skip, replace, or delete.
        if (this.durableLogEdits == null) {
            this.durableLogEdits = getDurableLogEditsFromUser();
        }
        // Edit Operations need to be sorted, and they should involve only one actual Operations (e.g., we do not allow 2
        // edits on the same operation id).
        checkDurableLogEdits(this.durableLogEdits);

        // Show the edits to be committed to the original durable log so the user can confirm.
        output("The following edits will be used to edit the Original Log:");
        durableLogEdits.forEach(System.out::println);

        // Create a new Backup Log to store the Original Log contents.
        @Cleanup
        DurableDataLog backupDataLog = dataLogFactory.createDurableDataLog(BACKUP_LOG_ID);
        backupDataLog.initialize(TIMEOUT);

        // Instantiate the processor that will back up the Original Log contents into the Backup Log.
        int operationsReadFromOriginalLog = 0;
        try (BackupLogProcessor backupLogProcessor = new BackupLogProcessor(backupDataLog, executorService)) {
            operationsReadFromOriginalLog = readDurableDataLogWithCustomCallback(backupLogProcessor, containerId, originalDataLogReadOnly);
            // Ugly, but it ensures that the last write done in the DataFrameBuilder is actually persisted.
            backupDataLog.append(new CompositeByteArraySegment(new byte[0]), TIMEOUT).join();
            // The number of processed operation should match the number of read operations from DebugRecoveryProcessor.
            assert backupLogProcessor.getProcessedOperations() == operationsReadFromOriginalLog;
            assert !backupLogProcessor.isFailed;
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: Safely rollback here.
        }
        // Validate that the Original and Backup logs have the same number of operations.
        @Cleanup
        val validationBackupDataLog = dataLogFactory.createDebugLogWrapper(BACKUP_LOG_ID);
        @Cleanup
        val validationBackupDataLogReadOnly = validationBackupDataLog.asReadOnly();
        int backupLogReadOperations = readDurableDataLogWithCustomCallback((a, b) -> output("Reading: " + a), BACKUP_LOG_ID, validationBackupDataLogReadOnly);
        outputInfo("Original DurableLog operations read: " + operationsReadFromOriginalLog +
                ", Backup DurableLog operations read: " + backupLogReadOperations);

        // Ensure that the Original log contains the same number of Operations than the Backup log upon a new log read.
        assert operationsReadFromOriginalLog == backupLogReadOperations;

        output("Original DurableLog has been backed up correctly. Ready to apply admin-provided changes to the Original Log.");
        if (!this.testMode && !confirmContinue()) {
            output("Not editing original DurableLog this time.");
            return;
        }

        // Create a new Repair log to store the result of edits applied to the Original Log.
        @Cleanup
        DurableDataLog editedDataLog = dataLogFactory.createDurableDataLog(REPAIR_LOG_ID);
        editedDataLog.initialize(TIMEOUT);

        // Instantiate the processor that will write the edited contents into the Repair Log.
        try (EditingLogProcessor logEditState = new EditingLogProcessor(editedDataLog, durableLogEdits, executorService)) {
            int readBackupLogAgain = readDurableDataLogWithCustomCallback(logEditState, BACKUP_LOG_ID, backupDataLog);
            // Ugly, but it ensures that the last write done in the DataFrameBuilder is actually persisted.
            editedDataLog.append(new CompositeByteArraySegment(new byte[0]), TIMEOUT).join();
            outputInfo("Backup DurableLog operations read on first attempt: " + backupLogReadOperations +
                    ", Backup DurableLog operations read on second attempt: " + readBackupLogAgain);
            assert !logEditState.isFailed;
        } catch (Exception ex) {
            outputError("There have been errors while creating the edited version of the DurableLog.");
            ex.printStackTrace();
            // TODO: Safely rollback here.
        }

        int editedDurableLogOperations = readDurableDataLogWithCustomCallback((op, list) -> System.out.println("EDITED LOG OPS: " + op), REPAIR_LOG_ID, editedDataLog);
        outputInfo("Edited DurableLog Operations read: " + editedDurableLogOperations);

        // Overwrite the original DurableLog metadata with the edited DurableLog metadata.
        @Cleanup
        val editedLogWrapper = dataLogFactory.createDebugLogWrapper(REPAIR_LOG_ID);
        output("Original DurableLog Metadata: " + originalDataLog.fetchMetadata());
        output("Edited DurableLog Metadata: " + editedLogWrapper.fetchMetadata());
        originalDataLog.forceMetadataOverWrite(editedLogWrapper.fetchMetadata());
        output("New Original DurableLog Metadata (after replacement): " + originalDataLog.fetchMetadata());

        // Read the edited contents that are now reachable from the original log id.
        try {
            @Cleanup
            val finalEditedLog = originalDataLog.asReadOnly();
            int finalEditedLogReadOps = readDurableDataLogWithCustomCallback((op, list) -> System.out.println(op), containerId, finalEditedLog);
            outputInfo("Original DurableLog operations read (after editing): " + finalEditedLogReadOps);
        } catch (Exception ex) {
            outputError("Problem reading Original DurableLog after editing.");
            ex.printStackTrace();
        }

        // TODO: Cleanup Edited and Backup Logs
    }

    @VisibleForTesting
    void checkDurableLogEdits(List<LogEditOperation> durableLogEdits) {
        // TODO: Add checks to durableLogEdits to validate that they are added correctly.
    }

    @VisibleForTesting
    List<LogEditOperation> getDurableLogEditsFromUser() {
        List<LogEditOperation> durableLogEdits = new ArrayList<>();
        boolean finishInputCommands = false;
        while (!finishInputCommands) {
            try {
                switch (getStringUserInput("Select edit action on DurableLog: [delete|add|replace]")) {
                    case "delete":
                        long initialOpId = getLongUserInput("Initial operation id to delete? (inclusive)");
                        long finalOpId = getLongUserInput("Initial operation id to delete? (exclusive)");
                        durableLogEdits.add(new LogEditOperation(LogEditType.DELETE_OPERATION, initialOpId, finalOpId, null));
                        break;
                    case "add":
                        initialOpId = getLongUserInput("At which Operation sequence number would you like to add new Operations?");
                        do {
                            durableLogEdits.add(new LogEditOperation(LogEditType.ADD_OPERATION, initialOpId, initialOpId, createUserDefinedOperation()));
                            outputInfo("You can add more Operations at this sequence number or not.");
                        } while (confirmContinue());
                        break;
                    case "replace":
                        initialOpId = getLongUserInput("What Operation sequence number would you like to replace?");
                        durableLogEdits.add(new LogEditOperation(LogEditType.REPLACE_OPERATION, initialOpId, initialOpId, createUserDefinedOperation()));
                        break;
                    default:
                        output("Invalid operation, please select one of [delete|add|replace]");
                }
            } catch (NumberFormatException ex) {
                outputError("Wrong input argument.");
                ex.printStackTrace();
            } catch (Exception ex) {
                outputError("Some problem has happened.");
                ex.printStackTrace();
            }
            outputInfo("You can continue adding edits to the original DurableLog.");
            finishInputCommands = confirmContinue();
        }

        return durableLogEdits;
    }

    @VisibleForTesting
    Operation createUserDefinedOperation() {
        final String operations = "[DeleteSegmentOperation|MergeSegmentOperation|MetadataCheckpointOperation|" +
                "StorageMetadataCheckpointOperation|StreamSegmentAppendOperation|StreamSegmentMapOperation|" +
                "StreamSegmentSealOperation|StreamSegmentTruncateOperation|UpdateAttributesOperation]";
        switch (getStringUserInput("Type one of the following Operations to instantiate: " + operations)) {
            case "DeleteSegmentOperation":
                long segmentId = getLongUserInput("Introduce Segment Id for DeleteSegmentOperation:");
                return new DeleteSegmentOperation(segmentId);
            case "MergeSegmentOperation":
                long targetSegmentId = getLongUserInput("Introduce Target Segment Id for MergeSegmentOperation:");
                long sourceSegmentId = getLongUserInput("Introduce Source Segment Id for MergeSegmentOperation:");
                return new MergeSegmentOperation(targetSegmentId, sourceSegmentId, createAttributeUpdateCollection());
            case "MetadataCheckpointOperation":
            case "StorageMetadataCheckpointOperation":
            case "StreamSegmentAppendOperation":
                throw new UnsupportedOperationException();
            case "StreamSegmentMapOperation":
                return new StreamSegmentMapOperation(createSegmentProperties());
            case "StreamSegmentSealOperation":
                segmentId = getLongUserInput("Introduce Segment Id for StreamSegmentSealOperation:");
                return new StreamSegmentSealOperation(segmentId);
            case "StreamSegmentTruncateOperation":
                segmentId = getLongUserInput("Introduce Segment Id for StreamSegmentTruncateOperation:");
                long offset = getLongUserInput("Introduce Offset for StreamSegmentTruncateOperation:");
                return new StreamSegmentTruncateOperation(segmentId, offset);
            case "UpdateAttributesOperation":
                segmentId = getLongUserInput("Introduce Segment Id for UpdateAttributesOperation:");
                return new UpdateAttributesOperation(segmentId, createAttributeUpdateCollection());
            default:
                output("Invalid operation, please select one of " + operations);
        }
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    SegmentProperties createSegmentProperties() {
        String segmentName = getStringUserInput("Introduce the name of the Segment: ");
        long offset = getLongUserInput("Introduce the offset of the Segment: ");
        long length = getLongUserInput("Introduce the length of the Segment: ");
        long storageLength = getLongUserInput("Introduce the storage length of the Segment: ");
        boolean sealed = getBooleanUserInput("Is the Segment sealed? [true/false]: ");
        boolean sealedInStorage = getBooleanUserInput("Is the Segment sealed in storage? [true/false]: ");
        boolean deleted = getBooleanUserInput("Is the Segment deleted? [true/false]: ");
        boolean deletedInStorage = getBooleanUserInput("Is the Segment deleted in storage? [true/false]: ");
        outputInfo("You are about to start adding Attributes to the SegmentProperties instance.");
        boolean finishInputCommands = confirmContinue();
        Map<AttributeId, Long> attributes = new HashMap<>();
        while (!finishInputCommands) {
            output("Creating an AttributeUpdateCollection for this operation.");
            try {
                AttributeId attributeId = AttributeId.fromUUID(UUID.fromString(getStringUserInput("Introduce UUID for this Attribute: ")));
                long value = getLongUserInput("Introduce the Value for this Attribute:");
                attributes.put(attributeId, value);
            } catch(NumberFormatException ex){
                outputError("Wrong input argument.");
                ex.printStackTrace();
            } catch(Exception ex){
                outputError("Some problem has happened.");
                ex.printStackTrace();
            }
            outputInfo("You can continue adding AttributeUpdates to the AttributeUpdateCollection.");
            finishInputCommands = confirmContinue();
        }
        long lastModified = getLongUserInput("Introduce last modified timestamp for the Segment (milliseconds): ");
        return StreamSegmentInformation.builder().name(segmentName).startOffset(offset).length(length).storageLength(storageLength)
                .sealed(sealed).deleted(deleted).sealedInStorage(sealedInStorage).deletedInStorage(deletedInStorage)
                .attributes(attributes).lastModified(new ImmutableDate(lastModified)).build();
    }

    @VisibleForTesting
    AttributeUpdateCollection createAttributeUpdateCollection() {
        AttributeUpdateCollection attributeUpdates = new AttributeUpdateCollection();
        outputInfo("You are about to start adding AttributeUpdates to the AttributeUpdateCollection.");
        boolean finishInputCommands = confirmContinue();
        while (!finishInputCommands) {
            output("Creating an AttributeUpdateCollection for this operation.");
            try {
                AttributeId attributeId = AttributeId.fromUUID(UUID.fromString(getStringUserInput("Introduce UUID for this AttributeUpdate: ")));
                AttributeUpdateType type = AttributeUpdateType.get((byte) getIntUserInput("Introduce AttributeUpdateType for this AttributeUpdate" +
                        "(0 (None), 1 (Replace), 2 (ReplaceIfGreater), 3 (Accumulate), 4(ReplaceIfEquals)): "));
                long value = getLongUserInput("Introduce the Value for this AttributeUpdate:");
                long comparisonValue = getLongUserInput("Introduce the comparison Value for this AttributeUpdate:");
                attributeUpdates.add(new AttributeUpdate(attributeId, type, value, comparisonValue));
            } catch(NumberFormatException ex){
                outputError("Wrong input argument.");
                ex.printStackTrace();
            } catch(Exception ex){
                outputError("Some problem has happened.");
                ex.printStackTrace();
            }
            outputInfo("You can continue adding AttributeUpdates to the AttributeUpdateCollection.");
            finishInputCommands = confirmContinue();
        }
        return attributeUpdates;
    }

    /**
     * Reads a {@link DurableDataLog} associated with a container id and runs the callback on each {@link Operation}
     * read from the log.
     *
     * @param callback Callback to be run upon each {@link Operation} read.
     * @param containerId Container id to read from.
     * @param durableDataLog {@link DurableDataLog} of the Container to be read.
     * @return Number of {@link Operation}s read.
     * @throws Exception
     */
    private int readDurableDataLogWithCustomCallback(BiConsumer<Operation, List<DataFrameRecord.EntryInfo>> callback,
                                                     int containerId, DurableDataLog durableDataLog) throws Exception {
        val logReaderCallbacks = new DebugRecoveryProcessor.OperationCallbacks(
                callback,
                op -> false, // We are not interested on doing actual recovery, just reading the operations.
                null,
                null);
        val containerConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ContainerConfig::builder);
        val readIndexConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ReadIndexConfig::builder);
        @Cleanup
        val rp = DebugRecoveryProcessor.create(containerId, durableDataLog,
                containerConfig, readIndexConfig, this.executorService, logReaderCallbacks);
        int operationsRead = rp.performRecovery();
        output("Number of operations read from DurableLog: " + operationsRead);
        return operationsRead;
    }

    /**
     * This class provides the basic logic for reading from a {@link DurableDataLog} and writing the read {@link Operation}s
     * to another {@link DurableDataLog}. Internally, it uses a {@link DataFrameBuilder} to write to the target {@link DurableDataLog}
     * and performs one write at a time, waiting for the previous write to complete before issuing the next one. It also
     * provides counters to inform about the state of the processing as well as closing the resources.
     */
    abstract class AbstractLogProcessor implements BiConsumer<Operation, List<DataFrameRecord.EntryInfo>>, AutoCloseable {
        protected final AtomicLong processedOperations = new AtomicLong(0);
        protected final Map<Long, CompletableFuture<Void>> operationProcessingTracker = new ConcurrentHashMap<>();
        @NonNull
        protected final DataFrameBuilder<Operation> dataFrameBuilder;
        @Getter
        protected boolean isFailed = false;
        @Getter
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicInteger beforeCommit = new AtomicInteger();
        private final AtomicInteger commitSuccess = new AtomicInteger();
        private final AtomicInteger commitFailure = new AtomicInteger();

        AbstractLogProcessor(DurableDataLog durableDataLog, ScheduledExecutorService executor) {
            DataFrameBuilder.Args args = new DataFrameBuilder.Args(a -> this.beforeCommit.getAndIncrement(),
                    b -> {
                        operationProcessingTracker.get(b.getLastFullySerializedSequenceNumber()).complete(null);
                        this.commitSuccess.getAndIncrement();
                    },
                    (c, d) -> {
                        operationProcessingTracker.get(d.getLastFullySerializedSequenceNumber()).complete(null);
                        isFailed = true; // Consider a single failed write as a failure in the whole process.
                        commitFailure.getAndIncrement();
                    },
                    executor);
            this.dataFrameBuilder = new DataFrameBuilder<>(durableDataLog, OperationSerializer.DEFAULT, args);
        }

        public long getProcessedOperations() {
            return processedOperations.get();
        }

        public void close() {
            if (closed.compareAndSet(false, true)) {
                this.dataFrameBuilder.flush();
                this.dataFrameBuilder.close();
                this.operationProcessingTracker.clear();
            }
        }

        /**
         * Writes an {@link Operation} to the {@link DataFrameBuilder} and wait for the {@link DataFrameBuilder.Args}
         * callbacks are invoked after the operation is written to the target {@link DurableDataLog}.
         *
         * @param operation {@link Operation} to be written and completed before continue with further writes.
         * @throws IOException
         */
        protected void writeAndConfirm(Operation operation) throws IOException {
            trackOperation(operation);
            this.dataFrameBuilder.append(operation);
            this.dataFrameBuilder.flush();
            waitForOperationCommit(operation);
        }

        private void trackOperation(Operation operation) {
            if (this.operationProcessingTracker.containsKey(operation.getSequenceNumber()) {
                outputError("WARNING: Duplicate Operation being written " + operation);
                return;
            }
            CompletableFuture<Void> confirmedWrite = new CompletableFuture<>();
            this.operationProcessingTracker.put(operation.getSequenceNumber(), confirmedWrite);
        }

        private void waitForOperationCommit(Operation operation) {
            this.operationProcessingTracker.get(operation.getSequenceNumber()).join();
        }
    }

    class BackupLogProcessor extends AbstractLogProcessor {

        BackupLogProcessor(DurableDataLog backupDataLog, ScheduledExecutorService executor) {
            super(backupDataLog, executor);
        }

        public void accept(Operation operation, List<DataFrameRecord.EntryInfo> frameEntries) {
            try {
                writeAndConfirm(operation);
                processedOperations.incrementAndGet();
            } catch (Exception e) {
                outputError("Error serializing operation " + operation);
                e.printStackTrace();
                isFailed = true;
            }
        }
    }

    class EditingLogProcessor extends AbstractLogProcessor {
        @NonNull
        private final List<LogEditOperation> durableLogEdits;
        private long newSequenceNumber = 1;

        EditingLogProcessor(DurableDataLog editedDataLog, List<LogEditOperation> durableLogEdits, ScheduledExecutorService executor) {
            super(editedDataLog, executor);
            this.durableLogEdits = durableLogEdits;
        }

        public void accept(Operation operation, List<DataFrameRecord.EntryInfo> frameEntries) {
            try {
                // Nothing to edit, just write the Operations from the original log to the edited one.
                if (!hasEditsToApply(operation)) {
                    operation.resetSequenceNumber(newSequenceNumber++);
                    writeAndConfirm(operation);
                } else {
                    // Edits to a DurableLog are sorted by their initial operation id and they are removed once they
                    // have been applied. The only case in which we can find a DurableLog Operation with a sequence
                    // number lower than the next DurableLog edit is that the data corruption issue we are trying to
                    // repair induces duplication of DataFrames.
                    if (operation.getSequenceNumber() < durableLogEdits.get(0).getInitialOperationId()) {
                        outputError("Found an Operation with a lower sequence number than the initial" +
                                "id of the next edit to apply. This may be symptom of a duplicated DataFrame and will" +
                                "also duplicate the associated edit: " + operation);
                        if (!confirmContinue()) {
                            output("Not editing original DurableLog for this operation.");
                            return;
                        }
                    }
                    // We have edits to do.
                    LogEditOperation logEdit = durableLogEdits.get(0);
                    switch (logEdit.getType()) {
                        case DELETE_OPERATION:
                            // A Delete Edit Operation on a DurableLog consists of not writing the range of Operations
                            // between its initial (inclusive) and final (exclusive) operation id.
                            // Example: Original Log = [1, 2, 3, 4, 5]
                            //          delete 2-4
                            //          Result Log = [1, 2 (4), 3 (5)] (former operation sequence number)
                            applyDeleteEditOperation(operation, logEdit);
                            break;
                        case ADD_OPERATION:
                            // An Add Edit Operation on a DurableLog consists of appending the desired Operation
                            // encapsulated in the Add Edit Operation before the actual Operation contained in the log.
                            // Note that we may want to add multiple new Operations at a specific position before the
                            // actual one.
                            // Example: Original Log = [1, 2, 3, 4, 5]
                            //          add 2, opA, opB, opC
                            //          Result Log = [1, 2 (opA), 3 (opB), 4 (opC), 5 (2), 6 (3), 7 (4), 8 (5)] (former operation sequence number)
                            applyAddEditOperation(operation, logEdit);
                            break;
                        case REPLACE_OPERATION:
                            // A Replace Edit Operation on a DurableLog consists of deleting the current Operation and
                            // adding the new Operation encapsulated in the Replace Edit Operation.
                            // Example: Original Log = [1, 2, 3, 4, 5]
                            //          replace 2, opA
                            //          Result Log = [1, 2 (opA), 3, 4, 5] (former operation sequence number)
                            applyReplaceEditOperation(logEdit);
                            break;
                        default:
                            outputError("Unknown DurableLog edit type: " + durableLogEdits.get(0).getType());
                    }
                }
            } catch (Exception e) {
                outputError("Error serializing operation " + operation);
                e.printStackTrace();
                isFailed = true;
            }
        }

        /**
         * Replaces the input {@link Operation}s by the content of the replace {@link LogEditOperation} om the target log.
         * Each replace {@link LogEditOperation} is removed from the list of edits once applied to the target log.
         *
         * @param logEdit @{@link LogEditOperation} of type {@link LogEditType#REPLACE_OPERATION} to be added to the target
         *                replacing the original {@link Operation}.
         * @throws IOException
         */
        private void applyReplaceEditOperation(LogEditOperation logEdit) throws IOException {
            logEdit.getNewOperation().setSequenceNumber(newSequenceNumber++);
            writeAndConfirm(logEdit.getNewOperation());
            durableLogEdits.remove(0);
        }

        /**
         * Adds one or more {@link Operation}s to the target log just before the {@link Operation} passed as input (that
         * is also added after all the new {@link Operation}s are added). Each add {@link LogEditOperation} is removed
         * from the list of edits once applied to the target log.
         *
         * @param operation {@link Operation} read from the log.
         * @param logEdit @{@link LogEditOperation} of type {@link LogEditType#ADD_OPERATION} to be added to the target
         *                log before the input {@link Operation}.
         * @throws IOException
         */
        private void applyAddEditOperation(Operation operation, LogEditOperation logEdit) throws IOException {
            long currentInitialAddOperation = logEdit.getInitialOperationId();
            while (!durableLogEdits.isEmpty() && logEdit.getType().equals(LogEditType.ADD_OPERATION)
                    && logEdit.getInitialOperationId() == currentInitialAddOperation) {
                logEdit = durableLogEdits.get(0);
                logEdit.getNewOperation().setSequenceNumber(newSequenceNumber++);
                writeAndConfirm(logEdit.getNewOperation());
                durableLogEdits.remove(0);
            }
            // After all the additions are done, add the current log operation.
            operation.resetSequenceNumber(newSequenceNumber++);
            writeAndConfirm(operation);
        }

        /**
         * Skips all the {@link Operation} from the original logs encompassed between the {@link LogEditOperation}
         * initial (inclusive) and final (exclusive) ids. When the last applicable delete has been applied, the edit
         * operation is removed from the list of edits to apply.
         *
         * @param operation {@link Operation} read from the log.
         * @param logEdit @{@link LogEditOperation} of type {@link LogEditType#DELETE_OPERATION} that defines the sequence
         *                numbers of the {@link Operation}s to do not write to the target log.
         */
        private void applyDeleteEditOperation(Operation operation, LogEditOperation logEdit) {
            outputInfo("Deleting operation from DurableLog: " + operation);
            if (logEdit.getFinalOperationId() == operation.getSequenceNumber() + 1) {
                // Once reached the end of the Delete Edit Operation range, remove it from the list.
                try {
                    durableLogEdits.remove(0);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                outputInfo("Completed Delete Edit Operation on DurableLog: " + logEdit);
            }
        }

        /**
         * Decides whether there are edits to apply on the log for the specific sequence id of the input {@link Operation}.
         *
         * @param op {@link Operation} to check if there are edits to apply.
         * @return Whether there are edits to apply to the log at the specific position of the input {@link Operation}.
         */
        private boolean hasEditsToApply(Operation op) {
            if (this.durableLogEdits.isEmpty()) {
                return false;
            }
            LogEditType editType = durableLogEdits.get(0).getType();
            long editInitialOpId = durableLogEdits.get(0).getInitialOperationId();
            long editFinalOpId = durableLogEdits.get(0).getFinalOperationId();
            return editInitialOpId == op.getSequenceNumber()
                    || (editType.equals(LogEditType.DELETE_OPERATION)
                        && editInitialOpId <= op.getSequenceNumber()
                        && editFinalOpId >= op.getSequenceNumber());
        }
    }

    /**
     * Available types of editing operations we can perform on a {@link DurableDataLog}.
     */
    enum LogEditType {
        DELETE_OPERATION,
        ADD_OPERATION,
        REPLACE_OPERATION
    }

    /**
     * Information encapsulated by each edit to the target log.
     */
    @Data
    static class LogEditOperation {
        private final LogEditType type;
        private final long initialOperationId;
        private final long finalOperationId;
        private final Operation newOperation;
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "durableLog-repair", "Allows to replace the data of a" +
                "DurableLog by an edited version of it in the case that some entries are damaged.");
    }
}
