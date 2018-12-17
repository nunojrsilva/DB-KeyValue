import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TwoPC {
    SegmentedJournal<Object> log;
    SegmentedJournalReader<Object> readerLog;
    SegmentedJournalWriter<Object> writerLog;

    Address[] end;
    int meuID;
    ManagedMessagingService ms;
    Serializer s;
    ScheduledExecutorService es;

    public TwoPC(Address[] end, int meuID, ManagedMessagingService ms) {
        this.end = end;
        this.meuID = meuID;
        this.ms = ms;

        s = DBKeyValueProtocol.newSerializer();

        log = SegmentedJournal.builder()
                .withName("exemploID" + this.meuID)
                .withSerializer(s)
                .build();

        es = Executors.newSingleThreadScheduledExecutor();

        readerLog = log.openReader(0);
        writerLog = log.writer();

    }
}
