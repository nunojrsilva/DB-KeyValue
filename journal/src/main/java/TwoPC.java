import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

class Participante implements Comparable{
    public Address endereco;
    public CompletableFuture<Boolean> espera = new CompletableFuture<>();

    public int compareTo(Object o){
        Participante p = (Participante)o;

        return this.endereco.toString().compareTo(p.endereco.toString());
    }
}

public class TwoPC {
    SegmentedJournal<Object> log;
    SegmentedJournalReader<Object> readerLog;
    SegmentedJournalWriter<Object> writerLog;

    Address[] end;
    int meuID;
    ManagedMessagingService ms;
    Serializer s;
    ScheduledExecutorService es;
    HashMap<Address,Participante> participantes = new HashMap<Address,Participante>();

    public TwoPC(Address[] end, int meuID, ManagedMessagingService ms) {
        this.end = end;
        this.meuID = meuID;
        this.ms = ms;
        for(Address ad: end){
            Participante part = new Participante();
            part.endereco = ad;
            participantes.put(ad,part);
        }

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
