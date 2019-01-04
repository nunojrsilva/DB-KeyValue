import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
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
    Address meuEnd;
    ManagedMessagingService ms;
    Serializer s;
    ScheduledExecutorService es;
    HashMap<Address,Participante> participantes = new HashMap<Address,Participante>();

    public TwoPC(Address[] end, Address meuID, ManagedMessagingService ms, Serializer ser,
                 ScheduledExecutorService ses) {
        this.end = end;
        this.meuEnd = meuID;
        this.ms = ms;
        for(Address ad: end){
            Participante part = new Participante();
            part.endereco = ad;
            participantes.put(ad,part);
        }

        s = ser;//DBKeyValueProtocol.newSerializer();

        es = ses;//Executors.newSingleThreadScheduledExecutor();

    }
}
