import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

//mensagem para ser enviada!

class Transaction{
    public int xid;
    public String resultado; //pode ser I; A ou C; F para o controlador
                            //pode ser P ou A; A ou C para o participante

    //Estas variaveis são para o Coodenador ... Criar uma class com extends?
    public HashSet<Address> quaisResponderam = new HashSet<>();
    public HashMap<Address, HashMap<Long, byte[]>> participantes;
    //so podemos fazer commit quando o tamanho dos dois maps forem iguais!
    public CompletableFuture<Boolean> terminada;

    public Transaction(int xid, String resultado){
        this.xid = xid;
        this.resultado = resultado;
    }

    public Transaction(int xid, String resultado, HashMap<Address, HashMap<Long, byte[]>> participantes) {
        this.xid = xid;
        this.resultado = resultado;
        this.participantes = participantes;
    }
}


public class TestLog {
    public static void leLog(SegmentedJournal<Object> log){
        SegmentedJournalReader<Object> readLog = log.openReader(0);
        while(readLog.hasNext()){
            LogEntry e = (LogEntry) readLog.next().entry();
            System.out.println(e);
        }
    }

    public static void leLogs(Address e[]){
        Serializer ser = DBKeyValueProtocol.newSerializer();


        for(int i = 0; i < e.length; i++){
            SegmentedJournal<Object> log = SegmentedJournal.builder()
                    .withName("exemploID" + i)
                    .withSerializer(ser)
                    .build();
            System.out.println("-------------------------------ID:"+i+"-----------------------------");
            leLog(log);
        }
    }
    public static void main(String[] args) {

        Address[] end = {
                Address.from("localhost:23451"),
                Address.from("localhost:23452"),
                Address.from("localhost:23453"),
                //Address.from("localhost:23454"),
                //Address.from("localhost:23455"),
        };

        int id = Integer.parseInt(args[0]);
        if(id==-1){
            leLogs(end);
        }
        else {
            if (id == 0) {
                ManagedMessagingService ms = NettyMessagingService.builder()
                        .withAddress(end[id])
                        .build();
                ms.start();

                TwoPCControlador tpc = new TwoPCControlador(end, id, ms);

                //tpc.iniciaTransacao(valores);
            }
            else {
                ManagedMessagingService ms = NettyMessagingService.builder()
                        .withAddress(end[id])
                        .build();
                ms.start();
                TwoPCParticipante tpp = new TwoPCParticipante(end, id, ms);
            }


        }

    }
}
