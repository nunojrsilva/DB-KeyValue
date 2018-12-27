import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.util.HashMap;

public class Cliente {

    public static void main(String args[]){
        HashMap<Long, byte[]> valores = new HashMap<>();
        valores.put(Long.valueOf(1), "ola".getBytes());
        valores.put(Long.valueOf(2), "cenas".getBytes());
        valores.put(Long.valueOf(3), "muitascoisas".getBytes());

        ManagedMessagingService ms = NettyMessagingService.builder()
                .withAddress(Address.from("localhost:12341"))
                .build();
        ms.start();

        ClienteStub cs = new ClienteStub(ms, Integer.parseInt(args[0]));

        cs.put(valores)
            .thenAccept(a -> {
                System.out.println("Resultado: " + a);
        });

    }
}
