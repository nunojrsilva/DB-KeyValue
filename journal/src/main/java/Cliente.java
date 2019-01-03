import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public class Cliente {

    public static void main(String args[]){

        HashMap<Long, byte[]> valores = new HashMap<>();
        valores.put(Long.valueOf(1), "ola".getBytes());
        valores.put(Long.valueOf(2), "cenas".getBytes());
        valores.put(Long.valueOf(3), "muitascoisas".getBytes());

        String endereco = "localhost:12341";

        /*if(Integer.parseInt(args[0]) == 1){
            endereco = "localhost:12342";
            valores = new HashMap<>();
            valores.put(Long.valueOf(2), "cenas".getBytes());

        }*/


        ManagedMessagingService ms = NettyMessagingService.builder()
                .withAddress(Address.from(endereco))
                .build();
        ms.start();

        ClienteStub cs = new ClienteStub(ms);

        Collection<Long> c = new ArrayList<>(3);

        ((ArrayList<Long>) c).add(Long.valueOf(1));
        ((ArrayList<Long>) c).add(Long.valueOf(2));

        cs.put(valores)
            .thenAccept(a -> {
                System.out.println("Resultado do Put: " + a);
                cs.get(c).thenAccept(r -> {
                    System.out.println("PedidoGet foi concluido, resultado:" + r);
                    for (Map.Entry<Long, byte []> e : r.entrySet()) {
                        System.out.println("Chave : " + e.getKey() + " Valor: " + new String(e.getValue()));
                    }

                });
        });





    }
}
