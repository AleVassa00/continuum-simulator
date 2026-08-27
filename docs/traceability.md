# Tracciabilità dei requisiti A2

Questo documento separa i requisiti obbligatori della traccia dallo stato corrente
del progetto. `Parziale` significa che esiste una base verificabile, ma non ancora
sufficiente per la consegna.

| Requisito | Stato | Evidenza / lavoro rimanente |
|---|---|---|
| Applicazione in Go | Soddisfatto | Simulator, Edge e Cloud Worker sono programmi Go. |
| Almeno Edge e Cloud | Parziale | Edge e Cloud Worker funzionanti fino al temporal roll-up per Edge; Global Aggregator ancora da implementare. |
| Comunicazione persistente tra almeno due componenti | Soddisfatto | Kafka persistente collega Edge e Cloud Worker tramite `edge-aggregates` e `cloud-edge-aggregates`. |
| Scalabilità orizzontale | Parziale | Consumer group e partizioni supportano repliche multiple; il piano principale richiede esperimenti con 1, 2 e 4 Worker. Un test con 6 Worker, pari alle partizioni di `edge-aggregates`, e soltanto aggiuntivo. |
| Tolleranza ai guasti | Non selezionata | Il progetto è individuale e sceglie la scalabilità come requisito principale. |
| Deployment automatizzato e configurabile | Parziale | Compose generato dalla topologia; automazione multi-host e verifica reale su EC2 ancora da eseguire. |
| Docker Compose su EC2 | Da implementare | Compose locale disponibile; deployment EC2 non ancora implementato. |
| Simulazione rete con tc-netem | Da implementare | La futura valutazione riguardera entrambi i confini: Simulator-Edge Site ed Edge-Cloud/Kafka. Non e ancora implementata. |
| Valutazione sperimentale | Da implementare | Throughput, latenza e consumer lag con carico e repliche controllati. |
| Codice, relazione e README | Parziale | Codice e guide in sviluppo; relazione finale massimo 8 pagine. |

## Regola di avanzamento

Un requisito passa a `Soddisfatto` soltanto dopo una prova automatizzata o un
esperimento riproducibile. La sola presenza di un container o di una configurazione
non viene considerata implementazione funzionale.

## Vincoli sperimentali correnti

Lo stato delle finestre dei Cloud Worker risiede in RAM. Durante ciascun replay
il numero di Worker resta fisso: un crash o un rebalance puo perdere lo stato
parziale. State store, exactly-once, transazioni Kafka e recovery avanzato sono
fuori dal requisito di scalabilita scelto e non sono implementati.

`macroarea_id` e un metadato legacy della topologia, sempre uguale a `none`. Non
rappresenta un livello Fog e non viene letto dal Simulator o da `cmd/deploygen`.
