# Tracciabilità dei requisiti A2

Questo documento separa i requisiti obbligatori della traccia dallo stato corrente
del progetto. `Parziale` significa che esiste una base verificabile, ma non ancora
sufficiente per la consegna.

| Requisito | Stato | Evidenza / lavoro rimanente |
|---|---|---|
| Applicazione in Go | Soddisfatto | Simulator, Edge e Cloud Worker sono programmi Go. |
| Almeno Edge e Cloud | Parziale | Edge e Cloud Worker funzionanti fino al temporal roll-up per Edge; Global Aggregator ancora da implementare. |
| Comunicazione persistente tra almeno due componenti | Soddisfatto | Kafka persistente collega Edge e Cloud Worker tramite `edge-aggregates` e `cloud-edge-aggregates`. |
| Scalabilità orizzontale | Parziale | Consumer group e partizioni supportano repliche multiple; esperimenti 1, 2, 4 e 6 Worker ancora da eseguire. |
| Tolleranza ai guasti | Non selezionata | Il progetto è individuale e sceglie la scalabilità come requisito principale. |
| Deployment automatizzato e configurabile | Parziale | Compose generato dalla topologia; automazione multi-host e verifica reale su EC2 ancora da eseguire. |
| Docker Compose su EC2 | Da implementare | Compose locale disponibile; deployment EC2 non ancora implementato. |
| Simulazione rete con tc-netem | Da implementare | Da applicare al collegamento Edge-Cloud/Kafka. |
| Valutazione sperimentale | Da implementare | Throughput, latenza e consumer lag con carico e repliche controllati. |
| Codice, relazione e README | Parziale | Codice e guide in sviluppo; relazione finale massimo 8 pagine. |

## Regola di avanzamento

Un requisito passa a `Soddisfatto` soltanto dopo una prova automatizzata o un
esperimento riproducibile. La sola presenza di un container o di una configurazione
non viene considerata implementazione funzionale.
