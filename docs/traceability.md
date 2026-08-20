# Tracciabilità dei requisiti A2

Questo documento separa i requisiti obbligatori della traccia dallo stato corrente
del progetto. `Parziale` significa che esiste una base verificabile, ma non ancora
sufficiente per la consegna.

| Requisito | Stato | Evidenza / lavoro rimanente |
|---|---|---|
| Applicazione in Go | Soddisfatto | Simulator, Edge e Cloud Worker sono programmi Go. |
| Almeno Edge e Cloud | Parziale | Edge MQTT funzionante; pipeline Cloud non ancora implementata. |
| Comunicazione persistente tra almeno due componenti | Parziale | MQTT QoS 1 implementato; Kafka persistente Edge-Cloud ancora da implementare. |
| Scalabilità orizzontale | Da implementare | Esperimenti pianificati con 1, 2 e 4 Cloud Worker sullo stesso replay. |
| Tolleranza ai guasti | Non selezionata | Il progetto è individuale e sceglie la scalabilità come requisito principale. |
| Deployment automatizzato e configurabile | Parziale | CloudFormation e script Compose pronti; verifica reale su EC2 ancora da eseguire. |
| Docker Compose su EC2 | Parziale | Compose verificato localmente; deployment EC2 predisposto. |
| Simulazione rete con tc-netem | Da implementare | Verrà applicata al collegamento Edge-Kafka quando tale flusso sarà operativo. |
| Valutazione sperimentale | Da implementare | Throughput, latenza e consumer lag con carico e repliche controllati. |
| Codice, relazione e README | Parziale | Codice e guide in sviluppo; relazione finale massimo 8 pagine. |

## Regola di avanzamento

Un requisito passa a `Soddisfatto` soltanto dopo una prova automatizzata o un
esperimento riproducibile. La sola presenza di un container o di una configurazione
non viene considerata implementazione funzionale.

