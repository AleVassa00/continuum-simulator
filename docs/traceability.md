# Tracciabilità dei requisiti A2

Questo documento separa i requisiti obbligatori della traccia dallo stato corrente
del progetto. `Parziale` significa che esiste una base verificabile, ma non ancora
sufficiente per la consegna.

Le evidenze distinguono codice implementato, configurazione disponibile e prove
eseguite. `artifacts/experiments/baseline/effective-config.yaml` documenta i
parametri di una run, ma non ne dimostra l'esito. Il worktree contiene prove
locali preliminari con diversi numeri di Worker; non sono evidenza di un
deployment AWS né sostituiscono serie ripetute a parità di carico e senza perdite.

| Requisito | Stato | Evidenza / lavoro rimanente |
|---|---|---|
| Applicazione in Go | Soddisfatto | Simulator, Edge, Cloud Worker e Global Aggregator sono programmi Go; `go vet ./...` e `go build ./...` verificano il codice corrente. |
| Almeno Edge e Cloud | Parziale | Implementata e configurata l'intera pipeline `Simulator -> MQTT -> Edge -> Kafka -> Cloud Worker -> Kafka -> Global Aggregator`, incluso output `GlobalAggregate` nei log. Le verifiche locali dei contratti non sostituiscono una prova end-to-end con broker reali. |
| Comunicazione persistente tra almeno due componenti | Parziale | Implementati producer Kafka sincroni con `RequireAll`, retry e commit espliciti dei consumer. Configurati i topic `edge-aggregates` e `cloud-edge-aggregates` e il volume `kafka-data`; una prova riproducibile di persistenza non è documentata negli artefatti correnti. |
| Scalabilità orizzontale | Parziale | Implementati consumer group e generazione di più Worker; la baseline configura 1 Worker e 6 partizioni di `edge-aggregates`. Restano da documentare gli esperimenti con 1, 2 e 4 Worker; 6 Worker costituiscono una prova aggiuntiva. |
| Tolleranza ai guasti | Non selezionata | Il progetto è individuale e sceglie la scalabilità come requisito principale. |
| Deployment automatizzato e configurabile | Parziale | `cmd/deploygen` genera Compose locali e distribuiti dalla topologia e dalla configurazione YAML. `prepare-pilot.sh` prepara le release multi-host; `run-experiment.sh` orchestra readiness, avvio comune, completamento EOS e raccolta degli artefatti. La verifica sperimentale su EC2 resta da documentare. |
| Docker Compose su EC2 | Parziale | Implementati Terraform e bootstrap Docker per quattro host (Simulator, Edge, Cloud Core, Workers), Compose distribuiti e script AWS in `deploy/`. La presenza di questo codice non dimostra una run EC2 end-to-end completata. |
| Simulazione rete con tc-netem | Da implementare | La futura valutazione riguardera entrambi i confini: Simulator-Edge Site ed Edge-Cloud/Kafka. Non e ancora implementata. |
| Valutazione sperimentale | Parziale | Runner AWS con quote CPU/RAM, preflight capacità, hash di provenienza, collector host/container e lag Kafka, esportazione CSV e controlli di completezza/perdite. Test offline di parser, Bash e Compose in `deploy/scripts/aws/tests`. Restano pilot EC2, calibrazione, controllo crediti T3 e serie ripetute; i timestamp originali delle finestre sono conservati senza calcolare ritardi derivati. |
| Codice, relazione e README | Parziale | Codice e guide in sviluppo; relazione finale massimo 8 pagine. |

## Regola di avanzamento

Un requisito passa a `Soddisfatto` soltanto dopo una prova automatizzata o un
esperimento riproducibile. La sola presenza di un container o di una configurazione
non viene considerata implementazione funzionale.

Le verifiche locali di questo refactor comprendono `gofmt`, `go vet ./...`,
`go build ./...` e prove temporanee su contributi normali, duplicati, conflitti,
Edge differenti, late record ed EOS. Le prove temporanee sono state rimosse;
non attestano una run distribuita con broker reali o misure di scalabilità.

## Vincoli sperimentali correnti

Lo stato delle finestre dei Cloud Worker e del Global Aggregator risiede
esclusivamente in RAM. Gli offset Kafka vengono committati dopo l'elaborazione
di ciascun record, ma non esiste un checkpoint coordinato tra offset e stato
applicativo. Di conseguenza un crash durante una finestra aperta puo perdere
stato derivato da record il cui offset e gia stato committato: al riavvio tali
record non vengono riconsumati e la finestra parziale non e ricostruibile
automaticamente. State store, exactly-once, transazioni Kafka e recovery
avanzato sono fuori dal requisito di scalabilita scelto e non sono implementati.
Questa e una limitazione architetturale accettata, non un bug.

Cloud Worker e Global Aggregator deduplicano gli input nelle rispettive finestre
in memoria. Nel Global Aggregator, stesso Edge e stesso `AggregateID` nella
finestra aperta sono una no-op anche per attività e watermark; un ID differente
dello stesso Edge nella stessa finestra resta errore. Gli ID vengono eliminati
con la finestra; i record destinati a finestre già chiuse seguono la gestione
late esistente. Il watermark resta basato sull'event time, mentre il
processing-time serve alla rilevazione degli Edge idle. Gli EOS sono marker
senza payload e non aggiornano attività, progresso event-time o watermark.

`macroarea_id` e un metadato legacy della topologia, sempre uguale a `none`. Non
rappresenta un livello Fog e non viene letto dal Simulator o da `cmd/deploygen`.
