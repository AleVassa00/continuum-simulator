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
| Comunicazione persistente tra almeno due componenti | Parziale | Implementati producer Kafka sincroni con `RequireAll`, retry e commit espliciti dei consumer. Configurati i topic `edge-aggregates` e `cloud-partition-aggregates` e il volume `kafka-data`; i test del refactor non dimostrano recovery applicativo da crash. |
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

Le verifiche del refactor sono mantenute nel repository: `go test ./...`,
`gofmt`, `go vet ./...`, `go build ./...`, test Avro/progresso/EOS/deduplica,
test di equivalenza W1/W2/W4/W6 e dei relativi Compose. Un test opt-in esercita
il consumer group e i due passaggi Kafka reali; istruzioni nel
[contratto Cloud/Global](cloud-partition-aggregation.md). Queste verifiche non
sostituiscono misure di scalabilita o una run completa Simulator-MQTT-Edge su EC2.

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

Cloud Worker e Global deduplicano gli input nelle finestre pendenti, conservando
anche l'ultimo record per sorgente. I conflitti e i nuovi record dietro progresso
gia certificato invalidano la run; non esiste uno storico di deduplica illimitato.
Il Cloud conosce la membership Edge -> partition usando lo stesso Hash del
producer; finalizza tramite il minimo progresso degli Edge non terminati.
Il Global conosce solo P0...P5: riduce finestre identiche dopo partial o certificati
di contributo zero. EOS e progresso hanno ordine rispetto ai dati, mai semantica
basata sul silenzio. Le partition restano sei per tutta la run; crash o rebalance
con stato consumato richiedono il riavvio completo dell'esperimento.

`macroarea_id` e un metadato legacy della topologia, sempre uguale a `none`. Non
rappresenta un livello Fog e non viene letto dal Simulator o da `cmd/deploygen`.
