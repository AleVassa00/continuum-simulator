# Continuum environmental monitoring

Progetto Edge-Cloud in Go basato sul dataset BME280 di Sensor.Community. La
pipeline implementata arriva fino al Global Aggregator:

```text
Replay globale di gennaio
  -> sharding offline con la topologia sperimentale
  -> 13 replay shard
  -> Simulator x 13, una istanza per Edge Site
  -> Mosquitto x 13
  -> Edge Gateway x 13
  -> EdgeAggregate 5m per edge_id
  -> Kafka topic edge-aggregates
  -> Cloud Worker x N, stesso consumer group
  -> CloudEdgeAggregate 15m per edge_id
  -> Kafka topic cloud-edge-aggregates
  -> Global Aggregator
  -> GlobalAggregate nei log
```

Non e presente un livello Fog. Le tredici zone Edge sono nodi logici derivati dal
clustering geografico dei sensori e possono essere eseguite sullo stesso host con
risorse container limitate. Il Global Aggregator combina i contributi degli Edge
per finestra e scrive il risultato nei log.

Lo stato dei requisiti della traccia e mantenuto in
[`docs/traceability.md`](docs/traceability.md).

## Responsabilita

- Il **notebook** usa offline la topologia `sensor_id -> edge_id` per dividere il
  replay globale nei tredici workload locali.
- Ogni **Simulator** legge soltanto il proprio `REPLAY_FILE` e pubblica su un
  unico `MQTT_ENDPOINT`. `SITE_ID` identifica l'istanza, ma non viene usato per
  filtrare il dataset o derivare l'indirizzo del broker. Il pacing deriva da
  `EventTime` rispetto a una `REPLAY_EPOCH` globale; tutti i Simulator ricevono
  lo stesso `REPLAY_START_AT` e comprimono la timeline tramite
  `ACCELERATION_FACTOR`.
- Ogni **Edge** valida le misure e calcola media, somma, minimo, massimo e conteggi
  validi/non validi su finestre tumbling di event time, di default da 5 minuti.
  `GET /readyz` restituisce `200` soltanto dopo la connessione e la subscription
  MQTT a `sensors/+/telemetry` e `replay/<edgeID>/end`; altrimenti restituisce
  `503`. Una subscription fallita temporaneamente viene riprovata fino a tre tentativi complessivi con
  backoff breve, restando non-ready fino al successo effettivo.
- **Kafka** persiste gli `EdgeAggregate` e li distribuisce ai Cloud Worker usando
  la chiave `edge_id`.
- I **Cloud Worker** eseguono un temporal roll-up indipendente per ogni Edge: tre
  finestre locali da 5 minuti formano, per default, una finestra Cloud da 15
  minuti. I Worker non combinano Edge differenti.
- Il **Global Aggregator** consuma `cloud-edge-aggregates` e combina gli Edge
  attesi nella stessa finestra. Gli EOS segnalano il completamento del replay;
  quando tutti gli Edge sono terminati, esegue il flush finale.

## Contratti Kafka

`EdgeAggregate`, `CloudEdgeAggregate` e `GlobalAggregate` sono record JSON senza
campo di versione applicativa. Ogni metrica contiene:

```text
valid, invalid, sum, average, min, max
```

La somma rende componibili gli aggregati: il Worker calcola la media come
`sum(valid values) / valid`, senza effettuare una media delle medie.

`CloudEdgeAggregate` mantiene un solo `edge_id` e aggiunge
`input_aggregates`, cioe il numero di `EdgeAggregate` unici incorporati. Gli ID
sono deterministici: il Cloud Worker valida ogni input e controlla `AggregateID`
prima di incorporarlo. Gli input gia visti nella finestra attiva vengono ignorati
senza cambiare conteggi, misure o progresso temporale. Il set degli ID appartiene
alla finestra di ciascun Edge e viene liberato al cambio finestra o al flush.

La dimensione `CLOUD_WINDOW_SIZE` deve essere un multiplo della finestra Edge. Un
input fuori ordine o che attraversa il confine di una finestra Cloud viene
rifiutato esplicitamente.

Su entrambi i topic Kafka, l'EOS e un marker di controllo: key uguale all'ID
dell'Edge, header `record_type=end_of_replay`, value vuoto e timestamp Kafka
di pubblicazione. Cloud Worker e Global Aggregator ricavano l'Edge soltanto
dalla key, senza deserializzare il value.

## Semantica e limiti attuali

La telemetry MQTT usa QoS 0 sul topic `sensors/<sensorID>/telemetry`. Il Simulator
offre gli eventi a una coda bounded con capacita `TELEMETRY_QUEUE_CAPACITY`
(default 1000); a coda piena scarta localmente la nuova telemetry senza fermare
il pacing. L'egress consuma la coda in ordine e pubblica senza attendere PUBACK
per la telemetry, contabilizzando i tentativi e gli errori di publish.

A EOF reale del CSV il Simulator accoda l'EOS nello stesso stream: soltanto dopo
il drain della telemetry gia accettata pubblica su `replay/<edgeID>/end` un
payload vuoto con QoS 1, senza retain, attendendo il PUBACK. Un errore o timeout
dell'EOS fa fallire il replay. Anche uno shard vuoto produce EOS a EOF.

L'Edge riserva uno slot fisico aggiuntivo della ingress queue all'EOS. La
telemetry usa soltanto la capacita configurata e viene droppata quando questa
e piena. Le metriche riportano capacita, massima occupazione osservata della
sola telemetry e drop per coda piena; l'EOS non contribuisce all'occupazione.

I writer Kafka sono sincroni e richiedono gli acknowledgment del broker; il
consumer effettua commit esplicito dopo avere incorporato l'input e dopo
l'eventuale pubblicazione dell'output. I normali retry Kafka restano attivi;
la deduplica degli `EdgeAggregate` e responsabilita del Cloud Worker.

Lo stato del roll-up Cloud e in memoria. Kafka offre consegna at-least-once, ma
questo incremento non garantisce at-least-once end-to-end in presenza di crash o
rebalance: un Worker puo perdere una finestra non ancora emessa. Il numero di
repliche deve quindi essere fissato prima del replay e mantenuto invariato durante
il singolo esperimento. La fault tolerance non e il requisito individuale scelto.

Le finestre Edge e Cloud vengono chiuse dall'arrivo di un input appartenente alla
finestra successiva. Su EOS, l'Edge pubblica l'ultima finestra prima del marker
Kafka; il Cloud Worker esegue il flush del solo Edge terminato e pubblica il
relativo aggregato prima di inoltrare il marker. Allo shutdown graceful, Edge
e Cloud Worker pubblicano anche l'ultima finestra parziale ancora in memoria.

Nel Global Aggregator l'EOS non aggiorna `lastActivityByEdge`,
`maxWindowEndByEdge` o `firstAggregateAt` e non fa avanzare direttamente il
watermark. Restano invariati i trigger delle finestre globali, la startup grace,
i timeout degli Edge inattivi e la gestione degli aggregati late: il watermark
dipende dagli aggregati validi e dai timeout gia implementati.

## Dati

Il notebook mantiene il replay globale:

- `dataset/derived/2025-01_bme280_europe_sensors-150_seed-42.csv`.

Una singola scansione chunked, basata sulla topologia sperimentale, genera i
replay non versionati:

- `dataset/derived/replay_by_edge/edge-0.csv`;
- ...;
- `dataset/derived/replay_by_edge/edge-12.csv`.

Il dataset raw `dataset/2025-01_bme280.csv`, il replay globale e l'intera
`dataset/derived/` non sono versionati e devono essere materializzati tramite il
notebook. La whitelist Git permette invece esclusivamente questi piccoli
artefatti scientifici sotto `dataset/output/`:

```text
edge_k_metrics.csv
edge_population_summary.csv
edge_centroids.csv
europe_topology.csv
workload_by_edge.csv
workload_edge_summary.csv
kmeans_topology.csv
replay_load_scenarios.csv
replay_shards_summary.csv
```

`dataset/output/kmeans_topology.csv` e l'input sperimentale di `cmd/deploygen`.
Il suo campo `macroarea_id` e un metadato legacy, vale sempre `none`, non
rappresenta un Fog e non viene letto ne dal Simulator ne da deploygen, che usa
soltanto `edge_id`.

Il Simulator non legge la topologia a runtime: `REPLAY_FILE` identifica gia il
workload del sito, che viene letto completamente fino a EOF reale. Non esiste
un limite configurabile al numero di eventi riprodotti.

## Replay temporale accelerato

Ogni Simulator applica indipendentemente la stessa formula:

```text
eventOffset       = EventTime - REPLAY_EPOCH
acceleratedOffset = eventOffset / ACCELERATION_FACTOR
scheduledTime     = REPLAY_START_AT + acceleratedOffset
```

Se il processo e in anticipo, attende fino a `scheduledTime`; se e in ritardo,
offre subito l'evento alla coda senza aggiungere altre attese. Le deadline sono
assolute, quindi il tempo impiegato per parsing non produce deriva cumulativa.
`ACCELERATION_FACTOR` comprime le distanze originali e non rappresenta un target
di eventi al secondo: il throughput risultante dipende dalla densita del dataset.

`REPLAY_EPOCH` e `REPLAY_START_AT` sono identici per tutti i tredici container e
vengono passati dal deployment. Non esistono barrier, coordinatori, handshake o
messaggi fra Simulator: e soltanto configurazione comune del singolo run. In
particolare, nessun Simulator usa il primo evento del proprio shard come epoch.

Il loop rimane sequenziale e conserva l'ordine del CSV. Non vengono introdotti
holdback, watermark, allowed lateness o eventi out-of-order artificiali. Dopo la
deadline, l'evento viene offerto alla coda con `EventTime` originale.
L'egress assegna `EmittedAt` al momento della publish MQTT QoS 0.

`TELEMETRY_QUEUE_CAPACITY` limita la coda locale con drop delle nuove telemetry
a coda piena. A fine replay il riepilogo riporta eventi offerti, accettati e
scartati, scheduling lag medio e massimo, drain, throughput ed esito EOS.
Questo controllo non implementa
fault tolerance: non esistono checkpoint, recovery, durable producer queue o
retry applicativi.

La simulazione di rete ai confini Simulator-Edge Site ed Edge-Cloud/Kafka verra
affrontata separatamente.

## Avvio locale

Da `Sensor`, costruire le immagini applicative:

```powershell
docker build -f deploy/docker/simulator.Dockerfile -t continuum-simulator:local .
docker build -f deploy/docker/edge.Dockerfile -t continuum-edge:local .
docker build -f deploy/docker/cloud-worker.Dockerfile -t continuum-cloud-worker:local .
docker build -f deploy/docker/global-aggregator.Dockerfile -t continuum-global-aggregator:local .
```

Generare il Compose e avviare broker, Edge, Cloud Worker e Global Aggregator:

```powershell
go run ./cmd/deploygen
docker compose -f deploy/compose/continuum.generated.yml up -d
```

Avviare quindi le tredici istanze del profilo `replay`:

```powershell
go run ./cmd/deploygen

docker compose `
  -f deploy/compose/continuum.generated.yml `
  --profile replay up
```

Il Compose locale materializza i parametri da `experiments/baseline.yaml`:
il secondo comando `deploygen`, dopo l'avvio dell'infrastruttura, aggiorna
`REPLAY_START_AT` usando `workload.start_lead_time`. Tutti i tredici container
ricevono lo stesso valore. Il deployment distribuito richiede invece
`REPLAY_START_AT` a runtime. Non viene generato autonomamente nei container e
non costituisce un protocollo di sincronizzazione.

Compose attende automaticamente che Mosquitto sia healthy e che `/readyz`
dell'Edge restituisca `200`. Tutte le istanze usano
`continuum-simulator:local`; cambiano soltanto i parametri locali come `SITE_ID`,
`MQTT_ENDPOINT` e `REPLAY_FILE`, mentre i riferimenti temporali sono globali.
Per eseguire manualmente un solo sito dall'host:

```powershell
$env:SITE_ID="edge-3"
$env:MQTT_ENDPOINT="tcp://localhost:18833"
$env:REPLAY_FILE="dataset/derived/replay_by_edge/edge-3.csv"
$env:REPLAY_EPOCH="2025-01-01T00:00:00Z"
$env:REPLAY_START_AT=(Get-Date).ToUniversalTime().AddSeconds(10).ToString("o")
$env:ACCELERATION_FACTOR="1000"
$env:TELEMETRY_QUEUE_CAPACITY="1000"
go run ./cmd/simulator
```

La porta host e una scelta del deployment locale. Nei container gli endpoint
sono nomi Docker come `tcp://mqtt-edge-3:1883`; su AWS potranno diventare nomi
DNS privati senza modificare il programma Go.

Per confrontare un diverso numero di Worker, avviare una configurazione nuova
prima del replay:

impostare `cloud.workers` in `experiments/baseline.yaml`, rigenerare il Compose
con `go run ./cmd/deploygen` e avviare i servizi generati.

Il topic di output puo essere ispezionato con:

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic cloud-edge-aggregates --from-beginning
```

Al completamento dei CSV, gli EOS attraversano l'intera pipeline dopo i relativi
aggregati. Il Global Aggregator termina dopo gli EOS di tutti gli Edge attesi e
il flush finale, registrando `GLOBAL_REPLAY_COMPLETED`.

## Verifiche

```powershell
gofmt -l cmd internal
go vet ./...
go build ./...
docker compose -f deploy/compose/continuum.generated.yml config
```
