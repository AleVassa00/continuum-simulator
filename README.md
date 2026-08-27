# Continuum environmental monitoring

Progetto Edge-Cloud in Go basato sul dataset BME280 di Sensor.Community. La
pipeline attualmente implementata termina al secondo topic Kafka:

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
```

Non e presente un livello Fog. Le tredici zone Edge sono nodi logici derivati dal
clustering geografico dei sensori e possono essere eseguite sullo stesso host con
risorse container limitate. Global Aggregator, storage finale, deployment AWS e
`tc-netem` appartengono agli incrementi successivi.

Lo stato dei requisiti della traccia e mantenuto in
[`docs/traceability.md`](docs/traceability.md).

## Responsabilita

- Il **notebook** usa offline la topologia `sensor_id -> edge_id` per dividere il
  replay globale nei tredici workload locali.
- Ogni **Simulator** legge soltanto il proprio `REPLAY_FILE` e pubblica su un
  unico `MQTT_ENDPOINT`. `SITE_ID` identifica l'istanza, ma non viene usato per
  filtrare il dataset o derivare l'indirizzo del broker. Il pacing deriva da
  `ObservedAt` rispetto a una `REPLAY_EPOCH` globale; tutti i Simulator ricevono
  lo stesso `REPLAY_START_AT` e comprimono la timeline tramite
  `ACCELERATION_FACTOR`.
- Ogni **Edge** valida le misure e calcola media, somma, minimo, massimo e conteggi
  validi/non validi su finestre tumbling di event time, di default da 5 minuti.
  `GET /readyz` restituisce `200` soltanto dopo la connessione e la subscription
  MQTT a `sensors/+/telemetry`; altrimenti restituisce `503`. Una subscription
  fallita temporaneamente viene riprovata fino a tre tentativi complessivi con
  backoff breve, restando non-ready fino al successo effettivo.
- **Kafka** persiste gli `EdgeAggregate` e li distribuisce ai Cloud Worker usando
  la chiave `edge_id`.
- I **Cloud Worker** eseguono un temporal roll-up indipendente per ogni Edge: tre
  finestre locali da 5 minuti formano, per default, una finestra Cloud da 15
  minuti. I Worker non combinano Edge differenti.

## Contratti Kafka

`EdgeAggregate` usa `schema_version=2`. Ogni metrica contiene:

```text
valid, invalid, sum, average, min, max
```

La somma rende componibili gli aggregati: il Worker calcola la media come
`sum(valid values) / valid`, senza effettuare una media delle medie.

`CloudEdgeAggregate` usa `schema_version=1`, mantiene un solo `edge_id` e aggiunge
`input_aggregates`, cioe il numero di `EdgeAggregate` unici incorporati. Gli ID
sono deterministici e gli input duplicati nella finestra attiva vengono ignorati.

La dimensione `CLOUD_WINDOW_SIZE` deve essere un multiplo della finestra Edge. Un
input fuori ordine o che attraversa il confine di una finestra Cloud viene
rifiutato esplicitamente.

## Semantica e limiti attuali

MQTT usa QoS 1. Il Simulator pubblica in modo asincrono e conserva una FIFO
limitata di token in-flight; il limite di default e 1000. Quando raggiunge il
limite attende il token piu vecchio, mentre alla fine del CSV esegue il drain di
tutti i token rimasti. Un timeout o errore MQTT rende il run non valido: non
esiste alcun retry applicativo o tentativo di ripubblicazione.

I writer Kafka sono sincroni e richiedono gli acknowledgment del broker; il
consumer effettua commit esplicito dopo avere incorporato l'input e dopo
l'eventuale pubblicazione dell'output.

Lo stato del roll-up Cloud e in memoria. Kafka offre consegna at-least-once, ma
questo incremento non garantisce at-least-once end-to-end in presenza di crash o
rebalance: un Worker puo perdere una finestra non ancora emessa. Il numero di
repliche deve quindi essere fissato prima del replay e mantenuto invariato durante
il singolo esperimento. La fault tolerance non e il requisito individuale scelto.

Le finestre vengono chiuse dall'arrivo di un input appartenente alla finestra
successiva. Allo shutdown graceful, Edge e Cloud Worker pubblicano anche l'ultima
finestra parziale ancora in memoria.

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
workload del sito. `MAX_EVENTS` limita ogni singola istanza; con valore `1000`,
ciascuno dei tredici Simulator puo pubblicare fino a 1000 eventi. Non e un
limite globale coordinato.

## Replay temporale accelerato

Ogni Simulator applica indipendentemente la stessa formula:

```text
eventOffset       = ObservedAt - REPLAY_EPOCH
acceleratedOffset = eventOffset / ACCELERATION_FACTOR
scheduledTime     = REPLAY_START_AT + acceleratedOffset
```

Se il processo e in anticipo, attende fino a `scheduledTime`; se e in ritardo,
pubblica immediatamente senza aggiungere altre attese. Le deadline sono assolute,
quindi il tempo impiegato per parsing e publish non produce deriva cumulativa.
`ACCELERATION_FACTOR` comprime le distanze originali e non rappresenta un target
di eventi al secondo: il throughput risultante dipende dalla densita del dataset.

`REPLAY_EPOCH` e `REPLAY_START_AT` sono identici per tutti i tredici container e
vengono passati dal deployment. Non esistono barrier, coordinatori, handshake o
messaggi fra Simulator: e soltanto configurazione comune del singolo run. In
particolare, nessun Simulator usa il primo evento del proprio shard come epoch.

Il loop rimane sequenziale e conserva l'ordine del CSV. Non vengono introdotti
holdback, watermark, allowed lateness o eventi out-of-order artificiali. Dopo la
deadline, l'evento viene costruito con `ObservedAt` originale ed `EmittedAt`
reale, quindi pubblicato QoS 1 senza attendere immediatamente il relativo PUBACK.

`MQTT_MAX_IN_FLIGHT` limita i token pendenti e applica backpressure FIFO. A fine
replay il riepilogo riporta eventi, scheduling lag medio e massimo, picco
in-flight, durata reale e throughput medio. Questo controllo non implementa
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
```

Generare il Compose e avviare broker, Edge e Cloud Worker:

```powershell
go run ./cmd/deploygen
docker compose -f deploy/compose/continuum.generated.yml up -d
```

Avviare quindi le tredici istanze del profilo `replay`:

```powershell
$env:REPLAY_START_AT = (
    Get-Date
).ToUniversalTime().AddSeconds(10).ToString("o")

$env:REPLAY_EPOCH="2025-01-01T00:00:00Z"
$env:ACCELERATION_FACTOR="1000"
$env:MQTT_MAX_IN_FLIGHT="1000"
$env:MAX_EVENTS="1000"

docker compose `
  -f deploy/compose/continuum.generated.yml `
  --profile replay up
```

`REPLAY_START_AT` e obbligatorio. Va calcolato una sola volta, qualche secondo
nel futuro, prima del comando Compose: in questo modo tutti i tredici container
ricevono esattamente lo stesso valore. Non viene generato autonomamente nei
container e non costituisce un protocollo di sincronizzazione.

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
$env:MQTT_MAX_IN_FLIGHT="1000"
$env:MAX_EVENTS="1000"
go run ./cmd/simulator
```

La porta host e una scelta del deployment locale. Nei container gli endpoint
sono nomi Docker come `tcp://mqtt-edge-3:1883`; su AWS potranno diventare nomi
DNS privati senza modificare il programma Go.

Per confrontare un diverso numero di Worker, avviare una configurazione nuova
prima del replay:

```powershell
docker compose -f deploy/compose/continuum.generated.yml up -d --scale cloud-worker=4
```

Il topic di output puo essere ispezionato con:

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic cloud-edge-aggregates --from-beginning
```

Per preservare le ultime finestre, arrestare prima gli Edge, attendere che i
Worker consumino gli ultimi aggregati e arrestare infine i Worker. Il Global
Aggregator introdurra successivamente una gestione esplicita della fine replay.

## Verifiche

```powershell
go test ./...
go vet ./...
docker compose -f deploy/compose/continuum.generated.yml config
```
