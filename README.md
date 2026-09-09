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
  -> Kafka topic edge-aggregates / Avro
  -> Cloud Worker x N, stesso consumer group
  -> CloudPartitionAggregate 15m per source_partition (P0...P5)
  -> Kafka topic cloud-partition-aggregates / Avro
  -> Global Aggregator
  -> GlobalAggregate: log JSON (default) oppure colonne PostgreSQL
```

Non e presente un livello Fog. Le tredici zone Edge sono nodi logici derivati dal
clustering geografico dei sensori e possono essere eseguite sullo stesso host con
risorse container limitate. Il Global Aggregator combina i contributi delle partition
per finestra e scrive il risultato nel sink selezionato: log JSON o PostgreSQL.

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
- I **Cloud Worker** mantengono stato distinto per ogni partition Kafka di input
  assegnata dal consumer group. Combinano gli Edge di quella partition in finestre
  Cloud da 15 minuti. La membership Edge rimane qui, per certificare il progresso.
- Il **Global Aggregator** consuma `cloud-partition-aggregates` e riduce i partial
  di P0...P5 con identici confini di finestra. Non conosce gli Edge e non applica
  una seconda windowing policy. Termina dopo gli EOS delle sei source partition.

Il [contratto Cloud/Global](docs/cloud-partition-aggregation.md) descrive
finalizzazione, contributi vuoti, EOS, deduplica e limiti di recovery.

## Contratti Kafka

I payload di `EdgeAggregate` su `edge-aggregates` e di `CloudPartitionAggregate` su
`cloud-partition-aggregates` sono singoli record Apache Avro binari, senza prefissi
aggiuntivi. Gli schema statici sono in
[`internal/avrocodec/schemas`](internal/avrocodec/schemas) e vengono incorporati
nei binari; producer e consumer utilizzano lo stesso contratto durante ogni run.
Le nuove esecuzioni richiedono topic e offset nuovi: i precedenti record JSON
o Avro per-Edge non sono compatibili con il contratto Cloud per-partition.
`internal/avrocodec` esegue il mapping, mentre la business logic continua a usare
le struct di `internal/model`. MQTT e il sink finale `log` restano JSON; il sink
`postgres` salva invece colonne tipizzate. Ogni metrica contiene:

```text
valid, invalid, sum, average, min, max
```

Ogni schema definisce `MetricAggregate` una sola volta e lo riutilizza per le tre
misure; la definizione e identica nei due schema. `average`, `min` e `max` usano
union `["null", "double"]`, con default `null`.

I timestamp usano `long` con logical type `timestamp-nanos`: il mapping esplicito
conserva i nanosecondi di `time.Time`, inclusi eventuali confini di finestre
inferiori al millisecondo, e restituisce istanti UTC. Il codec rifiuta timestamp
fuori dall'intervallo rappresentabile come nanosecondi Unix signed a 64 bit
(circa 1677-2262), anziche troncarli o produrre overflow.
I contatori rimangono `uint64` nel dominio Go: vengono convertiti in Avro `long`
solo se non superano `9223372036854775807`; in lettura sono rifiutati i negativi.
Il decoder verifica la struttura Avro e l'assenza di byte residui. Il consumer
valida una volta gli identificatori, le finestre e le metriche prima di passare
il record all'aggregatore; deduplica e vincoli sullo stato restano nell'aggregatore.

La somma rende componibili gli aggregati: il Worker calcola la media come
`sum(valid values) / valid`, senza effettuare una media delle medie.

`CloudPartitionAggregate` contiene `source_partition`, i confini della finestra,
le metriche e `input_aggregates`, numero di `EdgeAggregate` unici incorporati.
Non contiene Worker ID o lista degli Edge. L'identita naturale e
`source_partition + window_start + window_end`; l'ID include tutti e tre i campi.
I duplicati identici nelle finestre pendenti sono no-op; quelli conflittuali sono
errori. Si conserva anche l'ultimo record per sorgente, non uno storico illimitato.

La dimensione `CLOUD_WINDOW_SIZE` deve essere un multiplo della finestra Edge. Un
input fuori ordine o che attraversa il confine di una finestra Cloud viene
rifiutato esplicitamente.

Su `edge-aggregates`, EOS resta key `edgeID`, header `record_type=end_of_replay`
e value vuoto. L'Edge pubblica sincronicamente l'ultimo aggregato prima dell'EOS,
usando lo stesso `kafka.Hash` e la stessa key. Le sei partition restano fisse.
Su `cloud-partition-aggregates`, data e controlli usano key `source_partition`:
`partition_progress` ha payload Avro, `partition_end_of_replay` ha value vuoto.
Ogni controllo segue tutti i partial che certifica. La source partition e quella
di input, non la partition fisica del topic di output (attualmente una sola).

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

Lo stato delle finestre dei Cloud Worker e del Global Aggregator risiede
esclusivamente in RAM. Gli offset Kafka vengono committati dopo l'elaborazione
di ciascun record, ma non esiste un checkpoint coordinato tra offset e stato
applicativo. Di conseguenza un crash durante una finestra aperta puo perdere
stato derivato da record il cui offset e gia stato committato: al riavvio tali
record non vengono riconsumati e la finestra parziale non e ricostruibile
automaticamente. Il numero di repliche deve quindi essere fissato prima del replay
e mantenuto invariato durante il singolo esperimento. La fault tolerance non e il
requisito individuale scelto ed e documentata come limitazione architetturale nota.

L'Edge chiude una finestra quando passa alla successiva; su EOS pubblica l'ultima
prima del marker Kafka. Il Cloud finalizza una finestra solo quando tutti gli Edge
della relativa partition hanno certificato progresso oltre il suo confine o EOS.
Un Edge non ancora osservato blocca il progresso: nessun timeout lo esclude.
Su EOS dell'ultimo Edge della partition il Cloud emette i partial rimasti, poi
`PartitionEndOfReplay`. Lo shutdown del Worker non forza il flush di stato incompleto.

Il Global attende per ciascuna delle sei partition un partial definitivo oppure
una certificazione ordinata di contributo zero (progress/EOS). Nessun idle timeout,
watermark ricalcolato o finestra aggiuntiva. Una sorgente bloccata puo trattenere
finestre in RAM: il timeout della run segnala un esperimento incompleto, non ne
simula la completezza. Rebalance dopo l'inizio del processamento invalida la run.

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

usare YAML separati, senza ritoccare `experiments/baseline.yaml` a ogni prova.
I file `cloud-scale-w1.yaml`, `cloud-scale-w2.yaml`, `cloud-scale-w4.yaml` (e
`cloud-scale-w6.yaml` opzionale) mantengono lo stesso workload locale e cambiano
soltanto nome esperimento e numero di Worker. Dopo la calibrazione eseguire
almeno tre ripetizioni per configurazione, conservando gli artefatti grezzi:

```powershell
.\script\run-local-experiment.ps1 -Experiment experiments\cloud-scale-w1.yaml -KeepRawArtifacts
# Nelle ripetizioni successive, se le immagini non sono cambiate: -SkipBuild
```

Il runner crea directory diverse per run. Non considerare valide per il confronto
le run con drop, misure mancanti o post-processing fallito, anche se il replay
risulta completato. Per EC2 usare il pilot separato `calibration-aws.yaml` e la
[guida AWS](deploy/scripts/aws/README.md): risorse vincolate e carico iniziale
diversi richiedono una nuova calibrazione prima delle serie definitive.

I topic degli aggregati contengono Avro binario e richiedono il relativo schema
per la decodifica. Con il sink predefinito, i risultati finali sono JSON nei log:

```powershell
docker compose -f deploy/compose/continuum.generated.yml logs global-aggregator
```

Al completamento dei CSV, gli EOS attraversano l'intera pipeline dopo i relativi
aggregati. Il Global Aggregator termina dopo gli EOS delle sei source partition e
il merge finale, registrando `GLOBAL_REPLAY_COMPLETED` dopo il commit Kafka.

## Sink finale del Global Aggregator

`GLOBAL_SINK_TYPE` ammette solo `log` e `postgres`. Il default `log` mantiene
l'output `GLOBAL_AGGREGATE {...}` su stdout, con gli stessi nomi snake_case e
valori nullable. Il Compose locale usa questo default e non richiede PostgreSQL.
Il mapping JSON e privato al sink: `MetricAggregate`, `EdgeAggregate`,
`CloudPartitionAggregate` e `GlobalAggregate` non hanno tag di serializzazione.
`SensorEvent` e `NullableFloat64` conservano il contratto MQTT/JSON.

Solo con `GLOBAL_SINK_TYPE=postgres` vengono caricate queste variabili:

| Variabile | Regola |
| --- | --- |
| `GLOBAL_POSTGRES_HOST` | Obbligatoria |
| `GLOBAL_POSTGRES_PORT` | Default `5432`, intero tra 1 e 65535 |
| `GLOBAL_POSTGRES_DATABASE` | Obbligatoria |
| `GLOBAL_POSTGRES_USER` | Obbligatoria |
| `GLOBAL_POSTGRES_PASSWORD` | Obbligatoria; preservata esattamente e mai stampata |
| `GLOBAL_POSTGRES_SSLMODE` | Default `verify-full`; ammessi anche `disable`, `allow`, `prefer`, `require`, `verify-ca` |

La configurazione viene validata al caricamento. Un unico `pgxpool` viene creato
all'avvio e verificato con `Ping`; un errore impedisce l'avvio del consumer Kafka.
Il pool viene riutilizzato per le INSERT e chiuso all'uscita, anche in caso di
errore. Ping e INSERT hanno timeout di 5 secondi. `verify-full` verifica anche
il certificato e il nome del server; l'immagine include i certificati CA di sistema.

Prima dell'avvio (con Global fermo), installare o aggiornare la tabella applicando
[`deploy/postgres/global_aggregates.sql`](deploy/postgres/global_aggregates.sql)
con `psql` o con gli strumenti di amministrazione PostgreSQL. Ad esempio, con
host, database e utente sostituiti ai segnaposto e password richiesta interattivamente:

```text
psql "host=HOST port=5432 dbname=DATABASE user=USER sslmode=verify-full" -W -v ON_ERROR_STOP=1 -f deploy/postgres/global_aggregates.sql
```

Il programma non crea la tabella. Le INSERT usano parametri SQL e
`ON CONFLICT (aggregate_id) DO NOTHING`: un retry non aggiorna la riga esistente.
Lo stesso script elimina la tabella con le vecchie colonne Edge e ricrea
`global_aggregates` con `expected_partitions` e `contributing_partitions`.
**I vecchi dati vengono cancellati, senza archivio o backup.** Una tabella gia
aggiornata viene invece conservata con tutti i suoi dati. Lo script e
transazionale e rieseguibile; schemi inattesi o dipendenze che impediscono
il DROP causano errore, senza usare `CASCADE`.
Eseguirlo con il ruolo proprietario della tabella e il corretto `search_path`.
Se il sink usa un ruolo diverso, concedere su quella nuova gli stessi permessi
di INSERT necessari.
I contatori oltre `MaxInt64` vengono rifiutati prima della query; i puntatori
`*float64` nil diventano `NULL`. I timestamp sono `TIMESTAMPTZ`, con precisione
PostgreSQL al microsecondo; la precisione e la logica in memoria/Avro restano invariate.
Le metriche occupano colonne normali, utilizzabili direttamente nelle query:

```sql
SELECT window_start, temperature_average
FROM global_aggregates
WHERE temperature_average > 25
ORDER BY window_start;
```

Il template distribuito `distributed-cloud-core.compose.tmpl` inoltra le variabili
`GLOBAL_SINK_TYPE` e `GLOBAL_POSTGRES_*` dall'environment del processo Compose o
dal suo file `.env`; non contiene credenziali. Dopo modifiche al template usare
`go run ./cmd/deploygen -mode distributed`. PostgreSQL viene configurato esternamente;
non e aggiunto alcun servizio database al deployment.

## Verifiche

```powershell
gofmt -l cmd internal
go test ./...
go vet ./...
go build ./...
docker compose -f deploy/compose/continuum.generated.yml config
```
