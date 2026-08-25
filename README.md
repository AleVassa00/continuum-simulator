# Continuum environmental monitoring

Progetto Edge-Cloud in Go basato sul dataset BME280 di Sensor.Community. La
pipeline attualmente implementata termina al secondo topic Kafka:

```text
CSV di gennaio
  -> Simulator
  -> MQTT/Mosquitto per zona
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

- Il **Simulator** legge la traccia CSV ordinata, individua l'Edge dalla topologia
  e pubblica ogni misura sul broker MQTT della zona.
- Ogni **Edge** valida le misure e calcola media, somma, minimo, massimo e conteggi
  validi/non validi su finestre tumbling di event time, di default da 5 minuti.
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

MQTT usa QoS 1. I writer Kafka sono sincroni e richiedono gli acknowledgment del
broker; il consumer effettua commit esplicito dopo avere incorporato l'input e
dopo l'eventuale pubblicazione dell'output.

Lo stato del roll-up Cloud e in memoria. Kafka offre consegna at-least-once, ma
questo incremento non garantisce at-least-once end-to-end in presenza di crash o
rebalance: un Worker puo perdere una finestra non ancora emessa. Il numero di
repliche deve quindi essere fissato prima del replay e mantenuto invariato durante
il singolo esperimento. La fault tolerance non e il requisito individuale scelto.

Le finestre vengono chiuse dall'arrivo di un input appartenente alla finestra
successiva. Allo shutdown graceful, Edge e Cloud Worker pubblicano anche l'ultima
finestra parziale ancora in memoria.

## Dati

Il Simulator usa:

- `dataset/derived/2025-01_bme280_europe_sensors-150_seed-42.csv`;
- `dataset/output/kmeans_topology.csv`.

I dati derivati non sono tracciati da Git e devono essere presenti localmente. Il
Simulator pubblica il CSV il piu velocemente possibile: e un replay bounded usato
come generatore di carico, non un replay temporizzato.

## Avvio locale

Da `Sensor`, costruire le immagini applicative:

```powershell
docker build -f deploy/docker/edge.Dockerfile -t continuum-edge:local .
docker build -f deploy/docker/cloud-worker.Dockerfile -t continuum-cloud-worker:local .
```

Generare il Compose dalla topologia e avviare la pipeline:

```powershell
go run ./cmd/deploygen
docker compose -f deploy/compose/continuum.generated.yml up -d
```

Eseguire un replay limitato:

```powershell
$env:MAX_EVENTS="1000"
go run ./cmd/simulator
```

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
