# Continuum environmental monitoring

Progetto **Edge → Cloud** basato sul dataset BME280 di Sensor.Community. In questa
prima vertical slice il flusso funzionante è:

```text
CSV di gennaio → Simulator → MQTT/Mosquitto → Edge logici
                                               │
                                      Kafka → Cloud (prossimo step)
```

Non è presente un livello Fog. La topologia conserva comunque le tre macroaree:
servono per raggruppare gli Edge e per il futuro partizionamento del carico Cloud,
senza obbligare l'architettura ad avere un processo Fog.

Lo stato puntuale rispetto alla traccia è mantenuto in
[`docs/traceability.md`](docs/traceability.md); i requisiti non ancora verificati
non vengono dichiarati come completati.

## Responsabilità

- Il **Simulator** legge le righe reali del CSV, conserva timestamp e valori grezzi,
  individua l'Edge dalla topologia e pubblica un evento MQTT.
- **Mosquitto** consegna ogni evento all'Edge che ha sottoscritto il relativo topic.
- Ogni **Edge** resta in ascolto: non effettua polling. Quando arriva un messaggio,
  controlla schema, sensore, assegnazione e presenza delle misure. Normalizzazione,
  range di validità e finestre saranno implementati nello step successivo.
- **Kafka, Cloud Worker e PostgreSQL** sono già configurati come direzione
  architetturale, ma non fanno ancora parte di questa vertical slice.

## Evento MQTT

Il topic è:

```text
telemetry/{edge_id}/{sensor_id}
```

Ogni Edge sottoscrive `telemetry/<proprio-edge-id>/+`. Il payload JSON contiene
`schema_version`, `event_id`, `sensor_id`, `location_id`, `sequence`, `observed_at`,
`emitted_at` e `measurements`. Non contiene `edge_id` o `macroarea_id`: sono metadati
di routing/infrastruttura, non proprietà prodotte dal sensore.

Si usa MQTT 5 con QoS 1 e `retain=false`. Il PUBACK conferma la ricezione da parte
del broker, non l'elaborazione dell'Edge.

## Replay temporale

Il replay non è un simulatore *next-event*. La deadline reale di ogni riga è:

```text
inizio_replay + (timestamp_csv - primo_timestamp_csv) / replay_speedup
```

Con `replay_speedup: 1000`, 1000 secondi del dataset durano un secondo reale. Righe
con lo stesso timestamp ricevono la stessa deadline: il pacer non introduce attese
tra loro. Rimangono soltanto i tempi reali necessari a codifica e trasporto MQTT.

## Dati e configurazione

- `dataset/2025-01_bme280.csv`: dataset mensile originale, usato offline.
- `dataset/derived/2025-01_bme280_europe_sensors-150_seed-42.csv`: traccia ordinata
  di gennaio per i 150 sensori selezionati; è il file letto dal Simulator.
- `../artifacts/eda/kmeans_topology.csv`: assegnazione
  `sensor_id → edge_id → macroarea_id` generata dal notebook.
- `config/project.yml`: colonne, misure, replay, finestre e middleware.

I path dei dati sono risolti rispetto a `config/project.yml`, non alla directory da
cui viene eseguito il comando.

## Prova locale con Docker Compose

Da `Sensor`, con Docker Desktop avviato:

```powershell
docker compose -f deploy/compose/continuum.yml up -d --build
```

La prima riga della traccia appartiene al sensore `87575`, assegnato a
`edge-m2-0`, già avviato nel Compose. Pubblica una sola riga:

```powershell
go run ./cmd/simulator -config config/project.yml -max-events 1
```

Per prove con più righe devi avviare gli Edge a cui appartengono quei sensori;
ciascun messaggio viene ricevuto soltanto dall'Edge indicato nel suo topic.

Per eseguire l'intera traccia, ometti `-max-events`. Arresta i container con:

```powershell
docker compose -f deploy/compose/continuum.yml down
```

## Verifiche

```powershell
go test ./...
go vet ./...
```

Il prossimo incremento è `Edge validation/normalization → finestre locali → Kafka`.

Per eseguire lo stesso Docker Compose su Amazon EC2, segui la guida
[`deploy/ec2/README.md`](deploy/ec2/README.md). CloudFormation prepara
l'infrastruttura, mentre lo script di deployment carica e avvia il Compose; MQTT
resta raggiungibile soltanto tramite tunnel SSH.
