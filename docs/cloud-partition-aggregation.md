# Contratto Cloud/Global per source partition

## Scopo e responsabilita

La macroarchitettura rimane Sensor -> MQTT -> Edge -> Kafka -> Cloud Workers ->
Kafka -> Global Aggregator -> sink. Le sei partition di `edge-aggregates` sono
unita logiche stabili; i Worker sono esecutori intercambiabili, non sorgenti degli
aggregati. Il refactor non cambia Sensor/MQTT, le finestre Edge, il producer Edge
o la strategia sperimentale W1/W2/W4/W6. Il Global non conosce membership Edge.

| Worker | Partition per esecutore (distribuzione bilanciata) | Partial logici per finestra con dati ovunque |
| --- | --- | --- |
| W1 | 6 | P0...P5: 6 |
| W2 | 3, 3 | P0...P5: 6 |
| W4 | 2, 2, 1, 1 | P0...P5: 6 |
| W6 | 1 ciascuno | P0...P5: 6 |

L'assegnazione specifica agli ID Worker e decisa da Kafka, non dalla tabella.
Ogni Worker ha un ciclo di calcolo sequenziale e un processor separato per ogni
partition assegnata. Le goroutine di fetch non costituiscono aggregatori logici.
Anche W1 pubblica i partial su Kafka e passa sempre dal Global.

## Identita e routing

- La sorgente effettiva e `kafka.Message.Partition`, non il Worker ID e neppure
  la sola key del record. Cloud verifica che Edge e partition reale corrispondano.
- Stato Cloud: `(source_partition, window_start)` con `window_end` derivato dalla
  durata Cloud; stato Global: esatta coppia `(window_start, window_end)`.
- `CloudPartitionAggregate` contiene source partition, confini temporali,
  `input_aggregates`, eventi e metriche componibili. Non include lista Edge.
- Identita naturale: `(source_partition, window_start, window_end)`. L'ID e
  `partition:<p>:<start UTC RFC3339Nano>:<end UTC RFC3339Nano>`.
  Non ci sono altre dimensioni di raggruppamento nel modello corrente: le tre
  misure sono campi dello stesso record, non gruppi per sensor type.
- Il topic di output e `cloud-partition-aggregates`, con una partition fisica.
  La sua key `0`...`5` identifica la source partition del topic di input.
- I payload data e `PartitionProgress` restano Avro binario con schema embedded;
  nessun JSON nel percorso Kafka, Schema Registry o discovery aggiuntivo.

`CLOUD_EXPECTED_EDGE_IDS` viene configurata solo sui Worker. La membership e
proiettata sulle partition usando direttamente `kafka.Hash.Balance` della stessa
versione della libreria usata dall'Edge, sulla lista ordinata `[0,1,2,3,4,5]`.
`SOURCE_PARTITION_COUNT` vale obbligatoriamente 6 su Worker e Global. I Worker
verificano anche metadata e ID delle partition all'avvio di ogni generazione.
Questa configurazione esplicita e sufficiente per il deployment fisso: non serve
far scoprire al Global gli Edge o introdurre nuovi servizi.

Prerequisiti: membership completa e identica tra Worker, un solo producer attivo
per edgeID, key/hash invariati, topic inizialmente vuoti e partition fisse.
Cambiare il numero di partition durante una run cambia la mappatura Edge ->
partition ed e **non supportato**. Il controllo dei metadata non e una migrazione.

## Perche la finalizzazione e deterministica

Il producer Edge (`cmd/edge/egress.go`) e sincrono (`Async=false`, `BatchSize=1`,
`RequireAll`) e pubblica aggregati ed EOS dalla stessa coda, sulla stessa key
edgeID con `kafka.Hash`. Su EOF l'ultimo EdgeAggregate precede l'EOS nella stessa
partition Kafka. Gli aggregati Edge sono finestre definitive, ordinate e non
sovrapposte; l'Edge corrente non riapre finestre per accettare dati tardivi.

Di conseguenza, dopo aver consumato un aggregato Edge `[a,b)`, Cloud sa che quel
producer non emettera nuovi contributi precedenti a `b`. Questo e un contratto
di progresso, non una supposizione fondata sulla frequenza degli eventi.

Per una partition P:

1. Cloud tiene l'ultimo `window_end` e il flag EOS di **ogni Edge atteso in P**.
2. Un Edge mai osservato e non terminato blocca la finalizzazione.
3. La frontiera e il minimo dei `window_end` degli Edge non terminati, troncato
   al confine Cloud da 15 minuti. Gli Edge terminati non limitano piu la frontiera.
4. Solo le finestre con `window_end <= frontiera` producono partial definitivi.
5. Gli eventuali partial sono pubblicati prima di `PartitionProgress(P, frontiera)`.
6. Quando tutti gli Edge di P hanno inviato EOS, Cloud pubblica le finestre
   residue in ordine temporale e infine `PartitionEndOfReplay(P)`.

Un Edge veloce non puo certificare un Edge lento nella stessa partition. Non
esistono idle timeout o `FlushEdge` che finalizzino prematuramente tutta P.
La chiusura del processo Worker non pubblica finestre non certificate.
Gli intervalli rimangono Edge 5m (configurazione corrente), Cloud 15m e Global
merge degli stessi confini Cloud, incluso l'ultimo intervallo con pochi dati.

## Partition o finestre senza dati

La rappresentazione sul wire e **sparsa**: un partial contiene almeno un input;
un contributo zero viene certificato da progress/EOS, non da un aggregate vuoto.
Questo evita di dover introdurre un orizzonte globale del replay per inventare
finestre su partition che non hanno mai ricevuto alcun dato.

`PartitionProgress(P, t)` dichiara che tutti i partial di P con fine <= t sono
gia stati pubblicati. Se una di quelle finestre non ha partial, il suo contributo
e quindi zero. `PartitionEndOfReplay(P)` estende la garanzia a tutte le finestre.
Una partition senza Edge configurati emette EOS all'assegnazione, anche senza
ricevere record Kafka. Un Edge con shard vuoto deve comunque inviare il suo EOS.

Il numero di partial **con dati** dipende dai dati nelle partition, mai dal numero
di Worker. Con dati in tutte le sei partition si producono esattamente sei
partial per finestra. Finestre interamente vuote nell'intero sistema non vengono
materializzate. Questo dettaglio distingue sei contributi logici certificati da
sei record Avro necessariamente presenti, ed e verificato dai test con partition
vuote. Non si sostituisce mai l'assenza di dati con un timeout.

## Completamento del Global

Il Global non calcola watermark, non tronca timestamp e non assegna nuove
finestre. Per una coppia esatta `(start,end)` attende, per ciascuna P0...P5:

- il partial definitivo per quella coppia; oppure
- progress >= end o EOS che certifichi un contributo zero mancante.

Solo allora riduce i contributi e scrive nel sink. La riduzione somma eventi,
conteggi validi/non validi e somme; combina min/max non nulli; ricalcola
ogni media come `sum/valid`. L'ordine di riduzione delle partition e deterministico.
`expected_partitions=6` e `contributing_partitions=6` indicano completezza logica,
includendo i contributi zero certificati: non contano quanti record hanno dati.

Il replay globale termina solo dopo EOS di tutte le sei partition, emissione di
tutte le finestre pendenti e commit del record finale. Dati e controlli di una
source partition hanno ordine Kafka; interleaving tra source partition e ammesso.

## Deduplica, commit e limiti accettati

La deduplica mantiene gli input delle finestre pendenti e l'ultimo record per
sorgente (Edge in Cloud, partition nel Global). Un duplicato uguale, ignorando
solo il timestamp di emissione, e una no-op; un contenuto conflittuale e un errore.
Un nuovo input fuori ordine/dietro progresso certificato invalida la run, non
viene corretto retroattivamente. EOS duplicati sono idempotenti. Non esiste
deduplica storica illimitata o supporto al replay parziale di una vecchia run.

Cloud committa l'offset successivo (`message.Offset + 1`) tramite la generazione
del consumer group solo dopo il successo delle pubblicazioni di data/controlli
derivate dall'input. Global committa dopo elaborazione/sink. Una publish o commit
fallita causa errore. Stato RAM e offset non sono atomici: questo **non e
exactly-once**, neanche con i retry sincroni o con gli ID deterministici.

Il consumer group puo ribilanciarsi in startup prima di consumare input. Dopo
aver elaborato dati, un cambio generazione invalida la run: lo stato non viene
trasferito. Ripartire da offset gia committati e rifiutato dal Worker. I Worker
rimangono nel gruppo dopo EOS locale per non ribilanciare gli altri. Fissare W
prima di avviare il replay e attendere il gruppo stabile con tutte le istanze.

Crash, rebalance durante il replay o errori richiedono una nuova run completa
con input/offset puliti, non semplicemente il riavvio di un container. Il Global
ha anch'esso stato volatile e non implementa recovery. Nessun checkpoint,
Redis, database intermedio, state migration, transazione o run_id e stato aggiunto.
Il sink PostgreSQL rimane soltanto finale.

Una sorgente assente puo bloccare la frontiera mentre altre avanzano: la memoria
puo crescere con lo skew. Un timeout di orchestrazione invalida la run ma non
autorizza emissioni incomplete. Questa e una limitazione deliberata del progetto
individuale A2 sulla scalabilita, non una garanzia production-grade.

## Compatibilita e file coinvolti

Non mescolare record/versioni precedenti e correnti. Il topic Cloud cambia nome;
i YAML sperimentali non hanno piu `global` con watermark/idle/window policy.
Il Global non legge `EXPECTED_EDGE_IDS`, `GLOBAL_WINDOW_SIZE` o i vecchi timeout.
I sink usano `expected_partitions`/`contributing_partitions`. Lo script
`deploy/postgres/global_aggregates.sql` installa o migra lo schema in transazione:
una vecchia tabella con colonne Edge viene eliminata e ricreata vuota con le
colonne partition. I vecchi dati vengono persi, senza archivio o backup.
Una tabella gia aggiornata e i suoi risultati rimangono intatti nelle riesecuzioni.
Schemi inattesi o dipendenze che impediscono il DROP causano errore; non viene
usato `CASCADE`. Eseguire
con Global fermo, ruolo proprietario e `search_path` corretto; eventuali permessi
per un ruolo sink separato vanno concessi sulla nuova tabella. Nessun database remoto viene migrato
automaticamente all'avvio dell'applicazione.

File del refactor, raggruppati per responsabilita:

- `internal/model/{aggregate,record_type,partition}.go`: contratti e identita.
- `internal/avrocodec/{codec,mapping}.go`, `schemas/cloud_partition_aggregate.avsc`,
  `schemas/partition_progress.avsc`: Avro; rimosso schema Cloud per-Edge.
- `internal/cloudworker/{aggregator,window,validation,membership}.go`: stato,
  mapping reale Hash, progresso e finalizzazione.
- `cmd/cloud-worker/{config,main,kafka,consumer,processor,egress}.go`: assegnazioni,
  partition reale, controlli, publish e commit.
- `internal/globalaggregator/{aggregator,window,validation}.go`: riduzione esatta;
  eliminato il precedente `watermark.go`.
- `cmd/global-aggregator/{config,main,consumer,processor,sink,postgres}.go` e
  `deploy/postgres/global_aggregates.sql`: nuovo protocollo e sink.
- `internal/experiment/config.go`, `cmd/deploygen/main.go`, template locali e
  distribuiti, `experiments/*.yaml`: sei partition e configurazione semplificata.
- Script runner/report locali e AWS, test, README e traceability: contratti e
  metriche coerenti; nessuna modifica ai parametri sperimentali W1/W2/W4/W6.

## Verifica riproducibile

Da `Sensor`:

```text
go test ./...
go vet ./...
go build ./...
gofmt -l cmd internal
python -m unittest discover -s deploy/scripts/aws/tests
```

I test Go ordinari non richiedono Kafka. Coprono codec Avro, Edge veloci/lenti,
partition e intervalli vuoti, EOS, duplicati/conflitti, errori publish/commit,
assenza di una seconda windowing policy e uguaglianza W1/W2/W4/W6. Per abilitare
anche i test Python opzionali configurare `BASH_BIN` e `DEPLOYGEN_BIN` come nella
guida AWS.

Test Kafka isolato (PowerShell; nessuna risorsa AWS):

```powershell
docker compose -p sensor-partition-refactor-test -f deploy/compose/partition-test.yml up -d --wait
$env:KAFKA_INTEGRATION_BROKER = 'localhost:19092'
go test ./cmd/cloud-worker -run '^TestKafkaPartitionPipeline$' -count=1 -timeout 12m -v
Remove-Item Env:KAFKA_INTEGRATION_BROKER
docker compose -p sensor-partition-refactor-test -f deploy/compose/partition-test.yml down
```

Il test usa il consumer group Worker reale, Kafka su entrambi i passaggi e il
reducer Global reale con sink di asserzione in memoria. Controlla risultati uguali
e offset committati per W1/W2/W4/W6, con 13 Edge e con un solo Edge (cinque
partition vuote). Non e un benchmark, una prova del processo Global/PostgreSQL
completo o una prova end-to-end Sensor/MQTT/Edge. Il broker e usa-e-getta senza
volumi del progetto; i topic di test restano nel suo container fino al cleanup.

Verifiche eseguite il 9 settembre 2026:

- `go test ./... -count=1`, `go vet ./...`, `go build ./...`: superati.
- `gofmt -l cmd internal`: nessun file da formattare; `git diff --check`: superato.
- Suite Python con Bash e deploygen abilitati: 25 test superati, nessuno saltato.
- `TestKafkaPartitionPipeline` su Apache Kafka 4.3.0: tutti gli otto casi
  (13 Edge/un Edge x W1/W2/W4/W6) superati. Ogni caso con 13 Edge produce
  18 partial e 3 global, per 637 eventi; ogni caso con un Edge produce 3 partial
  e 3 global, per 7 eventi, con cinque contributi zero certificati per finestra.
  Verificati anche gli offset successivi all'EOS sulle partition popolate.

Nella verifica reale sono emersi e sono stati risolti due dettagli di startup:
il Worker tollera `RebalanceInProgress` solo prima di consumare input; il test
isola la cache metadata tra deployment, evitando il riuso process-wide di
metadata precedenti alla creazione dei nuovi topic. Nessuna di queste attese
viene utilizzata come criterio di completezza di una finestra.
