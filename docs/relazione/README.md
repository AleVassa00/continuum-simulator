# Relazione A2

Relazione in italiano, in formato IEEE a due colonne. Il documento principale è `main.tex`; `main.pdf` è la versione compilata. Il limite della traccia è **8 pagine complessive**: la sezione di valutazione è predisposta per accogliere soltanto i grafici e le tabelle essenziali delle campagne definitive.

## Modifica e compilazione

I capitoli sono in `sections/`; `references.tex` contiene la bibliografia. Il diagramma del deployment è `figures/architettura.png` ed è incluso in `sections/03-architettura.tex`; Amazon RDS appartiene logicamente alla VPC anche se nel PNG è separato graficamente per leggibilità. La figura sul dataset è vettoriale e già inclusa in `figures/workload.pdf`.

Da questa cartella, con TeX Live o MiKTeX e i pacchetti IEEEtran, babel-italian, AMS, graphicx, booktabs, array, tabularx, xcolor, cite, PGF/TikZ e hyperref:

```powershell
pdflatex -interaction=nonstopmode -halt-on-error -no-shell-escape main.tex
pdflatex -interaction=nonstopmode -halt-on-error -no-shell-escape main.tex
```

In Overleaf caricare l'intera cartella e scegliere `main.tex` come documento principale, con compilatore pdfLaTeX. La figura già generata permette di compilare senza Python. Per rigenerarla dai CSV versionati, in un ambiente con Matplotlib:

```powershell
python generate_figures.py
```

Lo script legge `Sensor/dataset/output/edge_k_metrics.csv` e `replay_shards_summary.csv`. Non rigenera il dataset, non esegue il notebook e non avvia esperimenti.

## Corrispondenza con la traccia

La fonte vincolante è `progettoA2_SDCC2025-26.pdf`, nella radice del workspace. Pagina 1: requisiti applicativi e sperimentali. Pagina 2: deroga individuale, contenuti della relazione, limite di 8 pagine e formato ACM/IEEE consigliato. Pagina 3: criteri di valutazione.

| Contenuto richiesto | Dove si trova nella bozza |
|---|---|
| Architettura e scelte motivate | Introduzione, requisiti, architettura |
| Implementazione realizzata | Contratti, code, validazione, commit, sink |
| Piattaforma e librerie | Tabella delle dipendenze Go, deployment, notebook |
| Risultati analizzati | Sezione predisposta; inserire gli esiti delle campagne definitive |
| Valutazione di scalabilità | Protocollo fixed-workload con 1, 2, 3, 4 e 6 Cloud Worker |
| Condizioni di rete emulate | Campagna `tc-netem` sui confini Simulator--Edge ed Edge--Cloud |
| Limitazioni | Stato volatile, perdite, rebalance, singolo broker, rappresentatività |

Gli appunti `../discussione-professoressa-architettura.md` sono stati usati come contesto per computazione locale/globale, assenza del Fog e nodi logici containerizzati. Essendo una ricostruzione, non vengono presentati come una trascrizione né come prova di approvazione formale.

## Fonti interne delle affermazioni

I percorsi della tabella sono relativi a `Sensor/`.

| Affermazione | Evidenza consultata |
|---|---|
| Topologia prima del campionamento, EPSG:3035, seed 42, 20 inizializzazioni, 150 sensori | `notebooks/01_bme280_architecture_eda.ipynb` |
| 5.102 sensori europei, 13 cluster, silhouette 0,521, raggio massimo | `dataset/output/edge_k_metrics.csv`, `edge_population_summary.csv` |
| 2.106.784 righe, 150 sensori, estremi temporali e carico per zona | `dataset/output/replay_shards_summary.csv` |
| Rate naturale riferito a 31 giorni | Notebook e `dataset/output/replay_load_scenarios.csv` |
| Finestre e soglie delle misure | `cmd/edge/aggregation.go`, `processor.go`, `config.go` |
| Scheduling e code del Simulator | `cmd/simulator/replay.go`, `replay_egress.go`, `mqtt.go` |
| Partition, progress, EOS, commit e rebalance | `internal/cloudworker/`, `internal/globalaggregator/`, `cmd/cloud-worker/consumer.go`, `docs/cloud-partition-aggregation.md` |
| Schemi e conversioni | `internal/avrocodec/`, `internal/model/` |
| Sink finale PostgreSQL | `cmd/global-aggregator/postgres.go`, `deploy/postgres/global_aggregates.sql` |
| Librerie e versioni | `go.mod` |
| Numero di worker e accelerazioni | `experiments/baseline.yaml`, `cloud-scale-w*.yaml`, `calibration-aws.yaml` |
| Deployment e quote pilota | `cmd/deploygen/`, `deploy/terraform/variables.tf`, `deploy/resources/aws-pilot.env` |
| Raccolta metriche e controlli | `deploy/scripts/aws/README.md`, `summarize-run.py`, `cmd/kafka-lag-collector/` |
| Test Kafka reali del 9 settembre: esito già documentato, non rieseguito per questa bozza | `docs/cloud-partition-aggregation.md`, `cmd/cloud-worker/kafka_integration_test.go` |

Le due prove AWS citate sono:

- `artifacts/aws-runs/aws-20260909T190314Z-10220-20447/`;
- `artifacts/aws-runs/aws-20260910T121840Z-2091-20631/`.

Sono stati letti `run-summary.csv`, `postprocess-result.json` e i log di orchestrazione. Entrambe risultano fallite. Il 9 settembre il controllo finale si aspettava `edge-0` running quando era exited; il 10 settembre Kafka risulta unhealthy. La bozza non modifica questi giudizi e non diagnostica la causa dell'unhealthy senza ulteriori prove. I conteggi coincidenti non sono una dimostrazione completa di correttezza numerica e non rendono valida una run fallita.

I dati grezzi AWS sono ignorati da Git: quando si prepara la consegna occorre esportare una selezione di artefatti verificabili, priva di credenziali e dettagli non necessari. Le vecchie prove precedenti al collector Go non vanno mescolate alle nuove misure delle risorse.

## Lavoro necessario prima della consegna

1. Completare tre repliche valide per ciascuna configurazione della campagna di scalabilità.
2. Aggregare media e variabilità fra repliche e produrre i grafici indicati nei commenti di `sections/05-valutazione.tex`.
3. Eseguire la campagna di rete e distinguere le prove complete da quelle con perdite o errori.
4. Documentare tipi di istanza, quote container, frequenza delle metriche, run valide/scartate ed equivalenza degli output.
5. Sostituire i commenti promemoria della valutazione con risultati e discussione quantitativa.
6. Aggiornare abstract e conclusioni, quindi ricontrollare il limite delle 8 pagine.

## Verifica eseguita durante la redazione

L'11 settembre 2026 `go test ./... -count=1` è terminato con successo sul codice corrente, usando cache locali al workspace. Il test Kafka opt-in non è stato abilitato. `cmd/edge` e `cmd/simulator` risultano senza test Go; non è stata dichiarata una copertura inesistente. Nessun esperimento remoto è stato avviato per scrivere la relazione e nessun codice applicativo è stato modificato.

Il PDF è stato compilato con pdfLaTeX/MiKTeX, con due passaggi finali per risolvere citazioni e riferimenti. Verificati il numero di pagine, l'assenza di riferimenti irrisolti e di righe oltre i margini segnalate da LaTeX; tutte le sei pagine sono state renderizzate con Poppler e ispezionate visivamente. La figura è stata generata con Matplotlib 3.11.1. `git diff --check -- docs/relazione` non segnala errori di whitespace.
