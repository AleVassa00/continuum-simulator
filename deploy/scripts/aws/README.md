# AWS pilot preparation

`prepare-pilot.sh` prepares the four EC2 instances created by Terraform. It does
not create infrastructure and does not start any container.

## Prerequisites

The local machine must provide Bash 4 or newer (Git Bash or WSL on Windows),
Git, Go, `terraform`, `jq`, `ssh`, `scp`, `tar`, `sha256sum` and Python >= 3.9.
Use coherent paths/toolchains for the chosen shell. Python scripts use only the
standard library; set `PYTHON_BIN` if the interpreter is not named `python3`.
Terraform must already have state for the applied configuration under
`deploy/terraform`, unless `TERRAFORM_DIR` points to a different initialized
working directory.

Keep the EC2 private key outside the repository. The script rejects key paths
inside the repository tree.

## Usage

```bash
SSH_USER=ubuntu \
SSH_KEY_PATH=/secure/path/continuum-key.pem \
bash ./deploy/scripts/aws/prepare-pilot.sh
```

Use `SSH_USER=ec2-user` when required by the selected AMI.

Optional environment variables:

- `TERRAFORM_BIN`: Terraform executable, default `terraform`;
- `TERRAFORM_DIR`: Terraform working directory;
- `DEPLOYMENT_ID`: release identifier, otherwise a UTC timestamp;
- `SSH_WAIT_ATTEMPTS`: SSH retry count, default `60`;
- `SSH_WAIT_INTERVAL_SECONDS`: retry interval, default `5`;
- `TIME_SYNC_ATTEMPTS`: clock synchronization retry count, default `24`.
- `RESOURCE_PROFILE`: defaults to `deploy/resources/aws-pilot.env`;
- `ALLOW_DIRTY_WORKTREE`: default `0`; explicitly set `1` to prepare and verify
  uncommitted sources by SHA256 without making a commit.

## Result

Each host receives a role-specific release below
`/opt/continuum/releases/<deployment-id>`. After its image build and Compose
validation succeed, `/opt/continuum/current` points to that release.

The generated `.env` files use Terraform private IP outputs:

- Cloud Core advertises Kafka on its private IP;
- Edge and Workers use the Cloud Core private IP;
- Simulator uses the Edge private IP.

The Simulator `.env` deliberately does not set `REPLAY_START_AT`. The real value
must be supplied when the replay is started, after all services are ready.

## Execute one experiment run

After preparation, `run-experiment.sh` executes exactly one run. It resets the
previous containers and Kafka volume, starts Cloud Core, waits for Kafka and its
topics, starts the configured Workers, starts and waits for all 13 Edge
instances, and verifies clock synchronization on every host. Only then it reads
the current UTC time from the Simulator EC2, adds the experiment
`start_lead_time`, and starts all 13 Simulator containers with that single
`REPLAY_START_AT` value. In addition to the preparation prerequisites, this
script requires the Go toolchain locally to validate and materialize the
experiment configuration.

```bash
SSH_USER=ubuntu \
SSH_KEY_PATH=/secure/path/continuum-key.pem \
bash ./deploy/scripts/aws/run-experiment.sh
```

The experiment defaults to `experiments/baseline.yaml`. Select another prepared
experiment or the artifacts destination with:

```bash
EXPERIMENT_CONFIG=/path/to/experiment.yaml \
ARTIFACTS_ROOT=/path/to/aws-runs \
SSH_USER=ubuntu \
SSH_KEY_PATH=/secure/path/continuum-key.pem \
bash ./deploy/scripts/aws/run-experiment.sh
```

Useful optional settings are `RUN_ID`, `KAFKA_READY_TIMEOUT_SECONDS`,
`EDGE_READY_TIMEOUT_SECONDS`, `RUN_COMPLETION_TIMEOUT_SECONDS`, and
`POLL_INTERVAL_SECONDS`. The script aborts on failed startup, missing Kafka
topics, an unhealthy Edge, or an unsynchronized host clock.

Every run receives a unique directory (by default under `artifacts/aws-runs`)
containing the requested and effective experiment configuration, the actual
common replay timestamp, Worker count, Terraform host addresses, orchestration
metadata, and logs from all four hosts. The effective configuration is written
only after the replay timestamp has been computed from the synchronized
Simulator host. Services are intentionally left available for inspection after
completion; the next run begins by resetting them.

## Pilot corrente: calibrazione, non configurazione definitiva

`experiments/calibration-aws.yaml` parte da accelerazione **10000**, avvio con
60s di anticipo, code Simulator/Edge 5000, tolleranza di avvio 10s e 1 Worker.
Finestre Edge/Cloud 5m/15m, watermark 15m, idle timeout 5s, dataset e 13 siti
restano invariati. Le quote AWS sono diverse dalla macchina locale: 10000 è un
punto di partenza da misurare, non un carico già approvato per lo scaling.

`cmd/deploygen` non supporta campi CPU/RAM nei YAML sperimentali: i quattro
template distribuiti richiedono le variabili del profilo separato
`deploy/resources/aws-pilot.env`. Il Compose locale non cambia.

| Host / componenti | Istanza proposta | CPU massima totale | RAM massima totale |
| --- | --- | ---: | ---: |
| Simulator: 13 × 0.10 CPU / 64 MiB | t3.small | 1.30 | 832 MiB |
| Edge: 13 × 0.08 CPU / 64 MiB + 13 MQTT × 0.04 CPU / 32 MiB | t3.small | 1.56 | 1248 MiB |
| Core: Kafka 1 CPU / 2048 MiB, init 0.25 / 512 MiB, Global 0.50 / 256 MiB | t3.medium | 1.75 | 2816 MiB |
| Workers: ogni replica 0.25 CPU / 128 MiB, con 1/2/4/6 repliche | t3.small | 0.25 / 0.50 / 1 / 1.50 | 128 / 256 / 512 / 768 MiB |

Il Cloud Core ha più RAM, non più vCPU. Verificare i tipi consentiti nel Learner
Lab prima di applicare Terraform. Sono **limiti massimi, non core dedicati**:
restano possibili contesa I/O, throttling, OOM e drop. Swap del container
disabilitato (`memswap_limit = mem_limit`). Il preflight somma anche i servizi
temporanei, verifica la capacità reale degli host e lascia almeno 0.25 CPU e
512 MiB all'host. Il confronto scala container su un host Workers fisso, non
il numero di EC2: la quota per replica rimane costante, quella totale cresce.
Le 6 partizioni permettono al massimo 6 Worker attivi utili nel consumer group.
Le repliche si scelgono prima del replay, senza ridimensionamento durante la run.

## Primo avvio su Learner Lab

Prima del deployment verificare credenziali temporanee (incluso session token),
VPC, subnet, AMI, tipi consentiti, key pair e IP amministratore `/32`. Compilare
il file locale ignorato `terraform.tfvars` partendo dall'esempio: gli ID presenti
non garantiscono l'esistenza delle risorse nella sessione corrente. Esaminare il
piano prima dell'apply; nessuno script esegue automaticamente Terraform apply.
`terraform validate` non verifica i permessi AWS o l'esistenza delle risorse.

L'esempio mantiene la key pair `continuum-key`: deve corrispondere alla chiave
privata posseduta, non al solo nome del file PEM. Non versionare chiavi o credenziali.

Dopo la creazione delle istanze, da `Sensor` in Bash:

```bash
export SSH_USER=ubuntu                          # oppure ec2-user secondo l'AMI
export SSH_KEY_PATH="$HOME/.ssh/continuum-key.pem" # adattare al file reale
export EXPERIMENT_CONFIG="$PWD/experiments/calibration-aws.yaml"
export RESOURCE_PROFILE="$PWD/deploy/resources/aws-pilot.env"
export ALLOW_DIRTY_WORKTREE=1                    # esplicito, nessun commit richiesto

go run ./cmd/deploygen -mode distributed -experiment "$EXPERIMENT_CONFIG" &&
bash ./deploy/scripts/aws/prepare-pilot.sh &&
bash ./deploy/scripts/aws/run-experiment.sh
```

È una sola calibrazione. Il runner **rimuove container e volume Kafka della
precedente prova di questa installazione**: esportare prima i dati da conservare.
Gli artefatti locali preesistenti non vengono cancellati. Al termine non spegne
EC2 e non interrompe i relativi costi.

Se cambia il YAML, anche solo il numero di Worker, rigenerare e preparare la
release prima del runner. Cambiare soltanto `EXPERIMENT_CONFIG` non aggiorna i
Compose remoti e viene rifiutato dal controllo hash. A parità di configurazione
e release basta ripetere il runner, che crea directory uniche con timestamp.

## Provenienza e serie definitive

Il release manifest registra commit, `source_dirty`, SHA256 degli input sorgenti,
configurazione, Compose, replay shard, profilo risorse e `.env` per ruolo; cache
e artefatti non invalidano il fingerprint dei sorgenti. Il runner rifiuta input
locali o file remoti incoerenti con la release. Gli image ID e digest disponibili
sono conservati: la preparazione ricostruisce/pulla immagini, quindi controllare
che anche le immagini applicative e broker siano identiche fra le run confrontate,
non soltanto i tag. Se cambiano, ripreparare la serie coerentemente.

I YAML locali `cloud-scale-w1/w2/w4/w6.yaml` a 40000 non diventano automaticamente
definitivi su AWS. Prima verificare zero drop/errori MQTT, coda Edge < 100%, EOS e
conteggi completi, nessuna perdita/duplicazione globale, nessun restart/OOM, lag
Cloud persistente durante il replay e lag finale Cloud/Global zero. Un singolo
picco isolato non dimostra pressione significativa sui Worker. Controllare anche
Simulator, Kafka, host e crediti T3 per evitare colli di bottiglia estranei.

Se gli Edge perdono dati, abbassare il fattore; se i Worker sono poco impegnati,
aumentarlo gradualmente senza aggiungere sensori. Solo dopo la calibrazione
fissare gli stessi parametri per 1/2/4 Worker, con almeno 3 run ciascuno; 6 Worker
è aggiuntivo. Cambiano solo `cloud.workers` e `experiment.name` (uguale al nome
file). Non confrontare run con perdite o workload diverso. Alternare l'ordine
delle configurazioni aiuta a rilevare effetti di riscaldamento e crediti CPU.

## Artefatti, metriche e controlli di qualità

Ogni directory AWS conserva anche profilo risorse, Compose normalizzati JSON/YAML,
budget e capacità degli host, identità EC2, stati container e campioni grezzi.
Il post-processing produce:

- `run-summary.csv`, `postprocess-result.json`: esito, durata replay, drop,
  conteggi, lag Cloud/Global, CPU/RAM media e massima di Worker e Kafka;
- `simulator-stats.csv`, `edge-stats.csv`, `global-windows.csv`: dettaglio per
  sito/finestra, incluse le metriche aggregate;
- `container-stats.csv`, `container-stats-samples.csv`: CPU/RAM per container;
- `kafka-lag.csv`: snapshot completi durante il replay; i grezzi sono in
  `metrics/kafka-lag.log` e `kafka-consumer-groups-final.txt`.

L'intervallo va da `REPLAY_START_AT` al timestamp Docker di
`GLOBAL_REPLAY_COMPLETED`, non al momento in cui il polling lo scopre.
`replay_elapsed_seconds` include il drain fino al completamento globale.
CPU/RAM sono filtrate sull'intervallo: media aritmetica dei campioni, non media
ponderata nel tempo. CPU Docker 100% equivale a un core. Il riepilogo Worker
somma tutte le repliche per campione completo prima di calcolare media/massimo;
il CSV conserva anche i valori individuali. La RAM usa `MemUsage` Docker, non
la RAM totale dell'host, disponibile nei grezzi insieme a CPU, disco e rete.

Il massimo lag è campionato e può non catturare il picco reale. Si accettano
solo query riuscite con tutte le 6/1 partizioni; query fallite/incomplete non
valgono zero. Il timestamp di completamento della query definisce l'intervallo
di misura; la query non è una fotografia atomica. Le CLI Java aggiungono carico
a Kafka: mantenere uguale la strumentazione. `METRICS_INTERVAL_SECONDS` (default
5) è una pausa fra raccolte, non la frequenza esatta. I record dei topic includono
anche EOS, non solo aggregati.

`global-windows.csv` conserva `window_start`, `window_end` ed `emitted_at`
originali per tutte le finestre, inclusa l'ultima, senza calcolare ritardi derivati.
I log grezzi restano invariati.
La completezza globale assume il dataset attuale con tutti i 13 siti contributori,
non costituisce una regola universale per dataset sparsi.

Exit code post-processing: `0` controlli superati; `2` misure disponibili ma
qualità fallita; `1` misure mancanti/malformate. Un errore invalida anche un vecchio
`run-summary.csv` positivo. Il runner propaga questo errore se l'orchestrazione
è riuscita. `status=completed` descrive solo l'orchestrazione: controllare anche
`quality_status` e `postprocess_status`. Un quality pass non dimostra carico
adeguato, crediti sufficienti o scalabilità: serve l'analisi della serie.

Rielaborazione senza replay:

```bash
python3 deploy/scripts/aws/summarize-run.py artifacts/aws-runs/<run-id>
```

I grezzi del runner PowerShell locale hanno formato diverso e non vanno passati
direttamente al parser AWS. Per T3 conservare modalità crediti e metriche
CloudWatch per InstanceId: `CPUCreditBalance`, `CPUCreditUsage`,
`CPUSurplusCreditBalance`, `CPUSurplusCreditsCharged`. I template dei comandi e
l'intervallo sono in `run-metadata.json`; la raccolta non è automatica. La
risoluzione può differire dai campioni Docker: documentare throttling/surplus,
senza attribuirli al numero di Worker.

La simulazione rete `tc-netem` resta un requisito separato da implementare e
misurare. Questa modifica non aggiunge Fog e non cambia watermark, semantiche
MQTT/Kafka, EOS o aggregazioni. Le modifiche Go già presenti nel worktree devono
essere valutate separatamente.

## Test locali senza avvio di container o accesso AWS

```bash
go test ./... && go vet ./... && go build ./...
terraform -chdir=deploy/terraform fmt -check
terraform -chdir=deploy/terraform validate
python3 -m unittest discover -s deploy/scripts/aws/tests -v

# Attivare anche le integrazioni con veri Bash, deploygen e Docker Compose:
mkdir -p .build/aws-pilot-checks
go build -o .build/aws-pilot-checks/deploygen.exe ./cmd/deploygen
BASH_BIN="$(command -v bash)" \
DEPLOYGEN_BIN="$PWD/.build/aws-pilot-checks/deploygen.exe" \
python3 -m unittest discover -s deploy/scripts/aws/tests -v
```

I test generano file temporanei per 1/2/4/6 Worker e verificano quote, hash,
normalizzazione e invarianti del workload senza modificare esperimenti e
Compose locale. Senza le variabili corrispondenti le integrazioni sono saltate:
per una verifica completa controllare l'assenza di skip.
