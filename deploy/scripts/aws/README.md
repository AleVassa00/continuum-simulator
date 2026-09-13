# AWS pilot preparation

`prepare-pilot.sh` prepares the four EC2 instances created by Terraform. It does
not create infrastructure and does not start any container.

## Prerequisites

Run inside WSL/Linux with Bash 4 or newer, `flock`, Git, Go, `terraform`, `jq`,
`ssh`, `scp`, `tar`, `sha256sum` and Python >= 3.9.
Use coherent paths/toolchains for the chosen shell. Python scripts use only the
standard library; set `PYTHON_BIN` if the interpreter is not named `python3`.
Terraform must already have state for the applied configuration under
`deploy/terraform`, unless `TERRAFORM_DIR` points to a different initialized
working directory.

Keep the EC2 private key outside the repository. The script rejects key paths
inside the repository tree.

## Usage

### Runner and environment configuration

Use `run-full.sh` as the normal entry point:

```bash
bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w1.yaml
# Repeat the exact prepared release:
bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w1.yaml --reuse-release
```

Both runners default to `experiments/cloud-scale-w1.yaml`. `--provision` is
explicit and still requests confirmation before applying its saved Terraform
plan. Nothing is provisioned in the normal/reuse modes.

All three entry points (`run-full`, `prepare-pilot`, `run-experiment`) share
the local environment loader. Precedence, highest first:

1. Explicit YAML argument to `run-full`.
2. Variables already set in the calling shell.
3. `~/.config/continuum/secrets.env` (existing WSL password/SSH setup may stay here).
4. `~/.config/continuum/pilot.env` (optional non-secret options).
5. `deploy/aws-session.env` (AWS credentials).
6. Script defaults.

Override the three file locations with `CONTINUUM_SECRETS_FILE`,
`CONTINUUM_PILOT_FILE`, `AWS_SESSION_FILE`. An explicitly selected missing file
is an error. Children inherit the resolved environment without loading the
files again. Keep only AWS variables in the session file; existing SSH/profile
settings there remain supported, but dedicated files and shell overrides win.
Relative SSH key/resource-profile/Terraform/artifact paths are rooted at the repo.
Use `TERRAFORM_BIN` consistently for a non-default Terraform executable.

Suggested optional `pilot.env` (no password):

```bash
SSH_USER=ubuntu
SSH_KEY_PATH=/home/your-user/.ssh/continuum-key.pem
RESOURCE_PROFILE=deploy/resources/aws-pilot.env
PYTHON_BIN=python3
```

Keep experiment parameters in YAML and resource limits in the resource profile.
Remote role `.env` files are generated outputs, not another configuration source
to edit manually. Changing source code still requires a commit or explicit
`ALLOW_DIRTY_WORKTREE=1`; generated files and Terraform state no longer trigger
that source-only check. YAML, Compose, dataset and environment integrity remain
checked through the release manifest.

The wrapper generates Compose/manifest files under ignored `.build/aws-compose`
using deploygen's `-distributed-output-dir`, leaving versioned examples intact.
For manual `prepare-pilot.sh`, `DISTRIBUTED_COMPOSE_DIR` selects that directory;
when omitted it continues to use `deploy/compose/distributed`.

A shared `flock` prevents simultaneous preparation/runs from the same checkout,
and is held until final artifact export. It is not a distributed lock across
different machines/checkouts: administer this pilot from one WSL checkout.
Do not delete `.build/aws-pilot.lock` while a runner is active. Ordinary staged
files use umask 022, dataset directories/files are explicitly 0755/0644, and
secret environments/archives remain 0600.

### RDS PostgreSQL

Terraform also defines one private, encrypted, Single-AZ PostgreSQL 17 instance
in `us-east-1a`, using the two existing subnets in `us-east-1a` and `us-east-1b`.
Only the `cloud-core` security group can connect to port 5432. The four EC2
definitions are unchanged. `rds_database_name`, `rds_username` and
`rds_instance_class` are configurable (defaults: `continuum`, `continuum_admin`,
`db.t3.micro`). This small burstable class is a pilot baseline, not a performance
guarantee. No infrastructure is created by the preparation script.

Supply a random password through `TF_VAR_rds_password` when operating Terraform
and supply **the same value** to `prepare-pilot.sh`. The accepted alphabet is
letters, digits and `_+=.!-`, length 16–128; this intentionally avoids dotenv
interpolation/escaping. In Bash, read it without shell-history exposure:

```bash
read -rsp 'RDS password: ' TF_VAR_rds_password; printf '\n'
export TF_VAR_rds_password
```

Never commit the password, `.env`, Terraform state or saved plans. `sensitive`
redacts normal Terraform output but **does not encrypt state/plan files**: keep
them in protected storage. The pre-existing tracked `deploy/terraform/tfplan`
must not be reused/overwritten for RDS: new plans must stay outside the repository
or under ignored `.build/`. Do not publish JSON state/plan output.
Do not run preparation with shell tracing enabled in a parent shell.

Preparation reads the non-secret `rds_connection` output and writes the complete
`GLOBAL_POSTGRES_*` environment only on `cloud-core`, with mode 0600. Transfer
archives are mode 0600 and release directories mode 0700. Docker build contexts
exclude `.env`; Compose files contain references, not actual credentials.
Normalized Compose artifacts redact the password. Host administrators and users
with Docker access can still read runtime credentials; protect those privileges
and remove obsolete release environments when retiring a deployment.

The experiment runner automatically initializes/verifies the schema after
stopping the previous containers and before truncating PostgreSQL or starting
the new Global. This also applies to `--reuse-release` and direct runs.
For manual installation only, with experiments stopped, run on **cloud-core**:

```bash
bash /opt/continuum/current/deploy/scripts/aws/init-rds-schema.sh
```

This starts only a temporary PostgreSQL client from the Global image (no Kafka
dependencies), connecting privately to RDS. It installs
`deploy/postgres/global_aggregates.sql` with SQL errors treated as failures.
If the partition-based table already exists, it preserves its data; incompatible
legacy tables are refused, not migrated/dropped. Run while experiments are stopped.
The existing runner still explicitly truncates `global_aggregates` before each
Global start and aborts if the reset fails. Preparation never initializes the DB
or starts containers automatically.

Both the sink and schema client use `verify-full`, with the official regional
[AWS RDS CA bundle](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html)
downloaded during the image build; rebuild when updating trusted certificates.
The schema client adds `postgresql17-client` to the Global image. No Go sink or
aggregation code changes are required. The pilot uses the configured RDS admin
account for installation and writes; a least-privilege runtime account is a
separate hardening step.

Deletion protection is enabled and a final snapshot is required when explicitly
dismantling RDS; disable protection deliberately and choose an unused final
snapshot identifier if `continuum-global-final` already exists. Automated backups
are retained for one day. This integration does not verify live subnet routing,
NACLs, instance-class availability or credentials; check these at deployment time.

After successful workload completion and after stopping metric collectors, the
runner exports PostgreSQL rows into `global-aggregates.ndjson` in the run's
artifact directory. The summary reads this file for the postgres sink and logs
for the log sink, retaining the same calculations. Export failures fail the
command explicitly; partial exports remain `.tmp` and are not used as results.
No duplicate logging is added to the application data path. The database still
contains only the current run and is truncated before the next one.

### Prepare the hosts

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
Finestre Edge/Cloud 5m/15m, dataset e 13 siti restano invariati. Il Cloud certifica
il progresso per source partition; il Global riduce le stesse finestre senza
watermark o idle timeout propri. Le quote AWS sono diverse dalla macchina locale: 10000 è un
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
di misura; la query non è una fotografia atomica. Il lag è la differenza fra
log-end offset e offset committato dal consumer group, non una latenza in secondi.
I record dei topic includono anche EOS, non solo aggregati.

Il collector Go persistente legge soltanto gli offset (`OffsetFetch` e
`ListOffsets`), senza entrare nei consumer group o fare commit. Viene avviato
come container temporaneo dalla stessa immagine preparata del Global, con
0.05 CPU e 64 MiB: condivide la rete di Kafka ma non il suo cgroup. Non avvia
CLI Java nel broker; il costo di osservazione resta presente sull'host ma la
CPU del collector non viene più conteggiata come CPU del container Kafka.
Il runner lo rimuove alla chiusura e usa un helper analogo per lo snapshot finale.
I nomi/formati degli artefatti e il parser restano invariati. Un offset non ancora
committato è indisponibile, non zero.

`METRICS_INTERVAL_SECONDS` (default 5) imposta la cadenza richiesta del lag;
query lente saltano i tick senza accumulare richieste concorrenti (timeout di
3 secondi per gruppo). Per le metriche host/Docker resta una pausa fra raccolte,
non la frequenza esatta. Mantenere identica la strumentazione fra W1/W2/W4/W6;
non confrontare direttamente la CPU Kafka con run che usavano le CLI Java.

Il controllo lifecycle richiede Edge running/healthy prima del replay ed
exited/0 dopo EOS; per gli Edge terminati non richiede una healthcheck positiva.
Restart e OOM restano errori, mentre i broker MQTT devono restare running/healthy.
Dopo questo aggiornamento è necessario ripetere `prepare-pilot.sh` per includere
il collector nell'immagine e riallineare il fingerprint dei sorgenti, poi eseguire
il runner. Le vecchie run e i relativi giudizi di qualità non vengono modificati.

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
misurare. Il refactor Cloud/Global non aggiunge Fog e preserva MQTT e il contratto
Edge -> Kafka. Il topic di output e ora `cloud-partition-aggregates`; il Global
conosce soltanto le sei source partition. Le regole di progresso/EOS e le
incompatibilita con vecchie run sono nel
[contratto Cloud/Global](../../../docs/cloud-partition-aggregation.md).

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
