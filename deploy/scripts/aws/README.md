# AWS pilot deployment

La parte AWS è divisa in tre responsabilità semplici.

## 1. Terraform: infrastruttura

Terraform gestisce esclusivamente le risorse AWS: EC2, security group, rete e RDS.

Per una normale run Terraform non viene eseguito.

```bash
bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w1.yaml
```

Se gli IP pubblici sono cambiati dopo stop/start delle EC2:

```bash
bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w1.yaml --refresh
```

Se prima della run deve essere eseguito anche il deployment applicativo, aggiornare
gli output Terraform separatamente e poi procedere con deploy e run:

```bash
bash deploy/scripts/aws/refresh-infra.sh
bash deploy/scripts/aws/deploy-pilot.sh experiments/cloud-scale-w1.yaml
bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w1.yaml
```

Se l'infrastruttura deve essere realmente modificata:

```bash
bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w1.yaml --provision
```

`--provision` esegue soltanto `terraform init`, `plan` e `apply` prima di avviare
`run-experiment.sh`. Non distribuisce l'applicazione: `deploy-pilot.sh` resta un
comando esplicito e separato.
`--refresh` richiama `refresh-infra.sh`, che esegue `terraform init` e
`apply -refresh-only` senza modificare il deployment applicativo.

Se il provisioning crea o sostituisce una EC2, la run successiva segnalerà che il
deployment applicativo è assente. In quel caso eseguire esplicitamente
`deploy-pilot.sh` e poi rilanciare `run-full.sh` senza `--provision`.

## 2. deploy-pilot.sh: deployment applicativo

Quando cambia codice, Dockerfile o configurazione dell'esperimento che modifica i Compose,
si distribuisce nuovamente l'applicazione:

```bash
bash deploy/scripts/aws/deploy-pilot.sh experiments/cloud-scale-w1.yaml
```

Lo script:

1. legge `deploy/pilot.env`;
2. legge da Terraform gli indirizzi delle quattro EC2 e la configurazione RDS;
3. esegue `deploygen`;
4. copia i file in `/opt/continuum/current`;
5. scrive il `.env` specifico di ogni host;
6. copia i replay shard solo sull'host Simulator;
7. costruisce le immagini Docker;
8. valida i Compose.

Non vengono create release versionate e non vengono confrontati hash dei sorgenti locali con
la macchina remota. Prima della run viene verificata soltanto la presenza dei file necessari
su ciascun host e che `deployment-info.json` indichi l'esperimento richiesto.

L'immagine applicativa usa il tag locale `current`.

## 3. run-experiment.sh: esperimento

`run-experiment.sh` non distribuisce software e non modifica l'infrastruttura.

Si occupa soltanto del ciclo dell'esperimento:

- reset dei container della run precedente;
- verifica/inizializzazione schema RDS;
- avvio Kafka, Global Aggregator, Cloud Worker, Edge e Simulator;
- attivazione del Partition Coordinator;
- raccolta metriche;
- attesa del completamento;
- export dei risultati PostgreSQL;
- generazione degli artefatti sotto
  `artifacts/aws-runs/<nome-esperimento>/<run-id>/`.

Normalmente non viene lanciato direttamente: `run-full.sh` gli passa
`EXPERIMENT_CONFIG`.

## Configurazione locale

L'unico file locale di configurazione è:

```text
deploy/pilot.env
```

Deve rimanere fuori da Git.

Per crearlo a partire dall'esempio:

```bash
cp deploy/pilot.env.example deploy/pilot.env
```

Esempio di campi:

```bash
SSH_USER=ec2-user
SSH_KEY_PATH=/percorso/chiave.pem

AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...

TF_VAR_rds_password=...
RESOURCE_PROFILE=deploy/resources/aws-pilot.env
```

## Workflow pratico

Dopo una modifica al codice o al file esperimento:

```bash
bash deploy/scripts/aws/deploy-pilot.sh experiments/cloud-scale-w1.yaml
bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w1.yaml
```

Per ripetere la stessa identica configurazione:

```bash
bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w1.yaml
```

Per lo scale-out Cloud, dato che `cloud.workers` modifica il Compose generato:

```bash
bash deploy/scripts/aws/deploy-pilot.sh experiments/cloud-scale-w1.yaml
bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w1.yaml

bash deploy/scripts/aws/deploy-pilot.sh experiments/cloud-scale-w2.yaml
bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w2.yaml

bash deploy/scripts/aws/deploy-pilot.sh experiments/cloud-scale-w4.yaml
bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w4.yaml

bash deploy/scripts/aws/deploy-pilot.sh experiments/cloud-scale-w6.yaml
bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w6.yaml
```
