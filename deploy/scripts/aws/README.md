# Esecuzione su AWS

## Prerequisiti

Installare:

- AWS CLI
- Terraform
- Go
- Bash
- SSH/SCP

Configurare le credenziali AWS e verificare che siano valide:

```bash
aws sts get-caller-identity
```

## 1. Configurazione Terraform

Creare il file:

```bash
cp deploy/terraform/terraform.tfvars.example \
   deploy/terraform/terraform.tfvars
```

Modificare i valori necessari in:

`deploy/terraform/terraform.tfvars`

Impostare la password di PostgreSQL:

```bash
export TF_VAR_rds_password="<PASSWORD>"
```

## 2. Configurazione deployment

Creare il file:

`deploy/pilot.env`

con:

```bash
SSH_USER=ec2-user
SSH_KEY_PATH=/path/to/private-key.pem

AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...

TF_VAR_rds_password=...

RESOURCE_PROFILE=deploy/resources/aws-pilot.env
```

## 3. Primo avvio

Alla prima esecuzione creare innanzitutto l'infrastruttura AWS tramite Terraform.

Dalla root del repository:

```bash
cd deploy/terraform

terraform init
terraform plan
terraform apply
```

Confermare il `terraform apply` quando richiesto.

Al termine del provisioning tornare alla root del repository:

```bash
cd ../..
```

Distribuire quindi l'applicazione sulle istanze appena create:

```bash
bash deploy/scripts/aws/deploy-pilot.sh \
  experiments/worker-scaling/cloud-scale-w1.yaml
```

Infine avviare l'esperimento:

```bash
bash deploy/scripts/aws/run-full.sh \
  experiments/worker-scaling/cloud-scale-w1.yaml
```

## 4. Esecuzioni successive

Per ripetere lo stesso esperimento senza modifiche al codice o alla configurazione:

```bash
bash deploy/scripts/aws/run-full.sh \
  experiments/worker-scaling/cloud-scale-w1.yaml
```

Se cambia il codice oppure si utilizza un differente file YAML:

```bash
bash deploy/scripts/aws/deploy-pilot.sh \
  <EXPERIMENT_YAML>

bash deploy/scripts/aws/run-full.sh \
  <EXPERIMENT_YAML>
```

## 5. Aggiornamento degli IP EC2

Se le istanze EC2 vengono fermate e successivamente riavviate, aggiornare gli output Terraform:

```bash
bash deploy/scripts/aws/refresh-infra.sh
```

Dopo il refresh è possibile eseguire normalmente il deployment o una nuova run.

## 6. Configurazioni disponibili

### Scalabilità

- `experiments/worker-scaling/cloud-scale-w1.yaml`
- `experiments/worker-scaling/cloud-scale-w2.yaml`
- `experiments/worker-scaling/cloud-scale-w3.yaml`
- `experiments/worker-scaling/cloud-scale-w4.yaml`
- `experiments/worker-scaling/cloud-scale-w6.yaml`

### Rete

- `experiments/network/network-baseline.yaml`
- `experiments/network/edge-cloud-delay-conservative.yaml`
- `experiments/network/edge-cloud-delay-bounded.yaml`
- `experiments/network/edge-cloud-bandwidth-conservative.yaml`
- `experiments/network/edge-cloud-bandwidth-bounded.yaml`

Per eseguire, ad esempio, l'esperimento di limitazione della banda con politica bounded:

```bash
bash deploy/scripts/aws/deploy-pilot.sh \
  experiments/network/edge-cloud-bandwidth-bounded.yaml

bash deploy/scripts/aws/run-full.sh \
  experiments/network/edge-cloud-bandwidth-bounded.yaml
```