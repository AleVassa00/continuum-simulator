#!/usr/bin/env bash
#
# ==============================================================================
# CONTINUUM AWS RUNNER - GUIDA RAPIDA
# ==============================================================================
#
# Questo script automatizza il workflow AWS del progetto:
#
#   credenziali/segreti
#       -> controllo AWS
#       -> deploygen
#       -> [opzionale] Terraform
#       -> prepare-pilot
#       -> inizializzazione/verifica schema RDS
#       -> run-experiment
#
# Va salvato in:
#
#   deploy/scripts/aws/run-full.sh
#
# e lanciato dalla root del repository oppure da qualunque altra directory.
#
# ------------------------------------------------------------------------------
# 1. PREREQUISITI UNA TANTUM
# ------------------------------------------------------------------------------
#
# A) Secret locali
#
# Crea:
#
#   ~/.config/continuum/secrets.env
#
# Esempio:
#
#   export TF_VAR_rds_password='UNA_PASSWORD_RDS_DI_ALMENO_16_CARATTERI'
#   export SSH_USER='ubuntu'
#   export SSH_KEY_PATH='/percorso/continuum-key.pem'
#
# Proteggi il file:
#
#   chmod 600 ~/.config/continuum/secrets.env
#
# Il runner lo carica automaticamente: NON serve eseguire manualmente "source"
# prima di ogni run.
#
# B) Credenziali AWS Learner Lab
#
# Il runner carica automaticamente:
#
#   deploy/aws-session.env
#
# Prima di lanciare il runner, aggiorna quel file quando la sessione Learner Lab
# cambia/scade.
#
# C) Comandi locali richiesti
#
#   aws
#   terraform
#   jq
#   ssh
#   go
#
# ------------------------------------------------------------------------------
# 2. PRIMA RUN / INFRASTRUTTURA AWS DA CREARE O MODIFICARE
# ------------------------------------------------------------------------------
#
# Usa --provision:
#
#   bash deploy/scripts/aws/run-full.sh \
#     experiments/cloud-scale-w1.yaml \
#     --provision
#
# Il runner esegue:
#
#   1. verifica credenziali AWS
#   2. deploygen
#   3. terraform init
#   4. terraform plan
#   5. chiede conferma prima di terraform apply
#   6. terraform apply
#   7. prepare-pilot.sh
#   8. init-rds-schema.sh su cloud-core via SSH
#   9. run-experiment.sh
#
# Il piano Terraform viene salvato temporaneamente sotto .build/terraform e poi
# eliminato. La password RDS non viene stampata dal runner.
#
# Usa --provision quando:
#
#   - RDS non esiste ancora;
#   - hai modificato Terraform;
#   - vuoi applicare modifiche all'infrastruttura AWS.
#
# ------------------------------------------------------------------------------
# 3. RUN DOPO MODIFICHE A CODICE O CONFIGURAZIONE ESPERIMENTO
# ------------------------------------------------------------------------------
#
# Se EC2 e RDS esistono già ma hai cambiato codice o YAML:
#
#   bash deploy/scripts/aws/run-full.sh \
#     experiments/cloud-scale-w1.yaml
#
# Il runner:
#
#   - NON esegue terraform apply;
#   - rigenera i Compose con deploygen;
#   - crea/prepara una nuova release con prepare-pilot;
#   - verifica/inizializza lo schema RDS;
#   - avvia l'esperimento.
#
# Questo è il comando normale da usare quando cambi, per esempio:
#
#   acceleration_factor
#   cloud.workers
#   consumer_commit_batch_size
#   kafka.partitions
#   codice Go / Docker / deployment
#
# ------------------------------------------------------------------------------
# 4. RIPETERE LA STESSA IDENTICA RUN
# ------------------------------------------------------------------------------
#
# Se NON hai cambiato codice, YAML o release e vuoi solo una seconda/terza
# ripetizione:
#
#   bash deploy/scripts/aws/run-full.sh \
#     experiments/cloud-scale-w1.yaml \
#     --reuse-release
#
# In questa modalità vengono saltati:
#
#   deploygen
#   Terraform
#   prepare-pilot
#   init-rds-schema
#
# e viene eseguito direttamente run-experiment.sh sulla release già preparata.
#
# Usa questa modalità solo quando la configurazione dell'esperimento è
# esattamente la stessa della release già distribuita.
#
# ------------------------------------------------------------------------------
# 5. COMANDO DI DEFAULT
# ------------------------------------------------------------------------------
#
# Se non passi il file YAML:
#
#   bash deploy/scripts/aws/run-full.sh
#
# viene usato:
#
#   experiments/cloud-scale-w1.yaml
#
# ------------------------------------------------------------------------------
# 6. HELP
# ------------------------------------------------------------------------------
#
#   bash deploy/scripts/aws/run-full.sh --help
#
# ------------------------------------------------------------------------------
# 7. ERRORI COMUNI
# ------------------------------------------------------------------------------
#
# "credenziali AWS assenti/scadute"
#   -> aggiorna deploy/aws-session.env con una nuova sessione Learner Lab.
#
# "TF_VAR_rds_password mancante/non valida"
#   -> controlla ~/.config/continuum/secrets.env.
#      La password deve avere 16-128 caratteri e usare lettere, numeri o:
#      _ + = . ! -
#
# "SSH_USER non impostato" / "SSH_KEY_PATH non impostato"
#   -> aggiungili a ~/.config/continuum/secrets.env.
#
# "output Terraform rds_connection assente"
#   -> RDS non è ancora stato creato/applicato:
#
#      bash deploy/scripts/aws/run-full.sh \
#        experiments/cloud-scale-w1.yaml \
#        --provision
#
# Se hai cambiato il file YAML NON usare --reuse-release.
#
# ==============================================================================
set -Eeuo pipefail
set +x
umask 077

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
source "${SCRIPT_DIR}/pilot-env.sh"
source "${SCRIPT_DIR}/pilot-lock.sh"
readonly DEFAULT_EXPERIMENT="experiments/cloud-scale-w1.yaml"

PROVISION=0
REUSE_RELEASE=0
EXPERIMENT_INPUT=""
PLAN_FILE=""

cleanup() {
  if [[ -n "${PLAN_FILE}" && -f "${PLAN_FILE}" ]]; then
    rm -f -- "${PLAN_FILE}"
  fi
}
trap cleanup EXIT

log() { printf '[run-full] %s\n' "$*"; }
die() { printf '[run-full] ERROR: %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
Uso:
  bash deploy/scripts/aws/run-full.sh [EXPERIMENT_YAML] [--provision]
  bash deploy/scripts/aws/run-full.sh [EXPERIMENT_YAML] --reuse-release

Esempi:
  bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w1.yaml --provision
  bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w1.yaml
  bash deploy/scripts/aws/run-full.sh experiments/cloud-scale-w1.yaml --reuse-release

--provision
  Esegue terraform init, plan e apply. Prima dell'apply chiede conferma.

--reuse-release
  Salta deploygen, Terraform, prepare-pilot e init RDS; esegue solo la run
  sulla release già preparata.

File locali caricati automaticamente:
  deploy/aws-session.env (credenziali AWS)
  ~/.config/continuum/pilot.env (opzioni non segrete)
  ~/.config/continuum/secrets.env

Override:
  CONTINUUM_PILOT_FILE=/path/pilot.env
  CONTINUUM_SECRETS_FILE=/path/secrets.env
  AWS_SESSION_FILE=/path/aws-session.env

Precedenza: argomento YAML > ambiente shell > secrets.env > pilot.env > sessione AWS.
I Compose del wrapper vengono generati in .build/aws-compose.

Variabili richieste:
  SSH_USER
  SSH_KEY_PATH

Il secrets.env può contenere:
  export TF_VAR_rds_password='...'
  export SSH_USER='ubuntu'
  export SSH_KEY_PATH='/path/to/key.pem'
EOF
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "comando richiesto non trovato: $1"
}

resolve_repo_file() {
  local input="$1"
  local candidate
  if [[ "${input}" = /* ]]; then
    candidate="${input}"
  else
    candidate="${REPO_ROOT}/${input}"
  fi
  [[ -f "${candidate}" ]] || return 1
  (
    cd "$(dirname "${candidate}")"
    printf '%s/%s\n' "$(pwd -P)" "$(basename "${candidate}")"
  )
}

validate_rds_secret() {
  [[ "${TF_VAR_rds_password:-}" =~ ^[A-Za-z0-9_+=.!-]{16,128}$ ]] ||
    die "TF_VAR_rds_password mancante/non valida; caricala da ~/.config/continuum/secrets.env"
}

validate_ssh_config() {
  [[ -n "${SSH_USER:-}" ]] || die "SSH_USER non impostato"
  [[ -n "${SSH_KEY_PATH:-}" ]] || die "SSH_KEY_PATH non impostato"
  [[ -f "${SSH_KEY_PATH}" ]] || die "chiave SSH non trovata: ${SSH_KEY_PATH}"
}

validate_aws_session() {
  local account arn
  account="$(aws sts get-caller-identity --query Account --output text)" ||
    die "credenziali AWS assenti/scadute; aggiorna aws-session.env"
  arn="$(aws sts get-caller-identity --query Arn --output text)" ||
    die "impossibile leggere l'identità AWS"
  log "AWS account=${account}"
  log "AWS identity=${arn}"
}

ensure_rds_is_provisioned() {
  "${TERRAFORM_BIN}" -chdir="${TERRAFORM_DIR}" output -json rds_connection >/dev/null 2>&1 ||
    die "output Terraform rds_connection assente; esegui il runner con --provision"
}

run_deploygen() {
  log "[2/6] deploygen"
  (
    cd "${REPO_ROOT}"
    go run ./cmd/deploygen -mode distributed -experiment "${EXPERIMENT_CONFIG}" \
      -distributed-output-dir "${DISTRIBUTED_COMPOSE_DIR}"
  )
}

provision_infrastructure() {
  local plan_dir answer
  log "[3/6] Terraform"
  mkdir -p "${REPO_ROOT}/.build/terraform"
  plan_dir="${REPO_ROOT}/.build/terraform"
  PLAN_FILE="$(mktemp "${plan_dir}/rds-plan.XXXXXX")"
  chmod 0600 "${PLAN_FILE}"

  "${TERRAFORM_BIN}" -chdir="${TERRAFORM_DIR}" init -input=false
  "${TERRAFORM_BIN}" -chdir="${TERRAFORM_DIR}" plan -input=false -out="${PLAN_FILE}"

  printf '\n'
  read -r -p "Applicare questo piano Terraform? [y/N] " answer
  case "${answer}" in
    y|Y|yes|YES) ;;
    *) die "terraform apply annullato" ;;
  esac

  "${TERRAFORM_BIN}" -chdir="${TERRAFORM_DIR}" apply -input=false "${PLAN_FILE}"
  rm -f -- "${PLAN_FILE}"
  PLAN_FILE=""
  ensure_rds_is_provisioned
}

prepare_release() {
  log "[4/6] prepare-pilot"
  (
    cd "${REPO_ROOT}"
    bash ./deploy/scripts/aws/prepare-pilot.sh
  )
}

initialize_rds_schema() {
  local cloud_core_ip
  local -a ssh_args

  log "[5/6] init/verifica schema RDS"
  cloud_core_ip="$(
    "${TERRAFORM_BIN}" -chdir="${TERRAFORM_DIR}" output -json public_ips |
      jq -er '."cloud-core" | select(type == "string" and length > 0)'
  )" || die "impossibile leggere l'IP pubblico di cloud-core"

  ssh_args=(
    -i "${SSH_KEY_PATH}"
    -o BatchMode=yes
    -o ConnectTimeout=10
    -o StrictHostKeyChecking=accept-new
  )

  ssh "${ssh_args[@]}" "${SSH_USER}@${cloud_core_ip}"     'bash /opt/continuum/current/deploy/scripts/aws/init-rds-schema.sh'
}

run_experiment() {
  log "[6/6] run-experiment"
  (
    cd "${REPO_ROOT}"
    bash ./deploy/scripts/aws/run-experiment.sh
  )
}

main() {
  local arg

  for arg in "$@"; do
    case "${arg}" in
      --provision) PROVISION=1 ;;
      --reuse-release) REUSE_RELEASE=1 ;;
      -h|--help) usage; exit 0 ;;
      --*) die "opzione sconosciuta: ${arg}" ;;
      *)
        [[ -z "${EXPERIMENT_INPUT}" ]] || die "specificare un solo file esperimento"
        EXPERIMENT_INPUT="${arg}"
        ;;
    esac
  done

  (( PROVISION == 0 || REUSE_RELEASE == 0 )) ||
    die "--provision e --reuse-release non possono essere usati insieme"

  load_pilot_environment "${REPO_ROOT}"
  export TERRAFORM_BIN="${TERRAFORM_BIN:-terraform}"
  export TERRAFORM_DIR="${TERRAFORM_DIR:-${REPO_ROOT}/deploy/terraform}"
  export DISTRIBUTED_COMPOSE_DIR="${REPO_ROOT}/.build/aws-compose"
  EXPERIMENT_INPUT="${EXPERIMENT_INPUT:-${EXPERIMENT_CONFIG:-${DEFAULT_EXPERIMENT}}}"

  # Imposta il file esperimento DOPO aver caricato aws-session.env, così un
  # eventuale EXPERIMENT_CONFIG presente nel file locale non sovrascrive
  # l'argomento passato al runner.
  EXPERIMENT_CONFIG="$(resolve_repo_file "${EXPERIMENT_INPUT}")" ||
    die "file esperimento non trovato: ${EXPERIMENT_INPUT}"
  export EXPERIMENT_CONFIG

  require_command aws
  require_command "${TERRAFORM_BIN}"
  require_command jq
  require_command ssh
  validate_ssh_config
  # Check every downstream dependency before preparing hosts or provisioning.
  for arg in go git scp tar sha256sum tee "${PYTHON_BIN:-python3}"; do
    require_command "$arg"
  done
  acquire_pilot_lock "${REPO_ROOT}"

  log "[1/6] preflight AWS"
  validate_aws_session

  if (( REUSE_RELEASE == 1 )); then
    log "riuso release: salto deploygen, Terraform, prepare-pilot e init RDS"
    ensure_rds_is_provisioned
    run_experiment
    return
  fi

  require_command go
  validate_rds_secret
  run_deploygen

  if (( PROVISION == 1 )); then
    provision_infrastructure
  else
    log "[3/6] Terraform apply saltato"
    ensure_rds_is_provisioned
  fi

  prepare_release
  initialize_rds_schema
  run_experiment
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
