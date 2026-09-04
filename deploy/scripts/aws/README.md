# AWS pilot preparation

`prepare-pilot.sh` prepares the four EC2 instances created by Terraform. It does
not create infrastructure and does not start any container.

## Prerequisites

The local machine must provide Bash 4 or newer, `terraform`, `jq`, `ssh`, `scp`
and `tar`.
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
