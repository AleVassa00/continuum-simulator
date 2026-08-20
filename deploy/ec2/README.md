# Deployment Docker Compose su Amazon EC2

Questa procedura implementa il requisito minimo della traccia: deployment
automatizzato e configurabile tramite Docker Compose su EC2.

La vertical slice attualmente eseguibile è:

```text
PC: Simulator ── tunnel SSH ──► EC2
                                └── Docker Compose
                                    ├── Mosquitto
                                    └── Edge edge-m2-0
```

Kafka, Cloud Worker e PostgreSQL verranno aggiunti allo stesso Compose quando la
relativa pipeline sarà funzionante. Non sono inseriti come container fittizi.

CloudFormation e Docker Compose hanno responsabilità separate:

- `cloudformation.yml` crea EC2 e Security Group e installa Docker/Compose;
- `deploy-compose.ps1` carica il progetto ed esegue il Compose su EC2;
- `continuum.yml` descrive i componenti applicativi containerizzati.

## 1. AWS Academy Learner Lab

Avvia il Learner Lab e mantieni la stessa Region durante tutta la procedura. Usa:

- la key pair già fornita dal laboratorio, normalmente `vockey`;
- il file PEM scaricato dal laboratorio, normalmente `labsuser.pem`;
- un VPC e una subnet appartenenti alla stessa Region.

Non caricare il PEM nel repository e non condividerlo. Recupera il tuo IPv4
pubblico da `https://checkip.amazonaws.com/` e aggiungi `/32`. Non usare
`0.0.0.0/0` per SSH.

## 2. Crea lo stack CloudFormation

In **CloudFormation → Create stack → With new resources**, seleziona
**Upload a template file** e carica `cloudformation.yml`.

Usa il nome stack `continuum-learning` e configura:

- `VpcId`: il VPC del Learner Lab;
- `SubnetId`: una subnet pubblica dello stesso VPC;
- `KeyName`: normalmente `vockey`;
- `SSHLocation`: il tuo IP pubblico con `/32`;
- `InstanceType`: inizialmente `t3.micro`.

Attendi `CREATE_COMPLETE` e copia `PublicDnsName` dalla scheda **Outputs**. Il
bootstrap installa una versione fissata del plugin Docker Compose e ne verifica il
checksum prima dell'installazione.

## 3. Verifica il bootstrap

Da PowerShell:

```powershell
ssh -i "C:\percorso\labsuser.pem" ec2-user@<PUBLIC_DNS>
```

Dentro EC2:

```bash
sudo cloud-init status --wait
sudo docker version
sudo docker compose version
```

## 4. Esegui il deployment applicativo

Da `Sensor` sul PC:

```powershell
.\deploy\ec2\deploy-compose.ps1 `
  -HostName <PUBLIC_DNS> `
  -KeyPath "C:\percorso\labsuser.pem" `
  -NodeId edge-m2-0
```

Lo script crea un archivio contenente solo sorgenti/configurazione/topologia
necessari, lo carica su EC2, costruisce l'immagine Go e avvia Docker Compose.

Per verificare i container:

```powershell
ssh -i "C:\percorso\labsuser.pem" ec2-user@<PUBLIC_DNS> `
  "sudo docker compose -f /opt/continuum/Sensor/deploy/compose/continuum.yml ps"
```

Per seguire l'Edge:

```powershell
ssh -i "C:\percorso\labsuser.pem" ec2-user@<PUBLIC_DNS> `
  "sudo docker compose -f /opt/continuum/Sensor/deploy/compose/continuum.yml logs -f edge"
```

## 5. Collega il Simulator locale

Assicurati che nessun broker locale occupi la porta 1883. Apri il tunnel e lascia
questo terminale in esecuzione:

```powershell
ssh -i "C:\percorso\labsuser.pem" -N `
  -L 1883:127.0.0.1:1883 ec2-user@<PUBLIC_DNS>
```

In un altro terminale pubblica la prima riga, assegnata a `edge-m2-0`:

```powershell
go run ./cmd/simulator -config config/project.yml -max-events 1
```

Il log `event=accepted` comparirà nel container Edge su EC2.

## 6. Chiusura della sessione

Quando termini la sessione:

1. interrompi il tunnel con `Ctrl+C`;
2. nel Learner Lab premi **End Lab**, che ferma la EC2;
3. non premere **Reset**, perché elimina tutte le risorse senza ripristinare il
   budget.

Al successivo avvio la EC2 può ricevere un nuovo IP/DNS pubblico: recupera il valore
aggiornato dalla Console EC2 prima di riconnetterti.

Quando il progetto non serve più, elimina solamente lo stack
`continuum-learning`, senza modificare gli stack amministrativi di AWS Academy.

