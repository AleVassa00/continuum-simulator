# RDS usa due subnet esistenti ma resta Single-AZ.
data "aws_subnet" "rds" {
  for_each = {
    us-east-1a = "subnet-02dca42218e70700e"
    us-east-1b = "subnet-0ec23203b61e25425"
  }
  id = each.value
}

resource "aws_db_subnet_group" "global" {
  name_prefix = "continuum-global-"
  subnet_ids  = [for subnet in data.aws_subnet.rds : subnet.id]

  lifecycle {
    precondition {
      condition = alltrue([
        for az, subnet in data.aws_subnet.rds :
        subnet.vpc_id == data.aws_vpc.pilot.id && subnet.availability_zone == az
      ])
      error_message = "RDS subnets must belong to the pilot VPC and the specified us-east-1 AZs."
    }
  }
}

resource "aws_security_group" "rds" {
  name_prefix = "continuum-rds-"
  description = "Private PostgreSQL access from cloud-core only"
  vpc_id      = data.aws_vpc.pilot.id
}

resource "aws_vpc_security_group_ingress_rule" "rds_cloud_core" {
  security_group_id            = aws_security_group.rds.id
  referenced_security_group_id = aws_security_group.cloud_core.id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

resource "aws_db_instance" "global" {
  identifier_prefix      = "continuum-global-"
  engine                 = "postgres"
  engine_version         = "17"
  instance_class         = var.rds_instance_class
  allocated_storage      = 20
  storage_type           = "gp3"
  storage_encrypted      = true
  db_name                = var.rds_database_name
  username               = var.rds_username
  password               = var.rds_password
  port                   = 5432
  db_subnet_group_name   = aws_db_subnet_group.global.name
  vpc_security_group_ids = [aws_security_group.rds.id]
  availability_zone      = "us-east-1a"
  multi_az               = false
  publicly_accessible    = false
  ca_cert_identifier     = "rds-ca-rsa2048-g1"

  backup_retention_period = 1
  # Evita cancellazioni accidentali del database.
  deletion_protection       = true
  skip_final_snapshot       = false
  final_snapshot_identifier = "continuum-global-final"
}
