resource "aws_security_group" "simulator" {
  name_prefix = "continuum-simulator-"
  description = "Security group for the continuum Simulator host"
  vpc_id      = data.aws_vpc.pilot.id

  tags = {
    Name = "continuum-simulator"
    Role = "simulator"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "edge" {
  name_prefix = "continuum-edge-"
  description = "Security group for the continuum Edge host"
  vpc_id      = data.aws_vpc.pilot.id

  tags = {
    Name = "continuum-edge"
    Role = "edge"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "cloud_core" {
  name_prefix = "continuum-cloud-core-"
  description = "Security group for the continuum Cloud Core host"
  vpc_id      = data.aws_vpc.pilot.id

  tags = {
    Name = "continuum-cloud-core"
    Role = "cloud-core"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_security_group" "workers" {
  name_prefix = "continuum-workers-"
  description = "Security group for the continuum Worker host"
  vpc_id      = data.aws_vpc.pilot.id

  tags = {
    Name = "continuum-workers"
    Role = "workers"
  }

  lifecycle {
    create_before_destroy = true
  }
}

locals {
  role_security_groups = {
    simulator  = aws_security_group.simulator.id
    edge       = aws_security_group.edge.id
    cloud-core = aws_security_group.cloud_core.id
    workers    = aws_security_group.workers.id
  }
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  for_each = local.role_security_groups

  security_group_id = each.value
  description       = "SSH administration from the configured administrator CIDR"
  cidr_ipv4         = var.admin_cidr
  from_port         = 22
  ip_protocol       = "tcp"
  to_port           = 22

  tags = {
    Name = "${each.key}-ssh-admin"
    Role = each.key
  }
}

resource "aws_vpc_security_group_egress_rule" "all_outbound" {
  for_each = local.role_security_groups

  security_group_id = each.value
  description       = "Outbound access for bootstrap, image downloads and application traffic"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"

  tags = {
    Name = "${each.key}-all-outbound"
    Role = each.key
  }
}

resource "aws_vpc_security_group_ingress_rule" "simulator_to_edge_mqtt" {
  security_group_id            = aws_security_group.edge.id
  referenced_security_group_id = aws_security_group.simulator.id
  description                  = "MQTT telemetry and replay control from Simulator to Edge"
  from_port                    = 18830
  ip_protocol                  = "tcp"
  to_port                      = 18842

  tags = {
    Name = "simulator-to-edge-mqtt"
    Role = "edge"
  }
}

resource "aws_vpc_security_group_ingress_rule" "edge_to_cloud_core_kafka" {
  security_group_id            = aws_security_group.cloud_core.id
  referenced_security_group_id = aws_security_group.edge.id
  description                  = "Kafka traffic from Edge to Cloud Core"
  from_port                    = 9092
  ip_protocol                  = "tcp"
  to_port                      = 9092

  tags = {
    Name = "edge-to-cloud-core-kafka"
    Role = "cloud-core"
  }
}

resource "aws_vpc_security_group_ingress_rule" "workers_to_cloud_core_kafka" {
  security_group_id            = aws_security_group.cloud_core.id
  referenced_security_group_id = aws_security_group.workers.id
  description                  = "Kafka traffic from Workers to Cloud Core"
  from_port                    = 9092
  ip_protocol                  = "tcp"
  to_port                      = 9092

  tags = {
    Name = "workers-to-cloud-core-kafka"
    Role = "cloud-core"
  }
}
