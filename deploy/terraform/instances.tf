locals {
  instances = {
    simulator = {
      name              = "continuum-simulator"
      instance_type     = var.simulator_instance_type
      security_group_id = aws_security_group.simulator.id
    }
    edge = {
      name              = "continuum-edge"
      instance_type     = var.edge_instance_type
      security_group_id = aws_security_group.edge.id
    }
    cloud-core = {
      name              = "continuum-cloud-core"
      instance_type     = var.cloud_core_instance_type
      security_group_id = aws_security_group.cloud_core.id
    }
    workers = {
      name              = "continuum-workers"
      instance_type     = var.worker_instance_type
      security_group_id = aws_security_group.workers.id
    }
  }
}

resource "aws_instance" "role" {
  for_each = local.instances

  ami                         = var.ami_id
  instance_type               = each.value.instance_type
  key_name                    = var.key_name
  subnet_id                   = data.aws_subnet.pilot.id
  associate_public_ip_address = true
  vpc_security_group_ids      = [each.value.security_group_id]

  user_data                   = file("${path.module}/bootstrap-docker.sh")
  user_data_replace_on_change = true

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.root_volume_size_gb
    encrypted             = true
    delete_on_termination = true
  }

  tags = {
    Name = each.value.name
    Role = each.key
  }

  volume_tags = {
    Name = "${each.value.name}-root"
    Role = each.key
  }

  lifecycle {
    precondition {
      condition     = data.aws_subnet.pilot.vpc_id == data.aws_vpc.pilot.id
      error_message = "The configured subnet_id does not belong to the configured vpc_id."
    }
  }
}
