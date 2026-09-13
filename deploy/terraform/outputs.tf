output "rds_endpoint" {
  description = "Private PostgreSQL endpoint (hostname:port)."
  value       = aws_db_instance.global.endpoint
}

output "rds_port" {
  value = aws_db_instance.global.port
}

output "rds_connection" {
  description = "Non-secret connection settings consumed by prepare-pilot.sh."
  value = {
    host     = aws_db_instance.global.address
    port     = aws_db_instance.global.port
    database = aws_db_instance.global.db_name
    username = aws_db_instance.global.username
  }
}

output "public_ips" {
  description = "Public IPv4 addresses used only for pilot administration."
  value = {
    for role, instance in aws_instance.role : role => instance.public_ip
  }
}

output "private_ips" {
  description = "Private IPv4 addresses to use for all application traffic."
  value = {
    for role, instance in aws_instance.role : role => instance.private_ip
  }
}

output "private_dns" {
  description = "Private DNS names assigned by the VPC, when enabled."
  value = {
    for role, instance in aws_instance.role : role => instance.private_dns
  }
}
