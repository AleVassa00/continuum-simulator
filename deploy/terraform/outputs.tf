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
