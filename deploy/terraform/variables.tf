variable "aws_region" {
  description = "AWS region in which to create the pilot instances."
  type        = string
  default     = "us-east-1"
}

variable "vpc_id" {
  description = "ID of the existing VPC used by the pilot."
  type        = string

  validation {
    condition     = can(regex("^vpc-[0-9a-f]+$", var.vpc_id))
    error_message = "vpc_id must be a valid VPC ID."
  }
}

variable "subnet_id" {
  description = "ID of the existing subnet used by all four instances."
  type        = string

  validation {
    condition     = can(regex("^subnet-[0-9a-f]+$", var.subnet_id))
    error_message = "subnet_id must be a valid subnet ID."
  }
}

variable "ami_id" {
  description = "Linux x86_64 AMI used by all four pilot instances."
  type        = string

  validation {
    condition     = can(regex("^ami-[0-9a-f]+$", var.ami_id))
    error_message = "ami_id must be a valid AMI ID."
  }
}

variable "key_name" {
  description = "Name of the existing EC2 key pair used for administration."
  type        = string

  validation {
    condition     = trimspace(var.key_name) != ""
    error_message = "key_name cannot be empty."
  }
}

variable "admin_cidr" {
  description = "Single IPv4 CIDR allowed to connect to SSH on the pilot instances."
  type        = string

  validation {
    condition = (
      can(cidrnetmask(var.admin_cidr)) &&
      trimspace(var.admin_cidr) != "0.0.0.0/0"
    )
    error_message = "admin_cidr must be a valid, restricted IPv4 CIDR and cannot be 0.0.0.0/0."
  }
}

variable "simulator_instance_type" {
  description = "EC2 instance type for the Simulator host."
  type        = string
  default     = "t3.small"
}

variable "edge_instance_type" {
  description = "EC2 instance type for the Edge host."
  type        = string
  default     = "t3.small"
}

variable "cloud_core_instance_type" {
  description = "EC2 instance type for the Cloud Core host."
  type        = string
  default     = "t3.small"
}

variable "worker_instance_type" {
  description = "EC2 instance type for the Worker host."
  type        = string
  default     = "t3.small"
}

variable "root_volume_size_gb" {
  description = "Size in GiB of each encrypted gp3 root volume."
  type        = number
  default     = 20

  validation {
    condition     = var.root_volume_size_gb >= 8
    error_message = "root_volume_size_gb must be at least 8 GiB."
  }
}
