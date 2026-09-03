data "aws_vpc" "pilot" {
  id = var.vpc_id
}

data "aws_subnet" "pilot" {
  id = var.subnet_id
}
