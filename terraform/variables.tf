# variables.tf. 

variable "aws_region" {
    description = "AWS Region"
    type = string 
    default = "us-east-1"
}

variable "environment" {
    description = "Environment Name"
    type = string 
    default = "dev"
}

variable "project" {
    description = "Project Name"
    type = string 
    default = "real-time-financial"
}

variable "instance_type" {
    description = "EC2 Instance Type"
    type = string 
    default = "t2.micro"
}

variable "ami_id" {
    description = "AMI ID (Ubuntu 24.04)"
    type = string 
    default = "ami-05cf1e9f73fbad2e2"
}

variable "key_pair_name" {
    description = "Name of existing EC2 eky pair for ssh access"
    type = string 
}