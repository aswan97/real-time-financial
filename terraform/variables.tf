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
    default = "m7g.large"
}

variable "ami_id" {
    description = "AMI ID (Ubuntu 24.04 ARM)"
    type = string 
    default = "ami-0953e2223326856ce"
}

variable "key_pair_name" {
    description = "Name of existing EC2 eky pair for ssh access"
    type = string 
}

variable "s3_bucket_name" {
    description = "Name of the S3 bucket"
    type = string 
    default = "real-time-financial-features-938854116035-us-east-1-an"
}

variable "availability_zone" {
    description = "Availability Zone"
    type = string
    default = "us-east-1a"
}