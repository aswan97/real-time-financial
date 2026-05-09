# main.tf 

# Data Sources 

data "aws_vpc" "default" {
  default = true
}

# AWS Subnet to use az
data "aws_subnet" "public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "availabilityZone"
    values = [var.availability_zone]
  }
}

# Security Group 

resource "aws_security_group" "app" {
  name        = "${var.environment}-app-sg"
  description = "Security group for app EC2 instance"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # Restrict to your IP in production
  }

  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.environment}-app-sg"
    Environment = var.environment
  }
}

# IAM Role (SSM Session Manager access) 

resource "aws_iam_role" "ec2_role" {
  name = "${var.environment}-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "${var.environment}-ec2-profile"
  role = aws_iam_role.ec2_role.name
}

# EC2 Instance 

resource "aws_instance" "app" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  subnet_id              = data.aws_subnet.public.id
  vpc_security_group_ids = [aws_security_group.app.id]
  key_name               = var.key_pair_name
  iam_instance_profile   = aws_iam_instance_profile.ec2_profile.name

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 8
    delete_on_termination = true
    encrypted             = true
  }

  # Logic to deploy docker compose 
  user_data = <<-EOF
  #!/bin/bash
  apt update -y
  apt install -y curl git
  
  # Install Docker
  curl -fsSL https://get.docker.com | sh

  systemctl start docker
  systemctl enable docker
  usermod -aG docker ubuntu

  git clone https://github.com/aswan97/real-time-financial.git /app
  cd /app

  cat > .env <<ENV
  S3_BUCKET_NAME=${data.aws_s3_bucket.uploads.bucket}
  AWS_REGION=${var.aws_region}
  ENV

EOF

  tags = {
    Name        = "${var.environment}-app"
    Environment = var.environment
  }
}

# Elastic IP 

resource "aws_eip" "app" {
  instance = aws_instance.app.id
  domain   = "vpc"

  tags = {
    Name        = "${var.environment}-app-eip"
    Environment = var.environment
  }
}

# S3 bucket
data "aws_s3_bucket" "uploads" {
  bucket = "${var.s3_bucket_name}"
}

resource "aws_s3_bucket_versioning" "uploads" {
  bucket = data.aws_s3_bucket.uploads.id 
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "uploads" {
  bucket = data.aws_s3_bucket.uploads.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Private S3 bucket 
resource "aws_s3_bucket_public_access_block" "uploads" {
  bucket                  = data.aws_s3_bucket.uploads.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM policy for the EC2 to upload files to S3
resource "aws_iam_role_policy" "s3_upload" {
  name = "${var.environment}-s3-upload"
  role = aws_iam_role.ec2_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        data.aws_s3_bucket.uploads.arn,
        "${data.aws_s3_bucket.uploads.arn}/*"
      ]
    }]
  })
}