#!/bin/bash
# AWS infrastructure setup script for batch processing
# This script creates all necessary AWS resources

set -e

# Configuration
REGION="${AWS_REGION:-ap-northeast-2}"
BUCKET_NAME="${S3_BUCKET:-ocr-batch-processing}"
ECR_REPO_NAME="${ECR_REPO:-batch-processor}"
SECRET_NAME="${SECRET_NAME:-upstage-api-key}"
BATCH_COMPUTE_ENV="${BATCH_COMPUTE_ENV:-batch-processor-compute-env}"
BATCH_JOB_QUEUE="${BATCH_JOB_QUEUE:-batch-processor-queue}"
BATCH_JOB_DEFINITION="${BATCH_JOB_DEFINITION:-pseudo-label-processor}"

echo "========================================="
echo "AWS Batch Infrastructure Setup"
echo "========================================="
echo "Region: $REGION"
echo "S3 Bucket: $BUCKET_NAME"
echo "ECR Repository: $ECR_REPO_NAME"
echo "========================================="

# Check if AWS CLI is configured
if ! aws sts get-caller-identity &>/dev/null; then
    echo "❌ AWS CLI not configured. Run 'aws configure' first."
    exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "✓ AWS Account: $ACCOUNT_ID"

# 1. Create S3 bucket
echo ""
echo "1️⃣  Creating S3 bucket..."
if aws s3 ls "s3://$BUCKET_NAME" 2>/dev/null; then
    echo "   ℹ️  Bucket already exists: $BUCKET_NAME"
else
    aws s3 mb "s3://$BUCKET_NAME" --region "$REGION"

    # Enable versioning
    aws s3api put-bucket-versioning \
        --bucket "$BUCKET_NAME" \
        --versioning-configuration Status=Enabled

    echo "   ✓ Created bucket: s3://$BUCKET_NAME"
fi

# Create folder structure
aws s3api put-object --bucket "$BUCKET_NAME" --key "data/processed/" --content-length 0
aws s3api put-object --bucket "$BUCKET_NAME" --key "checkpoints/" --content-length 0
echo "   ✓ Created folder structure"

# 2. Create ECR repository
echo ""
echo "2️⃣  Creating ECR repository..."
if aws ecr describe-repositories --repository-names "$ECR_REPO_NAME" --region "$REGION" &>/dev/null; then
    echo "   ℹ️  Repository already exists: $ECR_REPO_NAME"
else
    aws ecr create-repository \
        --repository-name "$ECR_REPO_NAME" \
        --region "$REGION" \
        --image-scanning-configuration scanOnPush=true
    echo "   ✓ Created repository: $ECR_REPO_NAME"
fi

ECR_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO_NAME}"
echo "   📦 ECR URI: $ECR_URI"

# 3. Store API key in Secrets Manager
echo ""
echo "3️⃣  Storing API key in Secrets Manager..."
if [ -n "$UPSTAGE_API_KEY" ]; then
    if aws secretsmanager describe-secret --secret-id "$SECRET_NAME" --region "$REGION" &>/dev/null; then
        aws secretsmanager update-secret \
            --secret-id "$SECRET_NAME" \
            --secret-string "$UPSTAGE_API_KEY" \
            --region "$REGION"
        echo "   ✓ Updated secret: $SECRET_NAME"
    else
        aws secretsmanager create-secret \
            --name "$SECRET_NAME" \
            --secret-string "$UPSTAGE_API_KEY" \
            --region "$REGION"
        echo "   ✓ Created secret: $SECRET_NAME"
    fi
else
    echo "   ⚠️  UPSTAGE_API_KEY not set. Skipping secret creation."
    echo "   💡 Set it later with: aws secretsmanager create-secret --name $SECRET_NAME --secret-string YOUR_KEY"
fi

# 4. Create IAM roles
echo ""
echo "4️⃣  Creating IAM roles..."

# Batch Job Role (used by containers)
JOB_ROLE_NAME="BatchProcessorJobRole"
TRUST_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
)

if aws iam get-role --role-name "$JOB_ROLE_NAME" &>/dev/null; then
    echo "   ℹ️  Job role already exists: $JOB_ROLE_NAME"
else
    aws iam create-role \
        --role-name "$JOB_ROLE_NAME" \
        --assume-role-policy-document "$TRUST_POLICY"

    # Attach policies
    aws iam attach-role-policy \
        --role-name "$JOB_ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/AmazonS3FullAccess"

    aws iam attach-role-policy \
        --role-name "$JOB_ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/SecretsManagerReadWrite"

    echo "   ✓ Created job role: $JOB_ROLE_NAME"
fi

JOB_ROLE_ARN=$(aws iam get-role --role-name "$JOB_ROLE_NAME" --query 'Role.Arn' --output text)

# Batch Execution Role
EXEC_ROLE_NAME="BatchProcessorExecutionRole"
if aws iam get-role --role-name "$EXEC_ROLE_NAME" &>/dev/null; then
    echo "   ℹ️  Execution role already exists: $EXEC_ROLE_NAME"
else
    aws iam create-role \
        --role-name "$EXEC_ROLE_NAME" \
        --assume-role-policy-document "$TRUST_POLICY"

    aws iam attach-role-policy \
        --role-name "$EXEC_ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"

    echo "   ✓ Created execution role: $EXEC_ROLE_NAME"
fi

EXEC_ROLE_ARN=$(aws iam get-role --role-name "$EXEC_ROLE_NAME" --query 'Role.Arn' --output text)

# Batch Service Role
SERVICE_ROLE_NAME="AWSBatchServiceRole"
if aws iam get-role --role-name "$SERVICE_ROLE_NAME" &>/dev/null; then
    echo "   ℹ️  Service role already exists: $SERVICE_ROLE_NAME"
else
    SERVICE_TRUST_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "batch.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
)
    aws iam create-role \
        --role-name "$SERVICE_ROLE_NAME" \
        --assume-role-policy-document "$SERVICE_TRUST_POLICY"

    aws iam attach-role-policy \
        --role-name "$SERVICE_ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"

    echo "   ✓ Created service role: $SERVICE_ROLE_NAME"
fi

SERVICE_ROLE_ARN=$(aws iam get-role --role-name "$SERVICE_ROLE_NAME" --query 'Role.Arn' --output text)

# Wait for IAM propagation
echo "   ⏳ Waiting for IAM roles to propagate (15s)..."
sleep 15

# 5. Create AWS Batch Compute Environment
echo ""
echo "5️⃣  Creating AWS Batch compute environment..."

# Get default VPC and subnets (or use first available VPC, or create one)
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=is-default,Values=true" --query 'Vpcs[0].VpcId' --output text --region "$REGION" 2>/dev/null)

# If no default VPC, get the first available VPC
if [ -z "$VPC_ID" ] || [ "$VPC_ID" == "None" ]; then
    echo "   ℹ️  No default VPC found, checking for existing VPCs..."
    VPC_ID=$(aws ec2 describe-vpcs --query 'Vpcs[0].VpcId' --output text --region "$REGION" 2>/dev/null)
fi

# If still no VPC, create one
if [ -z "$VPC_ID" ] || [ "$VPC_ID" == "None" ]; then
    echo "   ℹ️  No VPC found, creating default VPC..."
    VPC_OUTPUT=$(aws ec2 create-default-vpc --region "$REGION" 2>&1)
    if [ $? -eq 0 ]; then
        VPC_ID=$(echo "$VPC_OUTPUT" | grep -oP '"VpcId":\s*"\K[^"]+' || echo "$VPC_OUTPUT" | jq -r '.Vpc.VpcId' 2>/dev/null || echo "")
        if [ -z "$VPC_ID" ]; then
            # Try to get it from describe after creation
            sleep 2
            VPC_ID=$(aws ec2 describe-vpcs --filters "Name=is-default,Values=true" --query 'Vpcs[0].VpcId' --output text --region "$REGION" 2>/dev/null)
        fi
        echo "   ✓ Created default VPC: $VPC_ID"
    else
        echo "   ❌ Failed to create VPC. Error: $VPC_OUTPUT"
        echo "   💡 Please create a VPC manually in the AWS Console and rerun this script."
        exit 1
    fi
fi

if [ -z "$VPC_ID" ] || [ "$VPC_ID" == "None" ]; then
    echo "   ❌ No VPC available in region $REGION"
    exit 1
fi

echo "   ✓ Using VPC: $VPC_ID"

# Get subnets (wait a bit if VPC was just created)
SUBNETS=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" --query 'Subnets[*].SubnetId' --output text --region "$REGION" 2>/dev/null | tr '\t' ',' | sed 's/,$//')

# If no subnets found, wait a bit and try again (VPC creation takes time)
if [ -z "$SUBNETS" ]; then
    echo "   ⏳ Waiting for subnets to be available (10s)..."
    sleep 10
    SUBNETS=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" --query 'Subnets[*].SubnetId' --output text --region "$REGION" 2>/dev/null | tr '\t' ',' | sed 's/,$//')
fi

if [ -z "$SUBNETS" ]; then
    echo "   ❌ No subnets found in VPC $VPC_ID"
    echo "   💡 Subnets may take a few minutes to be created. Please wait and rerun this script."
    exit 1
fi

echo "   ✓ Found subnets: $SUBNETS"

# Get security group (try default first, then any security group in the VPC)
SECURITY_GROUP=$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=default" --query 'SecurityGroups[0].GroupId' --output text --region "$REGION" 2>/dev/null)

if [ -z "$SECURITY_GROUP" ] || [ "$SECURITY_GROUP" == "None" ]; then
    echo "   ℹ️  No default security group found, using first available security group..."
    SECURITY_GROUP=$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$VPC_ID" --query 'SecurityGroups[0].GroupId' --output text --region "$REGION" 2>/dev/null)
fi

if [ -z "$SECURITY_GROUP" ] || [ "$SECURITY_GROUP" == "None" ]; then
    echo "   ❌ No security group found in VPC $VPC_ID"
    exit 1
fi

echo "   ✓ Using security group: $SECURITY_GROUP"

if aws batch describe-compute-environments --compute-environments "$BATCH_COMPUTE_ENV" --region "$REGION" &>/dev/null; then
    echo "   ℹ️  Compute environment already exists: $BATCH_COMPUTE_ENV"
else
    # Create Fargate Spot compute environment (cheapest option)
    aws batch create-compute-environment \
        --compute-environment-name "$BATCH_COMPUTE_ENV" \
        --type MANAGED \
        --state ENABLED \
        --service-role "$SERVICE_ROLE_ARN" \
        --compute-resources "{
            \"type\": \"FARGATE_SPOT\",
            \"maxvCpus\": 16,
            \"subnets\": [$(echo $SUBNETS | sed 's/,/","/g' | sed 's/^/"/' | sed 's/$/"/') ],
            \"securityGroupIds\": [\"$SECURITY_GROUP\"]
        }" \
        --region "$REGION"

    echo "   ✓ Created Fargate Spot compute environment: $BATCH_COMPUTE_ENV"
fi

# 6. Create AWS Batch Job Queue
echo ""
echo "6️⃣  Creating AWS Batch job queue..."

if aws batch describe-job-queues --job-queues "$BATCH_JOB_QUEUE" --region "$REGION" &>/dev/null; then
    echo "   ℹ️  Job queue already exists: $BATCH_JOB_QUEUE"
else
    aws batch create-job-queue \
        --job-queue-name "$BATCH_JOB_QUEUE" \
        --state ENABLED \
        --priority 1 \
        --compute-environment-order "[{\"order\": 1, \"computeEnvironment\": \"$BATCH_COMPUTE_ENV\"}]" \
        --region "$REGION"

    echo "   ✓ Created job queue: $BATCH_JOB_QUEUE"
fi

# 7. Output configuration
echo ""
echo "========================================="
echo "✅ Setup Complete!"
echo "========================================="
echo ""
echo "📋 Configuration Summary:"
echo "  S3 Bucket:         s3://$BUCKET_NAME"
echo "  ECR Repository:    $ECR_URI"
echo "  Secret ARN:        arn:aws:secretsmanager:${REGION}:${ACCOUNT_ID}:secret:${SECRET_NAME}"
echo "  Job Role ARN:      $JOB_ROLE_ARN"
echo "  Execution Role:    $EXEC_ROLE_ARN"
echo "  Compute Env:       $BATCH_COMPUTE_ENV"
echo "  Job Queue:         $BATCH_JOB_QUEUE"
echo ""
echo "🔧 Next Steps:"
echo "  1. Upload dataset to S3:"
echo "     aws s3 cp data/processed/baseline_train.parquet s3://$BUCKET_NAME/data/processed/"
echo ""
echo "  2. Build and push Docker image:"
echo "     cd scripts/cloud"
echo "     docker build -f Dockerfile.batch -t $ECR_REPO_NAME ../.."
echo "     aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR_URI"
echo "     docker tag $ECR_REPO_NAME:latest $ECR_URI:latest"
echo "     docker push $ECR_URI:latest"
echo ""
echo "  3. Register job definition (see aws/batch-job-definition.json)"
echo ""
echo "  4. Configure GitHub secrets:"
echo "     - AWS_ACCESS_KEY_ID"
echo "     - AWS_SECRET_ACCESS_KEY"
echo "     - AWS_REGION=$REGION"
echo "     - ECR_REPOSITORY_URI=$ECR_URI"
echo "     - S3_BUCKET=$BUCKET_NAME"
echo ""
echo "========================================="

# Save configuration to file
mkdir -p aws
cat > aws/config.env <<EOF
# AWS Batch Configuration
# Generated: $(date)

AWS_REGION=$REGION
AWS_ACCOUNT_ID=$ACCOUNT_ID
S3_BUCKET=$BUCKET_NAME
ECR_REPOSITORY=$ECR_REPO_NAME
ECR_URI=$ECR_URI
SECRET_NAME=$SECRET_NAME
JOB_ROLE_ARN=$JOB_ROLE_ARN
EXEC_ROLE_ARN=$EXEC_ROLE_ARN
BATCH_COMPUTE_ENV=$BATCH_COMPUTE_ENV
BATCH_JOB_QUEUE=$BATCH_JOB_QUEUE
BATCH_JOB_DEFINITION=$BATCH_JOB_DEFINITION
EOF

echo "💾 Configuration saved to: aws/config.env"
