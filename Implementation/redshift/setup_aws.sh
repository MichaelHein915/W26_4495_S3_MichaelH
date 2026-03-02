#!/usr/bin/env bash
#
# Provisions AWS resources for the Redshift Serverless crypto pipeline:
#   1. S3 staging bucket (with 7-day lifecycle cleanup)
#   2. IAM role for Redshift to read S3
#   3. Redshift Serverless namespace + workgroup
#
# Prerequisites:
#   - AWS CLI v2.13+ configured with appropriate credentials
#
# Usage:
#   chmod +x setup_aws.sh
#   ./setup_aws.sh
#
set -euo pipefail
export AWS_PAGER=""

# ---------------------------------------------------------------------------
# Configuration — edit these to match your environment
# ---------------------------------------------------------------------------
AWS_REGION="${AWS_REGION:-us-west-2}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

S3_BUCKET="${S3_BUCKET:-crypto-pipeline-staging-${ACCOUNT_ID}}"
S3_PREFIX="${S3_STAGING_PREFIX:-crypto-data/}"

NAMESPACE_NAME="crypto-pipeline"
WORKGROUP_NAME="crypto-pipeline"
REDSHIFT_DB="crypto"
REDSHIFT_ADMIN_USER="admin"
BASE_RPU=8  # Minimum RPU for Serverless (scales automatically)

IAM_ROLE_NAME="RedshiftS3ReadRole"
SECURITY_GROUP_NAME="redshift-crypto-pipeline-sg"

VPC_ID="${VPC_ID:-}"   # Leave empty to use default VPC

echo "=== Crypto Pipeline — AWS Setup (Redshift Serverless) ==="
echo "Region:    ${AWS_REGION}"
echo "Account:   ${ACCOUNT_ID}"
echo "Namespace: ${NAMESPACE_NAME}"
echo "Workgroup: ${WORKGROUP_NAME}"
echo ""

# ---------------------------------------------------------------------------
# 1. S3 Staging Bucket
# ---------------------------------------------------------------------------
echo "[1/4] Creating S3 bucket: ${S3_BUCKET} ..."

if aws s3api head-bucket --bucket "${S3_BUCKET}" 2>/dev/null; then
    echo "  Bucket already exists, skipping."
else
    if [ "${AWS_REGION}" = "us-east-1" ]; then
        aws s3api create-bucket --bucket "${S3_BUCKET}" --region "${AWS_REGION}"
    else
        aws s3api create-bucket \
            --bucket "${S3_BUCKET}" \
            --region "${AWS_REGION}" \
            --create-bucket-configuration LocationConstraint="${AWS_REGION}"
    fi
    echo "  Bucket created."
fi

echo "  Applying 7-day lifecycle policy for staging prefix ..."
aws s3api put-bucket-lifecycle-configuration \
    --bucket "${S3_BUCKET}" \
    --region "${AWS_REGION}" \
    --lifecycle-configuration '{
        "Rules": [
            {
                "ID": "CleanupStagingFiles",
                "Filter": { "Prefix": "'"${S3_PREFIX}"'" },
                "Status": "Enabled",
                "Expiration": { "Days": 7 }
            }
        ]
    }'
echo "  Lifecycle policy applied."
echo ""

# ---------------------------------------------------------------------------
# 2. IAM Role for Redshift → S3 access
# ---------------------------------------------------------------------------
echo "[2/4] Creating IAM role: ${IAM_ROLE_NAME} ..."

TRUST_POLICY='{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": { "Service": "redshift.amazonaws.com" },
            "Action": "sts:AssumeRole"
        }
    ]
}'

if aws iam get-role --role-name "${IAM_ROLE_NAME}" &>/dev/null; then
    echo "  Role already exists, skipping creation."
else
    aws iam create-role \
        --role-name "${IAM_ROLE_NAME}" \
        --assume-role-policy-document "${TRUST_POLICY}" \
        --description "Allows Redshift to read from S3 staging bucket"
    echo "  Role created."
fi

S3_POLICY='{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetBucketLocation",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::'"${S3_BUCKET}"'",
                "arn:aws:s3:::'"${S3_BUCKET}"'/*"
            ]
        }
    ]
}'

aws iam put-role-policy \
    --role-name "${IAM_ROLE_NAME}" \
    --policy-name "S3StagingReadAccess" \
    --policy-document "${S3_POLICY}"

IAM_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${IAM_ROLE_NAME}"
echo "  IAM Role ARN: ${IAM_ROLE_ARN}"
echo ""

# ---------------------------------------------------------------------------
# 3. Security Group
# ---------------------------------------------------------------------------
echo "[3/4] Setting up networking ..."

if [ -z "${VPC_ID}" ]; then
    VPC_ID=$(aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" \
        --query "Vpcs[0].VpcId" --output text --region "${AWS_REGION}")
    echo "  Using default VPC: ${VPC_ID}"
fi

SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=${SECURITY_GROUP_NAME}" "Name=vpc-id,Values=${VPC_ID}" \
    --query "SecurityGroups[0].GroupId" --output text --region "${AWS_REGION}" 2>/dev/null || echo "None")

if [ "${SG_ID}" = "None" ] || [ -z "${SG_ID}" ]; then
    SG_ID=$(aws ec2 create-security-group \
        --group-name "${SECURITY_GROUP_NAME}" \
        --description "Redshift Serverless - crypto pipeline" \
        --vpc-id "${VPC_ID}" \
        --region "${AWS_REGION}" \
        --query "GroupId" --output text)

    aws ec2 authorize-security-group-ingress \
        --group-id "${SG_ID}" \
        --protocol tcp \
        --port 5439 \
        --cidr "0.0.0.0/0" \
        --region "${AWS_REGION}"

    echo "  Security group created: ${SG_ID} (port 5439 open)"
else
    echo "  Security group already exists: ${SG_ID}"
fi

SUBNET_IDS=$(aws ec2 describe-subnets \
    --filters "Name=vpc-id,Values=${VPC_ID}" \
    --query "Subnets[*].SubnetId" --output text --region "${AWS_REGION}")
echo "  Subnets: ${SUBNET_IDS}"
echo ""

# ---------------------------------------------------------------------------
# 4. Redshift Serverless (namespace + workgroup)
# ---------------------------------------------------------------------------
echo "[4/4] Creating Redshift Serverless ..."

ADMIN_PASSWORD="$(openssl rand -base64 24 | tr -d '/+=' | head -c 20)Aa1!"

# ---- Namespace ----
NS_STATUS=$(aws redshift-serverless get-namespace \
    --namespace-name "${NAMESPACE_NAME}" \
    --region "${AWS_REGION}" \
    --query "namespace.status" --output text 2>/dev/null || echo "not-found")

if [ "${NS_STATUS}" != "not-found" ]; then
    echo "  Namespace '${NAMESPACE_NAME}' already exists (status: ${NS_STATUS})."
else
    aws redshift-serverless create-namespace \
        --namespace-name "${NAMESPACE_NAME}" \
        --db-name "${REDSHIFT_DB}" \
        --admin-username "${REDSHIFT_ADMIN_USER}" \
        --admin-user-password "${ADMIN_PASSWORD}" \
        --iam-roles "${IAM_ROLE_ARN}" \
        --region "${AWS_REGION}" \
        --query "namespace.namespaceName" --output text

    echo "  Namespace created."
    echo ""
    echo "  *** SAVE THIS PASSWORD: ${ADMIN_PASSWORD} ***"
    echo "  (It will not be shown again)"
fi

# ---- Workgroup ----
WG_STATUS=$(aws redshift-serverless get-workgroup \
    --workgroup-name "${WORKGROUP_NAME}" \
    --region "${AWS_REGION}" \
    --query "workgroup.status" --output text 2>/dev/null || echo "not-found")

if [ "${WG_STATUS}" != "not-found" ]; then
    echo "  Workgroup '${WORKGROUP_NAME}' already exists (status: ${WG_STATUS})."
else
    # Convert space-separated subnet IDs to JSON array
    SUBNET_JSON=$(echo "${SUBNET_IDS}" | tr '\t' '\n' | sed 's/^/"/;s/$/"/' | paste -sd',' - | sed 's/^/[/;s/$/]/')

    aws redshift-serverless create-workgroup \
        --workgroup-name "${WORKGROUP_NAME}" \
        --namespace-name "${NAMESPACE_NAME}" \
        --base-capacity "${BASE_RPU}" \
        --security-group-ids "${SG_ID}" \
        --subnet-ids "${SUBNET_JSON}" \
        --publicly-accessible \
        --region "${AWS_REGION}" \
        --query "workgroup.workgroupName" --output text

    echo "  Workgroup creation initiated."
fi

echo ""
echo "  Waiting for workgroup to become AVAILABLE (this may take 2-5 minutes) ..."

for i in $(seq 1 60); do
    STATUS=$(aws redshift-serverless get-workgroup \
        --workgroup-name "${WORKGROUP_NAME}" \
        --region "${AWS_REGION}" \
        --query "workgroup.status" --output text 2>/dev/null || echo "UNKNOWN")
    if [ "${STATUS}" = "AVAILABLE" ]; then
        break
    fi
    echo "    Status: ${STATUS} (attempt ${i}/60) ..."
    sleep 5
done

if [ "${STATUS}" != "AVAILABLE" ]; then
    echo "  ERROR: Workgroup did not become available. Check the AWS console."
    exit 1
fi
echo "  Workgroup is AVAILABLE."

ENDPOINT=$(aws redshift-serverless get-workgroup \
    --workgroup-name "${WORKGROUP_NAME}" \
    --region "${AWS_REGION}" \
    --query "workgroup.endpoint.address" --output text)

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Add these to your .env file:"
echo ""
echo "  REDSHIFT_HOST=${ENDPOINT}"
echo "  REDSHIFT_PORT=5439"
echo "  REDSHIFT_DB=${REDSHIFT_DB}"
echo "  REDSHIFT_USER=${REDSHIFT_ADMIN_USER}"
echo "  REDSHIFT_PASSWORD=<the password printed above>"
echo "  REDSHIFT_IAM_ROLE=${IAM_ROLE_ARN}"
echo "  S3_BUCKET=${S3_BUCKET}"
echo "  S3_STAGING_PREFIX=${S3_PREFIX}"
echo ""
echo "Then apply the schema and start the sink:"
echo "  python Implementation/redshift/apply_schema.py"
echo "  python Implementation/redshift/test_connection.py"
echo "  python Implementation/redshift/redshift_sink.py"
