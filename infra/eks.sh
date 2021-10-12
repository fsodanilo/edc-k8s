eksctl create cluster --name=edck8s --managed --spot --instance-types=m5.xlarge \
    --nodes=5 --alb-ingress-access --node-private-networking --region=us-east-2 \
    --nodes-min=3 --nodes-max=6 --full-ecr-access --asg-access --nodegroup-name=ng-edck8s