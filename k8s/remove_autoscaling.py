import boto3

client = boto3.client('autoscaling')

# get all autoscaling groups
groups = client.describe_auto_scaling_groups()['AutoScalingGroups']

for group in groups:
      print("Deleting " + group['AutoScalingGroupName'] + "...")
      client.delete_auto_scaling_group(AutoScalingGroupName=group['AutoScalingGroupName'],
              ForceDelete=True)