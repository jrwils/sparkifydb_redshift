import configparser
import json
import sys

import boto3


class AWSManagement(object):
    def __init__(self):
        self._load_credentials()
        self._load_configs()

        kwargs = {
            'aws_access_key_id': self.credentials.get('AWS', 'KEY'),
            'aws_secret_access_key': self.credentials.get('AWS', 'SECRET'),
            'region_name': self.credentials.get('AWS', 'REGION')
        }

        self.iam = boto3.client('iam', **kwargs)
        self.redshift = boto3.client('redshift', **kwargs)
        self.ec2 = boto3.resource('ec2', **kwargs)

    def _load_credentials(self):
        self.credentials = configparser.ConfigParser()
        self.credentials.read_file(open('aws_credentials.cfg'))

    def _load_configs(self):
        self.config = configparser.ConfigParser()
        self.config.read_file(open('dwh.cfg'))

    def create_iam_role(self):
        """
        Create an IAM role that, once a policy is attached,
        will allow Redshift read-only access to S3.
        """

        role_policy_doc = {
            'Statement': [
                {
                    'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {
                        'Service': 'redshift.amazonaws.com'
                    }
                }
            ],
            'Version': '2012-10-17'
        }
        response = self.iam.create_role(
            Path='/',
            RoleName=self.config.get('IAM_ROLE', 'ROLE_NAME'),
            Description="Allow Redshift clusters to call AWS services",
            AssumeRolePolicyDocument=json.dumps(role_policy_doc)
        )
        return response

    def attach_role_policy(self):
        """
        Attach a policy to the newly created role
        to allow read-only access to S3.
        """
        response = self.iam.attach_role_policy(
            RoleName=self.config.get('IAM_ROLE', 'ROLE_NAME'),
            PolicyArn=self.config.get('IAM_ROLE', 'S3_POLICY_ARN')
        )
        return response

    def remove_role_policy(self):
        response = self.iam.detach_role_policy(
            RoleName=self.config.get('IAM_ROLE', 'ROLE_NAME'),
            PolicyArn=self.config.get('IAM_ROLE', 'S3_POLICY_ARN')
        )
        return response

    def delete_iam_role(self):
        response = self.iam.delete_role(
            RoleName=self.config.get('IAM_ROLE', 'ROLE_NAME')
        )
        return response

    def create_redshift_cluster(self, role_arn):
        response = self.redshift.create_cluster(
            ClusterType=self.config.get('CLUSTER', 'CLUSTER_TYPE'),
            NodeType=self.config.get('CLUSTER', 'NODE_TYPE'),
            NumberOfNodes=int(self.config.get('CLUSTER', 'NUMBER_OF_NODES')),
            DBName=self.config.get('CLUSTER', 'DB_NAME'),
            ClusterIdentifier=self.config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
            MasterUsername=self.config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=self.config.get('CLUSTER', 'DB_PASSWORD'),
            IamRoles=[role_arn]
        )
        return response

    def delete_redshift_cluster(self):
        response = self.redshift.delete_cluster(
            ClusterIdentifier=self.config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
            SkipFinalClusterSnapshot=True
        )
        return response

    @property
    def _default_security_group(self):
        c_info = self.cluster_information()
        vpc_id = c_info['Clusters'][0]['VpcId']
        vpc = self.ec2.Vpc(id=vpc_id)
        default_sg = list(vpc.security_groups.all())[0]
        return default_sg

    def open_tcp_port(self):
        response = self._default_security_group.authorize_ingress(
            GroupName=self._default_security_group.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(self.config.get('CLUSTER', 'DB_PORT')),
            ToPort=int(self.config.get('CLUSTER', 'DB_PORT')),
        )
        return response

    def close_tcp_port(self):
        response = self._default_security_group.revoke_ingress(
            GroupName=self._default_security_group.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(self.config.get('CLUSTER', 'DB_PORT')),
            ToPort=int(self.config.get('CLUSTER', 'DB_PORT'))
        )
        return response

    def cluster_information(self):
        cluster = self.redshift.describe_clusters(
            ClusterIdentifier=self.config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
        )
        return cluster

    def role_info(self):
        role_data = self.iam.get_role(
            RoleName=self.config.get('IAM_ROLE', 'ROLE_NAME')
        )
        return role_data


class AWSManage(object):
    def __init__(self):
        self.manage_aws = AWSManagement()

    def setup(self):
        print('Creating IAM Role')
        role_response = self.manage_aws.create_iam_role()
        if role_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print('Successfully Created Role')

        print('Attaching IAM Role Policy for S3 Access')
        policy_response = self.manage_aws.attach_role_policy()
        if policy_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print('Successfully Attached Role Policy')

        print('Creating cluster')
        redshift_response = self.manage_aws.create_redshift_cluster(
            role_response['Role']['Arn']
        )
        if redshift_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print('Cluster Creation Successfully Accepted.')
            print(
                "Run 'python3 aws_redshift.py status' for status information."
            )

    def teardown(self):
        print('Closing TCP Port')
        port_response = self.manage_aws.close_tcp_port()
        if port_response['Return'] is True:
            print('Successfully Closed Port')

        print('Requesting Cluster Deletion')
        cluster_response = self.manage_aws.delete_redshift_cluster()
        if cluster_response['Cluster']['ClusterStatus'] == 'deleting':
            print('Request to delete cluster accepted')

        print('Removing Policy from Role')
        policy_response = self.manage_aws.remove_role_policy()
        if policy_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print('Successfully Removed Role Policy')

        print('Deleting Role')
        role_response = self.manage_aws.delete_iam_role()
        if role_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print('Successfully Deleted Role')

    def cluster_status(self):
        c_info = self.manage_aws.cluster_information()
        status = c_info['Clusters'][0]['ClusterStatus']
        host = ''
        if status == 'available':
            host = c_info['Clusters'][0]['Endpoint']['Address']
        print('Status: {}'.format(status))
        print('Host: {}'.format(host))

    def open_tcp_port(self):
        print('Opening TCP Port')
        try:
            port_response = self.manage_aws.open_tcp_port()
            if port_response['Return'] is True:
                print('Successfully Opened Port')
        except Exception as e:
            print(e)

    def show_role_arn(self):
        role_data = self.manage_aws.role_info()
        print('Role: {}'.format(role_data['Role']['RoleName']))
        print('ARN: {}'.format(role_data['Role']['Arn']))


if __name__ == '__main__':
    if len(sys.argv) > 1:
        argument = sys.argv[1]
    else:
        argument = ''

    management = AWSManage()

    if argument == 'setup':
        management.setup()
    elif argument == 'teardown':
        management.teardown()
    elif argument == 'status':
        management.cluster_status()
    elif argument == 'openport':
        management.open_tcp_port()
    elif argument == 'roleinfo':
        management.show_role_arn()
    else:
        print(
            """
            Invalid or missing argument.
            Please pass one of the following valid arguments like so:

            python3 aws_redshift.py setup
            python3 aws_redshift.py teardown
            python3 aws_redshift.py openport
            python3 aws_redshift.py status
            python3 aws_redshift.py roleinfo
            """
        )
