"""Power BI - Trigger Datasource Refresh
User Inputs:
Authentication
Datasource

Requirements:
Immediately triggers the dataset to be refreshed.
Uses asyncronous refresh so we can check the status afterwards.
Provides refresh job id as a pickle file

Limitations:
Should only refresh one datasource
"""
import sys
from azure.identity import ClientSecretCredential
from azure.core.exceptions import ClientAuthenticationError
import shipyard_utils as shipyard
try:
    import errors
except BaseException:
    from . import errors


# create Artifacts folders
base_folder_name = shipyard.logs.determine_base_artifact_folder(
    'powerbi')
artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
    base_folder_name)
shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)


def get_access_token(tenant, client, client_secret):
    """Generates a PowerBI access token for use in PowerBI REST API requests.
    For details on where to find tenant, client and client_secret keys see:
    https://docs.microsoft.com/en-us/power-bi/developer/embedded/embed-service-principal
    """
    api = 'https://analysis.windows.net/powerbi/api/.default'
    auth = ClientSecretCredential(authority='https://login.microsoftonline.com/',
                                                        tenant_id = tenant,
                                                        client_id = client,
                                                        client_secret = client_secret)
    try:
        access_token = auth.get_token(api)
        access_token = access_token.token
        return access_token
    except (ValueError, ClientAuthenticationError):
        print('Failed to Authenticate:please check your credentials')
        sys.exit(errors.EXIT_CODE_INVALID_CREDENTIALS)