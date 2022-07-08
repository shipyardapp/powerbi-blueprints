import argparse
import sys
import requests
import shipyard_utils as shipyard
try:
    import helpers
    import errors
except BaseException:
    from . import helpers
    from . import errors


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant-id', dest='tenant_id', required=True)
    parser.add_argument('--client-id', dest='client_id', required=True)
    parser.add_argument('--client-secret', dest='client_secret', required=True)
    parser.add_argument('--group-id', dest='group_id', required=True)
    parser.add_argument('--dataset-id', dest='dataset_id', required=False)
    args = parser.parse_args()
    return args


def execute_refresh(group_id, dataset_id, access_token):
    """
    Executes/starts a PowerBI Asynchronous dataset refresh
    see: https://docs.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh
    """
    url_base = "https://api.powerbi.com/v1.0/myorg"
    refresh_endpoint = f"/groups/{group_id}/datasets/{dataset_id}/refreshes"
    refresh_api = url_base + refresh_endpoint
    api_headers = {
        'authorization': f"Bearer {access_token}",
        'Content-Type': 'application/json'
    }
    
    payload = {
    "type": "Full"
    }

    try:
        refresh_response = requests.post(refresh_api,
                                              json=payload,
                                              headers=api_headers)
        
        refresh_status_code = refresh_response.status_code
        # check if successful, if not return error message
        if refresh_status_code == requests.codes.ok:
            print(f"Refresh for {dataset_id} successful")
            # return the refresh_id from the application
            refresh_id = refresh_response.headers['x-ms-request-id']
            return refresh_id
        
        elif refresh_status_code == 400: # Bad request
            print("Refresh for {dataset_id} failed due to Bad Request Error.")
            sys.exit(errors.EXIT_CODE_BAD_REQUEST)

        elif refresh_status_code == 404: # invalid Dataset
            print("Refresh Failed.",
                  f"Check if dataset {dataset_id} and Group: {group_id} are correct")
            sys.exit(errors.EXIT_CODE_INVALID_DATASET_ID)

        elif refresh_status_code == 409: # Refresh Already running
            print(f"Refresh operation already running for dataset: {dataset_id}")
            sys.exit(errors.EXIT_CODE_REFRESH_ALREADY_RUNNING)
        
        else:
            print("Unknown error code: {refresh_status_code} when sending request")
            sys.exit(errors.EXIT_CODE_REFRESH_ERROR)
        
    except Exception as e:
        print(f"Refresh operation failed due to: {e}")
        sys.exit(errors.EXIT_CODE_UNKNOWN_ERROR)


def main():
    args = get_args()
    tenant_id = args.tenant_id
    client_id = args.client_id
    client_secret = args.client_secret
    group_id = args.group_id
    dataset_id = args.dataset_id
    
    # get access token
    access_token = helpers.get_access_token(tenant_id, 
                                            client_id, 
                                            client_secret)
    # execute refresh
    trigger_refresh = execute_refresh(group_id, dataset_id, access_token)
    refresh_id = trigger_refresh
    
    artifacts_subfolder = helpers.artifact_subfolder_paths
    
    # save refresh id as variable
    shipyard.logs.create_pickle_file(artifacts_subfolder, 
                                'refresh_id', refresh_id)

if __name__ == "__main__":
    main()
