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
    parser.add_argument('--dataset-id', dest='dataset_id', required=True)
    parser.add_argument('--refresh-id', dest='refresh_id', required=False)
    args = parser.parse_args()
    return args


def get_sync_status(dataset_id, refresh_id, access_token):
    """
    Executes/starts a PowerBI Asynchronous dataset refresh
    see: https://docs.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh
    """
    url_base = "https://api.powerbi.com/v1.0/myorg"
    status_endpoint = f"/datasets/{dataset_id}/refreshes/{refresh_id}"
    status_api = url_base + status_endpoint
    api_headers = {
        'authorization': f"Bearer {access_token}",
        'Content-Type': 'application/json'
    }
    try:
        status_response = requests.get(status_api, headers=api_headers)
        if status_response.status_code == requests.codes.ok:
            return status_response.json()
        else:
            print(f"Unknown error for dataset {dataset_id} refresh {refresh_id}")
    except Exception as e:
        print(f"Get Refresh status error occurred due to: {e}")
    


def handle_refresh_data(refresh_data):
    """
    Analyses sync run data to determine status and print sync run information
    
    Returns:
        status_code: Exit Status code detailing sync status
    """
    status = refresh_data['status']
    refresh_id = refresh_data['requestId']
    if status == "Completed":
        print(
            f"Refresh {refresh_id} completed successfully. ",
            f"Completed at: {refresh_data['endTime']}"
        )
        status_code = errors.EXIT_CODE_FINAL_STATUS_SUCCESS
        
    elif status == "Failed":
        error_code = refresh_data['error_code']
        error_message = refresh_data['error_message']
        print(f"Refresh {refresh_id} failed. {error_code} {error_message}")
        status_code = errors.EXIT_CODE_FINAL_STATUS_FAILED

    elif status == "Unknown":
        if refresh_data['extendedStatus'] == 'InProgress':
            print(f"Refresh {refresh_id} still in progress...")
            status_code = errors.EXIT_CODE_FINAL_STATUS_INCOMPLETE
        elif refresh_data['extendedStatus'] == 'NotStarted':
            print(f"{refresh_id} refresh procedure has not yet started")
            status_code = errors.EXIT_CODE_FINAL_STATUS_NOT_STARTED

    elif status == "Disabled":
        print("Refresh {refresh_id} operation is disabled")
        status_code = errors.EXIT_CODE_FINAL_STATUS_DISABLED

    elif status == "Cancelled":
        print("Refresh {refresh_id} operation was cancelled")
        status_code = errors.EXIT_CODE_FINAL_STATUS_CANCELLED
        
    else:
        print("An unknown error has occured with {refresh_id}")
        print("Unknown Sync status: {status}")
        status_code = errors.EXIT_CODE_UNKNOWN_ERROR
    
    return status_code


def main():
    args = get_args()
    tenant_id = args.tenant_id
    client_id = args.client_id
    client_secret = args.client_secret
    dataset_id = args.dataset_id
    
    # get access token
    access_token = helpers.get_access_token(tenant_id, 
                                            client_id, 
                                            client_secret)
    # fetch refresh data
    artifacts_subfolders = helpers.artifact_subfolder_paths
    if args.refresh_id:
        refresh_id = args.refresh_id
    else:
        refresh_id = shipyard.logs.read_pickle_file(
            artifacts_subfolders, 'refresh_id')
    
    # run check sync status
    refresh_data = get_sync_status(dataset_id, refresh_id, access_token)
    exit_code_status = handle_refresh_data(refresh_data)
    sys.exit(exit_code_status)


if __name__ == "__main__":
    main()




