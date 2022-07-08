"""Power BI - Verify Datasource Refresh Status
User Inputs:

Authentication
Refresh ID
Requirements:

Verifies the specified dataset refresh was successfully refreshed.
Should return separate statuses for things like "Unfinished, Success, Canceled, Errored, etc."
Should use pickle variable from the "Trigger Dataset Refresh" if no refresh ID is provided.
"""
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
    


def handle_sync_run_data(sync_run_data):
    """
    Analyses sync run data to determine status and print sync run information
    
    Returns:
        status_code: Exit Status code detailing sync status
    """
    status = sync_run_data['status']
    sync_id = sync_run_data['sync_id']
    if status == "completed":
        print(
            f"Sync {sync_id} completed successfully. ",
            f"Completed at: {sync_run_data['completed_at']}"
        )
        status_code = errors.EXIT_CODE_STATUS_COMPLETED
        
    elif status == "working":
        print(
            f"Sync {sync_id} still Running. ",
            f"Current records processed: {sync_run_data['records_processed']}"
        )
        status_code = errors.EXIT_CODE_STATUS_RUNNING
    
    elif status == "failed":
        error_code = sync_run_data['error_code']
        error_message = sync_run_data['error_message']
        print(f"Sync {sync_id} failed. {error_code} {error_message}")
        status_code = errors.EXIT_CODE_STATUS_FAILED
        
    else:
        print("An unknown error has occured with {sync_id}")
        print("Unknown Sync status: {status}")
        status_code = errors.EXIT_CODE_UNKNOWN_ERROR
    
    return status_code


def main():
    args = get_args()
    access_token = args.access_token  
    # create artifacts folder to save run id
    base_folder_name = shipyard.logs.determine_base_artifact_folder(
        'census')
    artifact_subfolder_paths = shipyard.logs.determine_artifact_subfolders(
        base_folder_name)
    shipyard.logs.create_artifacts_folders(artifact_subfolder_paths)
    
    # get sync run id variable from user or pickle file if not inputted
    if args.sync_run_id:
        sync_run_id = args.sync_run_id
    else:
        sync_run_id = shipyard.logs.read_pickle_file(
            artifact_subfolder_paths, 'sync_run_id')
    # run check sync status
    sync_run_data = get_sync_status(sync_run_id, access_token)
    exit_code_status = handle_sync_run_data(sync_run_data)
    sys.exit(exit_code_status)


if __name__ == "__main__":
    main()




