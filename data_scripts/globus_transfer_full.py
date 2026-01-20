import io
import os
import argparse
import urllib.request
import time
from pathlib import Path

from astropy.io import fits

import numpy as np
from astropy.table import Table, unique
import globus_sdk
from globus_sdk.scopes import TransferScopes
import time


DESI_TILEPIX_URL = (
    "https://data.desi.lbl.gov/public/dr1/spectro/redux/iron/healpix/tilepix.fits"
)
DESI_GLOBUS_ENDPOINT = "6b4e1f6a-e600-11ed-9b9b-c9bb788c490e"

# Tutorial client ID - replace with your own for production
CLIENT_ID = "61338d24-54d5-408f-a10d-66c06b59f6d2"


# Globus Authentication Helper Function
def login_and_get_transfer_client(native_client, *, scopes=TransferScopes.all):
    """
    Performs the Globus Native App auth flow and returns a TransferClient.
    Handles prompting the user for the auth code.
    Can be called with specific scopes if needed (e.g., for consent).
    """
    native_client.oauth2_start_flow(requested_scopes=scopes)
    authorize_url = native_client.oauth2_get_authorize_url()
    print(f"Please go to this URL and login: {authorize_url}")
    auth_code = input("Please enter the code you get after login here: ").strip()
    token_response = native_client.oauth2_exchange_code_for_tokens(auth_code)
    globus_transfer_data = token_response.by_resource_server["transfer.api.globus.org"]

    # Use the access token to create a Globus transfer client
    authorizer = globus_sdk.AccessTokenAuthorizer(globus_transfer_data["access_token"])
    transfer_client = globus_sdk.TransferClient(authorizer=authorizer)
    return transfer_client


# Submit a transfer with error handling and consent management
def submit_transfer_with_consent_handling(transfer_client, transfer_data):
    """Helper function to submit the transfer with consent handling."""
    try:
        print("Submitting transfer request...")
        transfer_result = transfer_client.submit_transfer(transfer_data)
        transfer_id = transfer_result["task_id"]
        print(f"Transfer submitted successfully!")
        print(
            "Monitor the transfer task here: "
            f"https://app.globus.org/activity/{transfer_id}"
        )
        return transfer_id
    except globus_sdk.TransferAPIError as err:
        # Check if it's a ConsentRequired error
        if hasattr(err.info, "consent_required") and err.info.consent_required:
            print("\n=== Consent Required ===")
            print(
                "The Globus transfer requires additional consent "
                "for the endpoints involved."
            )
            print("You need to log in again to grant the necessary permissions.")

            # Get required scopes from the error object
            required_scopes = err.info.consent_required.required_scopes

            # Re-authenticate with the required scopes
            native_client = globus_sdk.NativeAppAuthClient(CLIENT_ID)
            transfer_client = login_and_get_transfer_client(
                native_client, scopes=required_scopes
            )

            # Update the transfer_data object with the new client
            transfer_data.transfer_client = transfer_client

            # Retry the submission
            print("\nRetrying transfer submission with new consent...")
            transfer_result = transfer_client.submit_transfer(transfer_data)
            transfer_id = transfer_result["task_id"]
            print(f"Transfer submitted successfully!")
            print(
                "Monitor the transfer task here: "
                f"https://app.globus.org/activity/{transfer_id}"
            )
            return transfer_id
        else:
            # If it's a different Globus API error, re-raise it
            print(f"\nAn unexpected Globus Transfer API error occurred: {err}")
            raise err


# Main Script Logic
def read_healpix_ids(txt_path: str) -> np.ndarray:
    hp = []
    with open(txt_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "#" in line:
                line = line.split("#", 1)[0].strip()
            hp.append(int(line))
    if not hp:
        raise ValueError(f"No HEALPIX IDs found in file: {txt_path}")
    return np.array(sorted(set(hp)), dtype=int)


def download_tilepix(cache_path: str | None, retries: int, timeout: int) -> Table:
    if cache_path:
        p = Path(cache_path).expanduser()
        if p.exists():
            try:
                return Table.read(str(p), format="fits")
            except Exception:
                # Invalid cache (often an HTML error page); fall through to re-download.
                pass

    last_err = None
    for _ in range(retries):
        try:
            with urllib.request.urlopen(DESI_TILEPIX_URL, timeout=timeout) as response:
                data = response.read()
            break
        except Exception as err:
            last_err = err
            time.sleep(2)
    else:
        raise RuntimeError(
            f"Failed to download tilepix.fits after {retries} attempts. "
            f"Try again later or pass --tilepix-cache to use a local copy. Last error: {last_err}"
        )

    try:
        fits.open(io.BytesIO(data))
    except Exception as err:
        raise RuntimeError(
            "Downloaded tilepix.fits is not a valid FITS file. "
            "This often means the download returned an error page. "
            f"Last error: {err}"
        )

    t = Table.read(io.BytesIO(data))
    if cache_path:
        p = Path(cache_path).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        try:
            t.write(str(p), overwrite=True)
        except Exception:
            pass
    return t


def build_transfer_data(
    source_endpoint_id,
    destination_endpoint_id,
    label,
    sync_level,
    verify_checksum,
    skip_source_errors,
):
    return globus_sdk.TransferData(
        source_endpoint=source_endpoint_id,
        destination_endpoint=destination_endpoint_id,
        label=label,
        sync_level=sync_level,
        verify_checksum=verify_checksum,
        preserve_timestamp=True,
        fail_on_quota_errors=True,
        skip_source_errors=skip_source_errors,
        deadline=None,
    )


def main(args):
    # Globus endpoint IDs
    source_endpoint_id = DESI_GLOBUS_ENDPOINT
    destination_endpoint_id = args.destination_endpoint_id
    # User-provided endpoint path (ensure no trailing slash for consistency)
    destination_path = args.destination_path
    if not destination_path.startswith("/"):
        print(
            "Warning: destination_path is not absolute. "
            "Prepending '/' to avoid duplicated '/home/USER/home/USER/...'."
        )
        destination_path = "/" + destination_path
    destination_path = destination_path.rstrip("/")

    # Retrieve the DESI tilepix file
    print("Loading DESI tilepix file...")
    tilepix = download_tilepix(
        cache_path=args.tilepix_cache,
        retries=args.tilepix_retries,
        timeout=args.tilepix_timeout,
    )
    print("tilepix loaded.")
    # Filter rows based on args.surveys, keep specific columns, and remove duplicates
    tilepix = tilepix[np.any([tilepix["SURVEY"] == s for s in args.surveys], axis=0)]
    if args.programs:
        tilepix = tilepix[
            np.any([tilepix["PROGRAM"] == p for p in args.programs], axis=0)
        ]

    healpix_ids = None
    if args.healpix_file:
        healpix_ids = read_healpix_ids(args.healpix_file)
    elif args.healpix:
        healpix_ids = np.array(sorted(set(args.healpix)), dtype=int)
    if healpix_ids is not None:
        healpix_set = set(int(x) for x in healpix_ids)
        tilepix = tilepix[np.array([int(h) in healpix_set for h in tilepix["HEALPIX"]])]

    tilepix = tilepix["HEALPIX", "SURVEY", "PROGRAM"]
    tilepix = unique(tilepix, keys=["HEALPIX", "SURVEY", "PROGRAM"])
    print(
        f"Found {len(tilepix)} unique HEALPIX entries "
        f"for surveys: {', '.join(args.surveys)}"
    )

    # --- Globus Authentication and Transfer Setup ---
    native_client = globus_sdk.NativeAppAuthClient(CLIENT_ID)

    # Initial login attempt
    print("\nStarting Globus authentication...")
    transfer_client = login_and_get_transfer_client(native_client)

    # Skip endpoint activation check as we trust they are working
    print("\nAssuming endpoints are already activated...")

    # Create batches of transfers for better reliability
    batch_size = args.batch_size
    total_batches = (len(tilepix) + batch_size - 1) // batch_size
    if args.max_batches is not None:
        total_batches = min(total_batches, args.max_batches)

    print(
        f"\nProcessing {len(tilepix)} files "
        f"in {total_batches} batches of {batch_size} files each"
    )

    # Add the static transfer items to the first batch
    static_files = []
    for item in args.extra_files:
        dest_filename = os.path.basename(item)
        static_files.append((item, os.path.join(destination_path, dest_filename)))

    all_transfer_ids = []

    # Process the batches
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(tilepix))

        print(
            f"\nPreparing batch {batch_num + 1}/{total_batches} "
            f"(files {start_idx + 1}-{end_idx})"
        )

        # Create a transfer data object for this batch
        transfer_data = build_transfer_data(
            source_endpoint_id,
            destination_endpoint_id,
            f"DESI Transfer Batch {batch_num + 1}/{total_batches}",
            args.sync_level,
            args.verify_checksum,
            skip_source_errors=not args.fail_on_missing_source,
        )

        # Add static files to the first batch
        if batch_num == 0 and static_files:
            print("Adding static files to transfer batch...")
            for source_path, dest_path in static_files:
                transfer_data.add_item(source_path, dest_path)

        # Add items for this batch of healpix pixels
        print(f"Adding HEALPIX coadd files to batch {batch_num + 1}...")
        batch_tilepix = tilepix[start_idx:end_idx]

        for row in batch_tilepix:
            healpix = row["HEALPIX"]
            survey = row["SURVEY"]
            program = row["PROGRAM"]
            # Construct source path based on DESI structure
            pix_group = int(healpix / 100)
            source_path = (
                "/dr1/spectro/redux/iron/healpix/"
                f"{survey}/{program}/{pix_group}/{healpix}/"
                f"coadd-{survey}-{program}-{healpix}.fits"
            )
            dest_filename = f"coadd-{survey}-{program}-{healpix}.fits"
            transfer_data.add_item(
                source_path, os.path.join(destination_path, dest_filename)
            )

        # Submit this batch
        print(f"Submitting batch {batch_num + 1} with {len(batch_tilepix)} files...")
        transfer_id = submit_transfer_with_consent_handling(
            transfer_client, transfer_data
        )
        all_transfer_ids.append(transfer_id)

        if batch_num < total_batches - 1 and args.batch_delay > 0:
            print(f"Waiting {args.batch_delay} seconds before submitting next batch...")
            time.sleep(args.batch_delay)

    print("\nAll batches submitted successfully!")
    print("Transfer IDs:")
    for i, tid in enumerate(all_transfer_ids):
        print(f"  Batch {i+1}: https://app.globus.org/activity/{tid}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Transfer data from DESI to user-provided endpoint."
    )
    parser.add_argument(
        "destination_endpoint_id",
        type=str,
        help="The destination Globus endpoint ID.",
    )
    parser.add_argument(
        "destination_path",
        type=str,
        nargs="?",
        default="/home/roccoditella/astroai/spark/iclr/data/globus",
        help="The destination path on the endpoint (default: /home/roccoditella/astroai/spark/iclr/data/globus).",
    )
    parser.add_argument(
        "--surveys",
        type=str,
        nargs="+",
        help="Space delimited list of surveys to transfer.",
        default=["main"],
    )
    parser.add_argument(
        "--programs",
        type=str,
        nargs="+",
        help="Optional list of PROGRAM values to include (e.g., dark bright).",
        default=None,
    )
    parser.add_argument(
        "--healpix",
        type=int,
        nargs="+",
        help="Optional list of HEALPIX IDs to transfer.",
        default=None,
    )
    parser.add_argument(
        "--healpix-file",
        type=str,
        help="Path to a file with HEALPIX IDs (one per line).",
        default=None,
    )
    parser.add_argument(
        "--extra-files",
        type=str,
        nargs="+",
        help="Space delimited list of extra files to transfer.",
        default=[],
    )
    parser.add_argument(
        "--tilepix-cache",
        type=str,
        default=None,
        help="Optional local path to cache tilepix.fits.",
    )
    parser.add_argument(
        "--tilepix-retries",
        type=int,
        default=3,
        help="Number of retries when downloading tilepix.fits (default: 3).",
    )
    parser.add_argument(
        "--tilepix-timeout",
        type=int,
        default=30,
        help="Timeout in seconds for tilepix.fits download (default: 30).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of files per batch (default: 500)",
    )
    parser.add_argument(
        "--batch-delay",
        type=int,
        default=5,
        help="Delay in seconds between batch submissions (default: 5)",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Maximum number of batches to submit (default: no limit)",
    )
    parser.add_argument(
        "--sync-level",
        type=str,
        choices=["exists", "size", "mtime", "checksum"],
        default="exists",
        help="Sync level for transfer (default: exists)",
    )
    parser.add_argument(
        "--verify-checksum",
        action="store_true",
        help="Verify checksums after transfer",
    )
    parser.add_argument(
        "--fail-on-missing-source",
        action="store_true",
        help="Fail the transfer if a source file is missing.",
    )
    args = parser.parse_args()
    main(args)
