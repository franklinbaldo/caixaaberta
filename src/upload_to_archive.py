import os
import argparse
import datetime
from internetarchive import upload, get_item, search_items
from unittest.mock import patch

# Configuration for Archive.org
# These would ideally be more dynamic or configurable if needed
ARCHIVE_ORG_ITEM_TITLE_PREFIX = "RealEstateDataDuckDB"
ARCHIVE_ORG_COLLECTION = "opensource_data" # Example collection, choose an appropriate one
ARCHIVE_ORG_SUBJECTS = ["real estate", "housing data", "brazil", "public data", "duckdb"]

def get_archive_identifier(base_title):
    """
    Creates a unique identifier for Archive.org, typically based on the title.
    Archive.org identifiers have specific constraints (e.g., no spaces, certain characters).
    Internet Archive identifiers should only contain:
    - Alphanumeric characters (a-z, A-Z, 0-9)
    - Underscore (_)
    - Hyphen (-)
    - Period (.) - though often best to avoid if it can be confused with file extensions
    It's common to make them all lowercase.
    """
    import re
    # Convert to lowercase
    s = base_title.lower()
    # Replace spaces and consecutive problematic characters with a single underscore
    s = re.sub(r'[\s\W]+', '_', s) # \s for whitespace, \W for non-alphanumeric (opposite of \w)
    # Remove leading/trailing underscores that might result from replacements
    s = s.strip('_')
    # Internet Archive says: "Identifiers may contain only letters, numbers, hyphens, underscores, and periods."
    # The regex \w includes letters, numbers, and underscore. So we allow hyphens explicitly.
    # Let's refine to be more specific about allowed characters after initial cleanup.
    # Keep only alphanumeric, underscore, hyphen.
    s = re.sub(r'[^a-z0-9_-]', '', s)

    # Ensure it's not empty after sanitization
    if not s:
        s = "untitled_item" # Fallback for empty titles after sanitization
    return s

def upload_duckdb_to_archive(db_filepath, ia_access_key, ia_secret_key, item_identifier=None, item_title=None, collection=None, subjects=None, description=None, dry_run=False):
    """
    Uploads the specified DuckDB file to Archive.org.

    Args:
        db_filepath (str): Path to the DuckDB file to upload.
        ia_access_key (str): Internet Archive Access Key.
        ia_secret_key (str): Internet Archive Secret Key.
        item_identifier (str, optional): A unique identifier for the item on Archive.org.
                                         If None, one will be generated based on item_title.
        item_title (str, optional): The title for the item on Archive.org.
                                    Defaults to a title based on filename and date.
        collection (str, optional): The collection to upload to. Defaults to ARCHIVE_ORG_COLLECTION.
        subjects (list, optional): List of subject tags. Defaults to ARCHIVE_ORG_SUBJECTS.
        description (str, optional): Description for the archive item.
        dry_run (bool): If True, simulates the upload without actual network calls.

    Returns:
        str: The URL of the uploaded item, or a message indicating dry run.
    """
    if not os.path.exists(db_filepath):
        print(f"Error: Database file not found at {db_filepath}")
        return None

    filename = os.path.basename(db_filepath)
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")

    if not item_title:
        item_title = f"{ARCHIVE_ORG_ITEM_TITLE_PREFIX} - {filename} - {current_date}"

    if not item_identifier:
        # Generate a reasonably unique identifier.
        # For simplicity, let's base it on the title and add a timestamp to ensure uniqueness
        # if multiple uploads happen on the same day with the same prefix.
        # A more robust way might involve checking if an item with this ID already exists.
        timestamp_slug = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        base_id = get_archive_identifier(item_title)
        item_identifier = f"{base_id}_{timestamp_slug}"


    # Default metadata
    metadata = {
        "title": item_title,
        "collection": collection or ARCHIVE_ORG_COLLECTION,
        "subject": subjects or ARCHIVE_ORG_SUBJECTS,
        "description": description or f"DuckDB database containing real estate data, generated on {current_date}. File: {filename}",
        "mediatype": "data", # Important for data uploads
        "creator": "Real Estate Data Pipeline (Automated Script)", # Or your organization
        "date": current_date,
    }

    print(f"Preparing to upload '{filename}' to Archive.org.")
    print(f"Item Identifier: {item_identifier}")
    print(f"Item Title: {metadata['title']}")
    print(f"Collection: {metadata['collection']}")
    print(f"Metadata: {metadata}")

    if dry_run:
        print("\n--- DRY RUN ---")
        print(f"Would upload '{db_filepath}' to Archive.org item '{item_identifier}'.")
        print(f"Metadata: {metadata}")
        print("--- END DRY RUN ---")
        return f"DRY RUN: Would upload to https://archive.org/details/{item_identifier}"

    if not ia_access_key or not ia_secret_key:
        print("Error: Internet Archive Access Key or Secret Key is missing.")
        print("Please set IA_ACCESS_KEY and IA_SECRET_KEY environment variables.")
        return None

    try:
        print(f"\nUploading '{filename}' to item '{item_identifier}'...")
        # The `upload` function can create an item if it doesn't exist, or add files to an existing one.
        # It's generally better to create a new item for each version of the database,
        # or manage versions within a single item carefully if that's the strategy.
        # For this example, we create a new item identifier each time to avoid collisions.

        # Ensure the item exists or create it with metadata first (optional but good practice)
        # item = get_item(item_identifier)
        # if not item.exists:
        #     print(f"Item '{item_identifier}' does not exist. It will be created during upload.")

        # The 'files' argument takes a list of files to upload.
        # The 'metadata' argument sets the metadata for the item.
        # The 'access_key' and 'secret_key' are used for authentication.
        # 'queue_derive=False' can be useful for data items if no derivatives are needed.
        # 'verbose=True' gives more output.
        response = upload(
            identifier=item_identifier,
            files={filename: db_filepath}, # Uploads db_filepath as 'filename' in the item
            metadata=metadata,
            access_key=ia_access_key,
            secret_key=ia_secret_key,
            verbose=True,
            queue_derive=False # Often set for data files
        )

        if response and response[0].status_code == 200:
            item_url = f"https://archive.org/details/{item_identifier}"
            print(f"Upload successful! Item URL: {item_url}")
            return item_url
        else:
            print("Upload failed. Response:")
            print(response)
            return None

    except Exception as e:
        print(f"An error occurred during upload: {e}")
        # For debugging, you might want to print the full traceback
        # import traceback
        # traceback.print_exc()
        return None

def main():
    parser = argparse.ArgumentParser(description="Upload a DuckDB database to Archive.org.")
    parser.add_argument("db_filepath", help="Path to the DuckDB database file.")
    parser.add_argument("--identifier", help="Optional: Specific Archive.org item identifier.")
    parser.add_argument("--title", help="Optional: Title for the Archive.org item.")
    parser.add_argument("--description", help="Optional: Description for the Archive.org item.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate upload without actual network activity.")

    args = parser.parse_args()

    # Get credentials from environment variables
    ia_access_key = os.getenv("IA_ACCESS_KEY")
    ia_secret_key = os.getenv("IA_SECRET_KEY")

    if not args.dry_run and (not ia_access_key or not ia_secret_key):
        print("Error: IA_ACCESS_KEY and IA_SECRET_KEY environment variables must be set for actual uploads.")
        print("You can use --dry-run to simulate the process without credentials.")
        return

    # For testing purposes, if we are in a test environment and want to mock,
    # this is where the mocking logic would be more explicitly controlled.
    # The `dry_run` flag serves a similar purpose for CLI usage.

    # Example of how to use the mocking for programmatic calls if needed:
    # if os.getenv("CI_TEST_MODE") == "true" and not args.dry_run:
    #     print("CI TEST MODE: Simulating upload with mock.")
    #     with patch('internetarchive.upload') as mock_upload:
    #         mock_upload.return_value = [type('obj', (object,), {'status_code': 200})()] # Mock successful response
    #         upload_url = upload_duckdb_to_archive(
    #             args.db_filepath, "mock_key", "mock_secret", # Mock creds for the call
    #             item_identifier=args.identifier,
    #             item_title=args.title,
    #             description=args.description,
    #             dry_run=False # Set to false to test the mocked path
    #         )
    # else:
    upload_url = upload_duckdb_to_archive(
        args.db_filepath, ia_access_key, ia_secret_key,
        item_identifier=args.identifier,
        item_title=args.title,
        description=args.description,
        dry_run=args.dry_run
    )

    if upload_url:
        print(f"Process completed. URL: {upload_url}")
    else:
        print("Process failed or was a dry run with no URL generated.")

if __name__ == "__main__":
    main()
