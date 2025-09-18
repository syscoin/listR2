# listR2

Little script to keep track what objects are stored in a S3-compatible R2 bucket.

Expanded to check if objects of bucket 1 are found in bucket 2 and allow to copy them if missing.

Credentials and parameters are set in *listR2_config.ini*.  
Since bucket access keys are sensitive data, this repo only includes a template for the ini file.  
That template includes comments explaining sections and settings.

If *'check_secondary = False'*, no other data for that section are necessary.

Allow to only list a range of objects (sorted by date).  
Allow to check if objects exist in a second bucket.  
Allow to copy objects to the second bucket if they don't exist there.

To copy objects into the second bucket, the check must be enabled too.  
Use the logical variables accordingly.

The ini file has settings for mainnet and testnet,
making it more convenient to switch between both.

If mainnet and testnet are both enabled,
only a summary of both will be generated.  
The 'sum_only' switch does the same,
but can be used for a single network only.
