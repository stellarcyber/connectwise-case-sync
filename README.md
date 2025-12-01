# connectwise-case-sync
Containerised version of the Sync service between Stellar Cases and ConnectWise Service Tickets

There are 2 important files associated with the Sync service that are contained in this repository:
- config.yaml:
  * affects the behaviour of the sync service (items to be synced between Stellar and ConnectWise)
- cw-sync.env:
  * credentials (users / api keys) need to query the Stellar Cyber instance and ConnectWise

## Directions:

1. Clone this project on a target machine running Docker
   
    `git clone https://github.com/stellarcyber/connectwise-case-sync.git`

2. Navigate to the cloned repo and build the Docker image
   
    `docker build -t cw-case-sync .`

3. Create a directory on the Docker machine that will contain the config. This directory will be bind-mounted to the container at runtime.

   `mkdir /some/config/directory`

4. Copy the config template to the config directory named above and edit. All directives within the config file are commented as to behaviour and expected values.

   `cp config.yaml /some/config/directory/config.yaml`
   
   `vi /some/config/directory/config.yaml`

5. Create a highly protected directory to store the environmental variables that contain all the credentials needed for the service.

   `mkdir /some/protected/directory`

6. Copy the env template to the protected directory and edit.

   `cp cw-sync-template.env /some/protected/directory/cw-sync.env`
   
   `vi /some/protected/directory/cw-sync.env`

7. Run the Docker image using a bind mount to point to the config directory.
   - Replace **/some/config/directory** with the local directory used in step 3/4.
   - Replace **/some/protected/directory** with the local directory used in step 5/6.

   ``docker run --restart unless-stopped -d --mount type=bind,source=/some/config/directory,target=/app/data --env-file /some/protected/directory/cw-sync.env cw-case-sync:latest``

   Logs are stored in a `run.log` file within the config directory

   
 

    
