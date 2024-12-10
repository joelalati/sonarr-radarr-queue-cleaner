<a href="[https://www.paypal.com/ncp/payment/DKGKXXEYNDS7S" target="[_blank]"><img src="https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png" alt="Buy Me A Coffee" style="height: 41px !important;width: 174px !important;box-shadow: 0px 3px 2px 0px rgba(190, 190, 190, 0.5) !important;-webkit-box-shadow: 0px 3px 2px 0px rgba(190, 190, 190, 0.5) !important;" ></a>

# sonarr-radarr-queue-cleaner
This is a fork of MattDGTLs project with some added functionality. Credit to the overall idea can go here: https://github.com/MattDGTL/sonarr-radarr-queue-cleaner

A simple Sonarr and Radarr script to clean out stalled downloads.

A "strike" system to ensure the stalled downloads have been stalled for a while.

The amount of strikes and amount of time between checks can be changed in the config file.

To use with Docker:
  1. ```
     git clone https://github.com/PaeyMoopy/sonarr-radarr-queue-cleaner.git

     cd sonarr-radarr-queue-cleaner
     ```
2. edit the config file and input your server information.
   API_TIMEOUT = how often to check for stalled downloads in seconds.
   STRIKE_COUNT = how many strikes before looking for a new download.

   For example, if API_TIMEOUT = 600, and STRIKE_COUNT = 5, the system will check for stalled downloads every 10 minutes. If any item is stalled, it gets a strike. Once an item recieves 5 strikes it gets removed and searched.
   So in this example, any item that has been stalled for 1 hour will get removed and searched.
   ```
   nano config.json
   ```
3. Once you've saved the config:
   ```
   docker build -t media-cleaner .
   ```
4. Then start the container.
   ```
   docker run -d --name media-cleaner media-cleaner
   ```
   Or if you want it to start automatically on boot:
   ```
   docker run -d --name media-cleaner --restart unless-stopped media-cleaner
   ```


To use without docker:  
  1. ```
     git clone https://github.com/PaeyMoopy/sonarr-radarr-queue-cleaner.git

     cd sonarr-radarr-queue-cleaner
     ```
2. edit the config file and input your server information.
   API_TIMEOUT = how often to check for stalled downloads in seconds.
   STRIKE_COUNT = how many strikes before looking for a new download.

   For example, if API_TIMEOUT = 600, and STRIKE_COUNT = 5, the system will check for stalled downloads every 10 minutes. If any item is stalled, it gets a strike. Once an item recieves 5 strikes it gets removed and searched.
   So in this example, any item that has been stalled for 1 hour will get removed and searched.
   ```
   nano config.json
   ```
3. ```
   pip install requirements.txt
   ```
4. ```
   python3 cleaner.py
   ```
