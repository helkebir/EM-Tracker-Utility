# EM-Tracker-Utility

A command line utility for replying Polhemus EM tracker data.

## Usage

Install required packages using `pip install -r requirements.txt`. Run the `em_util.py` in CLI mode using `python em_util.py` for usage details. By default, the script spawns both a 'replay' thread streaming the data in real-time to a ZMQ pub-sub server on port 5555, under topic `sensor/em/i`. Concurrently, a 'subscriber' thread is spawned to print the streamed data.

A headless mode is available as well, with arguments and usage as follows:

```
usage: em_util.py [-h] [-f FILE] [-r] [-n SENSORS]

A ZMQ PUB server to replay sensor data from a CSV file.

optional arguments:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  Path to the input CSV file for headless operation.
  -r, --loop            Enable looping of the data replay in headless mode.
  -n SENSORS, --sensors SENSORS
                        Number of sensor topics to subscribe to in headless mode (default: 4).
```
