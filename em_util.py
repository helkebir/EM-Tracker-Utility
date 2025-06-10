import zmq
import pandas as pd
import numpy as np
import time
import struct
import threading
import os
import csv
import argparse
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# --- Data Packing/Unpacking Functions ---

def pack_sensor_data(row):
    """Packs a row of sensor data into a binary format."""
    return struct.pack('<iqfffffff',
                     int(row['sensor_id']),
                     int(row['time']),
                     row['x'], row['y'], row['z'],
                     row['qw'], row['qx'], row['qy'], row['qz'])

def unpack_sensor_data(message_bytes):
    """Unpacks a binary message into a tuple of sensor data."""
    return struct.unpack('<iqfffffff', message_bytes)


# --- ZMQ Publisher Functions ---

def replay(rel_path, loop_data=False):
    """
    A ZMQ PUB server that reads a CSV file and streams its content.
    Can optionally loop the data stream. Binds to port 5555.
    """
    if not os.path.exists(rel_path):
        print(f"REPLAYER: Error - File not found at '{rel_path}'")
        return

    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:5555")
    print(f"REPLAYER: ZMQ replayer running on port 5555, reading from '{rel_path}'...")
    time.sleep(1) # Pause to allow subscribers to connect

    column_mapping = {
        'SEU': 'sensor_id', 'Frame': 'time',
        'X1 (M)': 'x', 'Y1': 'y', 'Z1': 'z',
        'Qw1': 'qw', 'Qx1': 'qx', 'Qy1': 'qy', 'Qz1': 'qz'
    }

    try:
        df = pd.read_csv(rel_path)
        df.columns = df.columns.str.strip()
        df = df.rename(columns=column_mapping)
        df = df[list(column_mapping.values())]
        df['sensor_id'] = df['sensor_id'].astype(int)

        time_diffs = np.diff(df['time'], prepend=df['time'].iloc[0]) / 1000.0

        print(f"REPLAYER: Starting data replay. Looping is {'ENABLED' if loop_data else 'DISABLED'}.")
        
        while True:
            for i, row in df.iterrows():
                topic = f"sensor/em/{int(row['sensor_id'])}"
                message = pack_sensor_data(row)
                socket.send_multipart([topic.encode('utf-8'), message])
                print(f"REPLAYER: Sent on topic '{topic}'")
                if i < len(time_diffs) and time_diffs[i] > 0:
                    time.sleep(time_diffs[i])
            
            if not loop_data:
                break
            
            print("REPLAYER: Replay loop restarting...")
            time.sleep(1)

        print("REPLAYER: Replay finished.")
        socket.send_multipart([b'control/done', b''])
        time.sleep(1)

    except (KeyboardInterrupt, SystemExit):
        print("\nREPLAYER: Replay interrupted by user.")
    except Exception as e:
        print(f"REPLAYER: An error occurred: {e}")
    finally:
        socket.close()
        context.term()
        print("REPLAYER: Server shut down.")


# --- ZMQ Subscriber Function ---

def consume(N):
    """
    A ZMQ SUB client that subscribes to N sensor topics and a control topic.
    Connects to port 5555.
    """
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5555")
    print(f"CONSUMER: Subscribing to {N} sensor topics...")

    for i in range(1, N + 1):
        socket.setsockopt_string(zmq.SUBSCRIBE, f"sensor/em/{i}")
    socket.setsockopt_string(zmq.SUBSCRIBE, 'control/done')

    try:
        while True:
            topic_bytes, message_bytes = socket.recv_multipart()
            topic = topic_bytes.decode('utf-8')

            if topic == 'control/done':
                print("CONSUMER: Received done signal. Shutting down.")
                break

            data = unpack_sensor_data(message_bytes)
            output = (f"CONSUMER: Received on '{topic}': "
                      f"ID={data[0]}, Time={data[1]}, "
                      f"Pos=({data[2]:.2f}, {data[3]:.2f}, {data[4]:.2f}), "
                      f"Quat=({data[5]:.2f}, {data[6]:.2f}, {data[7]:.2f}, {data[8]:.2f})")
            print(output)

    except (KeyboardInterrupt, SystemExit):
        print("\nCONSUMER: Shutting down consumer...")
    finally:
        socket.close()
        context.term()
        print("CONSUMER: Consumer shut down.")


# --- CLI, Help, and File Explorer ---

def create_dummy_csv(file_path):
    """Creates a sample CSV file for the replay function."""
    header = ['SEU', 'FrErr', 'Frame', 'Sensor1', 'btn0_1', 'btn1_1', 'd1', 'aux(hex)_1', 'X1 (M)', 'Y1', 'Z1', 'Qw1', 'Qx1', 'Qy1', 'Qz1']
    data = [
        [1.0, 0, 100, 1, 0, 0, 0, '0x0', 0.1, 0.2, 0.3, 1.0, 0.0, 0.0, 0.0],
        [2.0, 0, 220, 2, 0, 0, 0, '0x0', 0.4, 0.5, 0.6, 0.9, 0.1, 0.0, 0.0],
        [1.0, 0, 350, 1, 0, 0, 0, '0x0', 0.1, 0.3, 0.4, 0.8, 0.2, 0.1, 0.0],
        [3.0, 0, 500, 3, 0, 0, 0, '0x0', 0.7, 0.8, 0.9, 0.7, 0.3, 0.2, 0.1],
    ]
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
    print(f"INFO: Created dummy CSV '{file_path}' with float sensor IDs.")

def print_help_message(console):
    """Displays the help panel in the CLI."""
    help_text = """
[bold]Purpose:[/] This tool replays sensor data from a CSV file over a ZMQ PUB socket on port 5555. It can be run in two modes: interactive or headless.

[bold]Interactive Usage (no arguments):[/]
* Navigate directories by entering the index number of a directory.
* Select a .csv file by entering its index number.
* [bold yellow]g[/]: Generate a dummy 'replay_data.csv' in the current directory.
* [bold yellow]q[/]: Quit the application.

[bold]Headless Usage (with arguments):[/]
This mode bypasses the interactive CLI for scripting or direct execution.

[bold]Arguments:[/]
* [bold cyan]-f, --file PATH[/]   (Required) Path to the input CSV file.
* [bold cyan]-r, --loop[/]        (Optional) Loop the data replay indefinitely.
* [bold cyan]-n, --sensors N[/]  (Optional) Number of sensor topics for the consumer to subscribe to. Defaults to 4.

[bold]Example:[/]
[green]python em_util.py -f data/log.csv -r -n 3[/]
    """
    console.print(Panel.fit(help_text, title="[bold yellow]Help / Usage[/]", border_style="yellow"))


def file_explorer(console, start_path='.'):
    """An interactive CLI file explorer to select a CSV file."""
    current_path = os.path.abspath(start_path)

    while True:
        try:
            items = os.listdir(current_path)
            dirs = sorted([d for d in items if os.path.isdir(os.path.join(current_path, d))])
            files = sorted([f for f in items if os.path.isfile(os.path.join(current_path, f))])

            table = Table(title=f"Path: {current_path}", style="cyan", title_style="bold magenta")
            table.add_column("Index", style="dim", width=5)
            table.add_column("Type", width=6)
            table.add_column("Name")

            table.add_row("0", "[dir]", ".. (Parent Directory)")
            table.add_row("h", "[opt]", "Help / Usage Information")
            table.add_row("g", "[opt]", "Generate dummy 'replay_data.csv'")
            table.add_row("q", "[opt]", "Quit")

            display_items = [("..", "[dir]")] + [(d, "[dir]") for d in dirs] + [(f, "[file]") for f in files]

            for i, (name, item_type) in enumerate(display_items):
                if i == 0: continue
                style = "white";
                if name.endswith('.csv'): style = "bold green"
                elif item_type == "[dir]": style = "bold blue"
                table.add_row(str(i), item_type, name, style=style)

            console.print(table)
            choice = console.input("Enter [bold yellow]Index[/] to select, or an [bold yellow]Option[/]: ").lower().strip()

            if choice == 'q': return None
            if choice == 'g': create_dummy_csv("replay_data.csv"); continue
            if choice == 'h': print_help_message(console); continue
            
            if not choice.isdigit():
                console.print("[bold red]Invalid input.[/]\n"); continue

            idx = int(choice)
            if not (0 <= idx < len(display_items)):
                console.print("[bold red]Index out of range.[/]\n"); continue

            selected_name, selected_type = display_items[idx]
            selected_path = os.path.join(current_path, selected_name)

            if selected_type == "[dir]":
                current_path = os.path.abspath(selected_path)
            elif selected_name.endswith('.csv'):
                console.print(f"\n[bold green]Selected file: {selected_path}[/]")
                return selected_path
            else:
                console.print(f"[bold red]'{selected_name}' is not a CSV file.[/]\n")
        except Exception as e:
            console.print(f"[bold red]An error occurred: {e}[/]"); return None

def cli():
    """Main Interactive Command Line Interface function."""
    console = Console()
    console.rule("[bold green]MarginDx EM Utility - ZMQ CSV Replay Tool - Interactive Mode[/]")
    console.print("[green]Author: Hamza El-Kebir (elkebir2@illinois.edu)[/]")
    console.print("[dim]Tip: Run with '-h' for headless mode options (e.g., 'python em_util.py -f data.csv').[/dim]\n")
    
    csv_path = file_explorer(console)
    if not csv_path:
        console.print("[bold yellow]No file selected. Exiting.[/]"); return

    loop_choice = console.input("Loop the data replay? (y/N): ").lower().strip()
    should_loop = (loop_choice == 'y')

    while True:
        try:
            n_str = console.input("Enter the number of sensor topics to subscribe to \[default: 4]: ")
            if not n_str.strip():
                n_sensors = 4; console.print(f"[dim]Using default value of {n_sensors} sensors.[/dim]"); break
            n_sensors = int(n_str)
            if n_sensors > 0: break
            else: console.print("[bold red]Please enter a positive number.[/]")
        except ValueError:
            console.print("[bold red]Invalid input. Please enter an integer.[/]")

    publisher_thread = threading.Thread(target=replay, args=(csv_path, should_loop))
    consumer_thread = threading.Thread(target=consume, args=(n_sensors,))

    console.rule(f"[bold green]Starting replay of '{os.path.basename(csv_path)}'[/]")
    publisher_thread.start()
    consumer_thread.start()

    try:
        publisher_thread.join()
        consumer_thread.join()
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Main thread caught interrupt. Exiting.[/]")

    console.rule("[bold green]Program Finished[/]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A ZMQ PUB server to replay sensor data from a CSV file.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '-f', '--file',
        type=str,
        help="Path to the input CSV file for headless operation."
    )
    parser.add_argument(
        '-r', '--loop',
        action='store_true',
        help="Enable looping of the data replay in headless mode."
    )
    parser.add_argument(
        '-n', '--sensors',
        type=int,
        default=4,
        help="Number of sensor topics to subscribe to in headless mode (default: 4)."
    )
    args = parser.parse_args()

    # If a file is provided via command line, run in headless mode.
    if args.file:
        print("--- Running in Headless Mode ---")
        publisher_thread = threading.Thread(target=replay, args=(args.file, args.loop))
        consumer_thread = threading.Thread(target=consume, args=(args.sensors,))
        
        publisher_thread.start()
        consumer_thread.start()
        try:
            publisher_thread.join()
            consumer_thread.join()
        except KeyboardInterrupt:
            print("\nCaught interrupt. Shutting down.")
        print("--- Headless Mode Finished ---")
    # Otherwise, launch the interactive CLI.
    else:
        cli()
