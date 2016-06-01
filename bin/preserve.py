#!/usr/bin/env python2

from __future__ import print_function
import argparse
import os
import subprocess

DEFAULT_NUM_MACHINES=1
DEFAULT_TIME="0:15:00"

class InvalidNumMachinesException(Exception): pass
class ReservationFailedException(Exception): pass
class ReservationNotFoundException(Exception): pass

def parse_arguments():
    """Parses arguments passed on the command-line."""
    parser = argparse.ArgumentParser(description="Manage reservations using preserve")
    subparsers = parser.add_subparsers(title="Reservation management commands")

    # Add subparser for "reserve" command
    reserve_parser = subparsers.add_parser("reserve", help="reserve machines", fromfile_prefix_chars="@")
    reserve_parser.add_argument("-n", "--num-machines", help="number of machines to reserve", action="store", type=int, default=DEFAULT_NUM_MACHINES)
    reserve_parser.add_argument("-t", "--time", help="time to reserve machines for as [[hh:]mm:]ss (default: %s)" % DEFAULT_TIME, action="store", default=DEFAULT_TIME)
    reserve_parser.add_argument("-q", "--quiet", help="output only the reservation id", action="store_true")
    reserve_parser.set_defaults(func=reserve_machines)

    # Add subparser for "list-reservations" command
    list_reservations_parser = subparsers.add_parser("list-reservations", help="list active reservations through preserve", fromfile_prefix_chars="@")
    list_reservations_parser.add_argument("-a", "--all", help="list reservations for all users", action="store_true")
    list_reservations_parser.set_defaults(func=list_reservations)

    # Add subparser for "fetch-reservation" command
    fetch_reservation_parser = subparsers.add_parser("fetch-reservation", help="display information on a single reserveration", fromfile_prefix_chars="@")
    fetch_reservation_parser.add_argument("reservation_id", metavar="reservation-id", help="id of the reservation to display", action="store")
    fetch_reservation_parser.add_argument("--as-args", help="print master and worker nodes as command-line arguments", action="store_true")
    fetch_reservation_parser.set_defaults(func=fetch_reservations)

    return parser.parse_args()

def _reserve_machines(num_machines, time, quiet=False):
    """Reserves a number of machines for a given time and outputs the reservation id."""
    if num_machines < 1:
        raise InvalidNumMachinesException("Number of machines must be at least one.")

    # Invoke preserve to make the reservation
    reservation_output = subprocess.Popen(["preserve", "-np", str(num_machines), "-t", time], stdout=subprocess.PIPE).communicate()[0].decode("utf-8")

    # Extract the reservation ID 
    for line in reservation_output.split('\n'):
        if line.startswith("Reservation number"):
            reservation_id = line.strip(":").split(" ")[-1]
            break
    if reservation_id is None:
        raise ReservationFailedException("preserve did not print a reservation id. Output:\n%s" % reservation_output)

    # Output the reservation ID
    if not quiet:
        print("Reservation succesful. Reservation ID is %s." % reservation_id)
    else:
        print(reservation_id)

def _parse_preserver_llist():
    """Parses the preserve output to extract a map of reservation objects."""
    list_output = subprocess.Popen(["preserve", "-llist"], stdout=subprocess.PIPE).communicate()[0].decode("utf-8")
    reservations = {}
    found_header = False
    for line in list_output.split('\n'):
        if not found_header:
            if line.startswith("id"):
                found_header = True
        elif line.strip():
            parts = line.split()
            reservations[parts[0]] = {
                "id": parts[0],
                "user": parts[1],
                "start_date": parts[2],
                "start_time": parts[3],
                "end_date": parts[4],
                "end_time": parts[5],
                "state": parts[6],
                "num_machines": parts[7],
                "machines": parts[8:]
            }
    return reservations


def _list_reservations(all_users):
    """Lists active reservations in a machine-parsable format."""
    reservations = _parse_preserver_llist()
    username = os.environ["USER"]
    for reservation_id in reservations:
        if all_users or reservations[reservation_id]["user"] == username:
            print(reservations[reservation_id])

def _fetch_reservation(reservation_id, as_args):
    """Displays information for a single reservation."""
    reservations = _parse_preserver_llist()
    reservation = reservations[reservation_id]
    if not reservation:
        raise ReservationNotFoundException('Could not find reservation for id "%s".' % reservation_id)
    
    machines = sorted(reservation["machines"])
    if as_args:
        arguments = ""
        if len(reservation["machines"]) > 0:
            arguments = "%s --master %s" % (arguments, reservation["machines"][0])
        if len(reservation["machines"]) > 1:
            for machine in reservation["machines"][1:]:
                arguments = "%s --worker %s" % (arguments, machine)
        print(arguments.strip())
    else:
        print("Reservation ID: %s" % reservation_id)
        print("State:          %s" % reservation["state"])
        print("Start time:     %s %s" % (reservation["start_date"], reservation["start_time"]))
        print("End time:       %s %s" % (reservation["end_date"], reservation["end_time"]))
        print("Machines:       %s" % " ".join(reservation["machines"]))

def reserve_machines(args):
    """Extracts arguments from CLI to reserve machines."""
    _reserve_machines(args.num_machines, args.time, args.quiet)

def list_reservations(args):
    """Extracts arguments from CLI to list reservations."""
    _list_reservations(args.all)

def fetch_reservations(args):
    """Extracts arguments from CLI to fetch a single reservation."""
    _fetch_reservation(args.reservation_id, args.as_args)

if __name__ == "__main__":
    args = parse_arguments()
    args.func(args)

