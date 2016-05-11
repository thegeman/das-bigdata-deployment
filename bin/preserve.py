#!/usr/bin/env python2

from __future__ import print_function
import argparse
import subprocess

DEFAULT_NUM_MACHINES=1
DEFAULT_TIME="0:15:00"

class InvalidNumMachinesException(Exception): pass
class ReservationFailedException(Exception): pass

def parse_arguments():
    """Parses arguments passed on the command-line."""
    parser = argparse.ArgumentParser(description="Manage reservations using preserve")
    subparsers = parser.add_subparsers(title="Reservation management commands")

    # Add subparser for "reserve" command
    reserve_parser = subparsers.add_parser("reserve", help="reserve machines", fromfile_prefix_chars="@")
    reserve_parser.add_argument("-n", "--num-machines", help="number of machines to reserve", action="store", type=int, default=DEFAULT_NUM_MACHINES)
    reserve_parser.add_argument("-t", "--time", help="time to reserve machines for as [[hh:]mm:]ss", action="store", default=DEFAULT_TIME)
    reserve_parser.add_argument("-q", "--quiet", help="output only the reservation id", action="store_true")
    reserve_parser.set_defaults(func=reserve_machines)

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

    if not quiet:
        print("Reservation succesful. Reservation ID is %s." % reservation_id)
    else:
        print(reservation_id)
    
    """Example output:

    Reservation number 17540:
    ---queued----
    Notice: before reservation start time, node allocation is tentative;
    nodes actually allocated may be different.
    Check with preserve -long-list when reservation has started.
    """

def reserve_machines(args):
    """Extracts arguments from CLI to reserve machines."""
    _reserve_machines(args.num_machine, args.time, args.quiet)

if __name__ == "__main__":
    args = parse_arguments()
    args.func(args)

