#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""This script fetches, calculates and displays meta information of a FHIR server."""

from typing import Callable
from typing import Union
import argparse
import os.path
import platform

import rich.console
import rich.live
import rich.table
import rich.text
import rich.tree

from fhirclient.client import FHIRClient
from fhirclient.models.bundle import Bundle
from fhirclient.models.capabilitystatement import CapabilityStatement
from fhirclient.models.fhirabstractbase import FHIRValidationError
from fhirclient.server import FHIRServer

# set up logging
import logging
logging.basicConfig(format="%(message)s", level=logging.ERROR)

__author__ = "Felix Zinkewitz"
__version__ = "0.1"
__date__ = "2023-01-21"

# set up rich_console
rich_console = rich.console.Console(highlight=False)

if platform.system() == "Windows":
    COLOR_GRAY = "bright_black"
else:
    COLOR_GRAY = "white"


def main(args) -> int:
    """Main function"""

    fhir_settings = {
        "app_id": os.path.basename(__file__),  # script name
        "api_base": args.server_url
    }

    fc = FHIRClient(settings=fhir_settings)

    try:
        rich_console.print(f"Checking connection to FHIR server \"{args.server_url}\"...")
        capability_statement = CapabilityStatement.read_from("metadata", fc.server)
        # print some meta data
        rich_console.print(f"[green]Remote: {capability_statement.software.name} {capability_statement.software.version} (FHIR version: {capability_statement.fhirVersion})")
    except Exception:
        rich_console.print(f"[bright_red]Connection to FHIR server \"{args.server_url}\" failed.")
        return 1

    if args.list:  # list and count all resources
        return list_resources(fc, capability_statement, args.zero, args.novalidation)
    elif args.resource:  # inspect resource
        return inspect_resource(fc, capability_statement, args.resource, args.limit, args.items, args.max_level - 1, args.novalidation)
    elif args.structure_definitions:  # list structure definitions
        return list_structure_definitions(fc, capability_statement, args.novalidation)
        

def bundle_read_from(path: str, server: FHIRServer, novalidation: bool) -> Bundle:
    """Helper function to create a bundle object with validation turned off.

    Taken from the fhirclient 'fhirabstractresource.py' modul.
    (Copyright 2015 Boston Children's Hospital, Apache License, Version 2.0)

    Requests data from the given REST path on the server and creates
    an instance of the bundle class.

    Args:
        path (str): the REST path to read from
        server (FHIRServer): an instance of a FHIR server or compatible class
        novalidation (bool): True if validation of fhir resources should be turned off

    Returns:
        Bundle: an instance of bundle class
    """

    if not path:
        raise Exception("Cannot read resource without REST path.")
    if server is None:
        raise Exception("Cannot read resource without server instance.")

    ret = server.request_json(path)
    if novalidation:
        instance = Bundle(jsondict=ret, strict=False)
    else:
        instance = Bundle(jsondict=ret, strict=True)
    instance.origin_server = server
    return instance


def fetch_resources(
        fc: FHIRClient,
        capability_statement: CapabilityStatement,
        resource_type: str,
        limit: Union[None, int],
        novalidation: bool,
        function_to_call: Callable,
        pass_entry_as_json: bool,
        function_params: list
) -> int:
    """Receives all resources of given type from a FHIR server and passes resources to {function_to_call}.

    Args:
        fc (FHIRClient): FHIRClient object
        capability_statement (CapabilityStatement): the CapabilityStatement object belonging to the fhir server
        resource_type (str): type of the resources to fetch
        limit (Union[None, int]): maximum number of resources to receive or None if no limit
        novalidation (bool): True if validation of fhir resources should be turned off
        function_to_call (Callable): Neo4j driver object
        pass_entry_as_json (bool): if True, the entry is passed as JSON instead as an object.
        function_params (list): a list of parameters to pass to {function_to_call}

    Returns:
        int: status code
    """

    # get total count of given resource on server
    try:
        bundle = bundle_read_from(resource_type + "?_summary=count", fc.server, novalidation)
    except Exception as e:  # for some reason we're unable to catch server.FHIRNotFoundException here, so catch all Exceptions instead
        rich_console.print(f"[bright_red]Error while reading resource: {e}.")
        return 1

    total = bundle.total

    if total == 0:
        rich_console.print(f"[bright_red]No resources of type \"{resource_type}\" found on server.")
        return 1
    else:
        rich_console.print(f"{total} resources of type \"{resource_type}\" on server. Fetching resources...")
        if novalidation is True:
            # unfortunately bundles with invalid entries does not contain any entry to process,
            # so even the valid entries of the bundle are lost.
            rich_console.print("Notice: Validation is turned off. Bundles with invalid entries will be discarded without information.")

    next_url = resource_type  # e.g. query "SERVER-URL\Patient" to receive all Patient data sets
    received = 0
    
    # helper function for consistent status text formation
    def format_status_text(rec, tot):
        return rich.text.Text(f"Received {rec} of {tot} items.")

    with rich.live.Live(rich.text.Text("Receiving items..."), transient=True, console=rich_console) as live:
        while True:
            try:
                # read bundle from server. Server decides how many entries the bundle will have.
                bundle = bundle_read_from(next_url, fc.server, novalidation)
            except FHIRValidationError:
                rich_console.print("[bright_red]Got validation error (consider using \"--novalidation\").")
                break
            except Exception:
                raise

            # did we receive valid entries?
            if bundle.entry is not None:            
                received = received + len(bundle.entry)
                
                # loop through entries and call {function_to_call} for further processing
                for entry in bundle.entry:
                    if pass_entry_as_json:
                        function_to_call(entry.resource.as_json(), *function_params)
                    else:
                        function_to_call(entry.resource, *function_params)
                        
                # update status
                live.update(format_status_text(received, total))
                
                # when limit is set, break of limit is reached
                if limit is not None and limit <= received:
                    rich_console.print(f"Reached limit of {limit} resources to receive.")
                    break

            # Is there a "next relation" in the link items?
            # Some servers send invalid domain name. It's a bit tricky to figure out the part of the link for {next_url}
            # Solution: Get server base URL from capability statement and split {fc.server.base_uri} using this base URL. Remove leading slash finally.
            next_url = False
            for link in bundle.link:
                if link.relation == "next":
                    next_url = link.url.split(capability_statement.implementation.url)[-1].lstrip("/")
                    break
            if next_url is False:
                break  # no "next relation", indicates end of bundles, leave loop
                
    # output final result
    rich_console.print(f"[green]{format_status_text(received, total)}")
    return 0


def list_resources(
        fc: FHIRClient,
        capability_statement: CapabilityStatement,
        display_zero: bool,
        novalidation: bool
) -> int:
    """Fetches a list of resources the FHIR server is capable of handling of.
    Receives the actually stored count of resources of each type.

    Args:
        fc (FHIRClient): FHIRClient object
        capability_statement (CapabilityStatement): the CapabilityStatement object belonging to the fhir server
        display_zero (bool): also output resources with zero count
        novalidation (bool): True if validation of fhir resources should be turned off

    Returns:
        int: status code
    """

    rich_console.print(f"Fetching list of resources of FHIR server \"{fc.server.base_uri}\"...")

    table = rich.table.Table(box=None, pad_edge=False)
    table.add_column("RESOURCE")
    table.add_column("COUNT")

    received = 0
    total = len(capability_statement.rest[0].resource)

    rich_console.print(f"{total} resource types on server. Receiving count of each resource type...")
    
    # helper function for consistent status text formation
    def format_status_text(rec, tot):
        return rich.text.Text(f"Processed {rec} of {tot} resources.")

    with rich.live.Live(rich.text.Text("Processing resources..."), transient=True) as live:
        for resource in capability_statement.rest[0].resource:
            bundle = bundle_read_from(resource.type + "?_summary=count", fc.server, novalidation)
            if display_zero or bundle.total > 0:
                table.add_row(resource.type, str(bundle.total))
            received += 1
            
            # update status
            live.update(format_status_text(received, total))

    # output results table
    rich_console.print(f"[green]{format_status_text(received, total)}")
    rich_console.print("")
    rich_console.print(table)

    return 0


def inspect_resource(
        fc: FHIRClient,
        capability_statement: CapabilityStatement,
        resource_type: str,
        limit: Union[None, int],
        inspect_items: bool,
        max_level: int,
        novalidation: bool
) -> int:
    """Receives and loops over all items of the given resource on the server.
    Counts and outputs elements of the resource as a tree view.

    Args:
        fc (FHIRClient): FHIRClient object
        capability_statement (CapabilityStatement): the CapabilityStatement object belonging to the fhir server
        resource_type (str): type of the resources to fetch
        limit (Union[None, int]): maximum number of resources to receive or None if no limit
        inspect_items (bool): when True, also inspect resources item values
        max_level (int): maximum level up to which the hierarchy is displayed in the tree view
        novalidation (bool): True if validation of fhir resources should be turned off

    Returns:
        int: status code
    """
    
    # limits for the inspect items feature:
    max_item_str_len = 50
    max_item_count = 50

    rich_console.print(f"Inspecting resource \"{resource_type}\" on FHIR server \"{fc.server.base_uri}\"...")

    # Items will be collected in the dictionary {items} as key value pairs.
    # The key will be the name of the item.
    # The value is either a list [count, dict()] with the overall count of the item and a dictionary to store item values and their count,
    # or it can also be another dictionary with key value pairs if the hierarchy branches further.
    items = dict()
    
    def process_entry(entry: dict, item_store: dict, level: int, inspect_items_pe: bool, max_level_pe: int):
        """Helper function for recursive processing of JSON-hierarchy."""
        
        # another helper function for item storing
        def store_item(key_si, subvalue_si, item_store_si):
            if key_si in item_store_si:
                item_store_si[key_si][0] += 1
            else:
                item_store_si[key_si] = [1, dict()]
            
            if inspect_items_pe:
                # store subvalue(s) count in dict
                # limit subvalues string length to max_item_str_len
                subvalue_si = str(subvalue_si)
                subvalue_si = (subvalue_si[:max_item_str_len] + '...') if len(subvalue_si) > (max_item_str_len + 3) else subvalue_si
                if subvalue_si in item_store_si[key_si][1]:
                    item_store_si[key_si][1][subvalue_si] += 1
                else:
                    item_store_si[key_si][1][subvalue_si] = 1
        
        for key, value in entry.items():
            # Some values come as lists, some not. Put values which are not lists into a list with just one item for further processing
            if type(value) != list:
                value = [value]
            for subvalue in value:  # process list of values
                # is subvalue a dictionary with key value pairs?
                if type(subvalue) == dict:
                    if level < max_level_pe:  # only process until max_level is reached
                        if key not in item_store:
                            item_store[key] = dict()
                        process_entry(subvalue, item_store[key], level + 1, inspect_items_pe, max_level_pe)
                    else:
                        store_item(key, subvalue, item_store)
                                
                else:  # subvalue is neither a list nor a dictionary --> store its count
                    store_item(key, subvalue, item_store)

    if fetch_resources(fc, capability_statement, resource_type, limit, novalidation, process_entry, True, [items, 0, inspect_items, max_level]) != 0:
        return 1  # an error occurred
    else:
        # output result tree view
        rich_console.print("")
        tree = rich.tree.Tree("[bold]" + resource_type)

        def build_tree(item: dict, tree_bt):
            """Helper function to build the tree view."""
            for key, value in item.items():
                if type(value) is not dict:
                    # construct branch string
                    branch_string = f"[bold]{key}[/bold]({value[0]})"
                    
                    if inspect_items:
                        branch_string += f"\n[{COLOR_GRAY}]"
                        # sort dictionary by count in descending order, limit to max_item_count entries
                        value[1] = dict(sorted(value[1].items(), key=lambda n: n[1], reverse=True)[:max_item_count])
                        # iterate over dictionary with stored item values and their count
                        for item_value, item_count in value[1].items():
                            branch_string += f"{item_value}({item_count}) "
                    
                    tree_bt.add(branch_string)
                else:
                    sub = tree_bt.add(f"[bold]{key}")
                    build_tree(value, sub)

        build_tree(items, tree)
        rich_console.print(tree)
        
        if inspect_items:
            rich_console.print(f"\nNotice: the output of item values is limited to {max_item_count}, sorted by count. Also the item string length is limited to {max_item_str_len} characters.")

        return 0


def list_structure_definitions(
        fc: FHIRClient,
        capability_statement: CapabilityStatement,
        novalidation: bool
) -> int:
    """Fetches the list of structure definitions from the FHIR server.

    Args:
        fc (FHIRClient): FHIRClient object
        capability_statement (CapabilityStatement): the CapabilityStatement object belonging to the fhir server
        novalidation (bool): True if validation of fhir resources should be turned off

    Returns:
        int: status code
    """

    rich_console.print(f"Fetching structure definitions of FHIR server \"{fc.server.base_uri}\"...")

    table = rich.table.Table(box=None, pad_edge=False)
    table.add_column("NAME")
    table.add_column("TYPE")
    table.add_column("URL")

    def process_entry(structure_definition):
        """Helper function for processing of received objects."""
        table.add_row(structure_definition.name, structure_definition.type, structure_definition.url)

    if fetch_resources(fc, capability_statement, "StructureDefinition", None, novalidation, process_entry, False, []) != 0:
        return 1  # an error occurred
    else:
        # output results table
        rich_console.print("")
        rich_console.print(table)
        return 0


# if started standalone, invoke main function
if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description=__doc__)
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("-l", "--list", help="list and count all resources", action="store_true")
        group.add_argument("-r", dest="resource", help="inspect given resource: output a tree view with resource items and their count")
        group.add_argument("-s", dest="structure_definitions", help="list structure definitions", action="store_true")
        parser.add_argument("--items", help="when inspecting a resource, also inspect resources item values", action="store_true")
        parser.add_argument("--level", metavar="N", dest="max_level", help="maximum level up to which the hierarchy is displayed in the tree view (default: 10)", type=int, default=10)
        parser.add_argument("--limit", metavar="N", dest="limit", help="limit number of FHIR resources to receive", type=int, default=None)
        parser.add_argument("--novalidation", help="turn validation off (default: on)", action="store_true")
        parser.add_argument("--zero", help="when listing all resources, output resources with count zero (will be omitted otherwise)", action="store_true")
        parser.add_argument("server_url", metavar="URL", help="URL of the FHIR server")
        parser.add_argument("--version", action="version", version="%(prog)s " + __version__)
        args = parser.parse_args()

        # check args
        if args.max_level <= 0:
            print("Value for \"--level\" must be >0.")
        else:
            main(args)

    except Exception:
        raise
