import argparse
import json
import multiprocessing
from time import sleep
from concurrent import futures
import constants
import grpc
import svc_pb2_grpc
from branch import Branch
from customer import Customer
from itertools import groupby


# Start branch gRPC server process
def serve_branch(branch: Branch, task_2_lock, task_3_lock):
    stub_list = []
    for branches in branch.other_branches:
        current_branch_stub = branches.createStub()
        stub_list.append(current_branch_stub)
    branch.createStub()
    branch.stubList = stub_list
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # Add the Branch servicer as a listener to the server
    svc_pb2_grpc.add_BranchServicer_to_server(branch, server)
    # Generate a listening port on the server for the branches
    port = "50000"
    port_list = list(port)
    id_list = list(str(branch.id))
    port_list[len(port_list) - len(id_list) :] = id_list
    str_port = "".join(port_list)
    server.add_insecure_port(f"localhost:{str_port}")
    print(f"Listening on port {str_port}")
    server.start()

    # Wait before writing
    sleep(0.5 * branch.id)
    with task_2_lock:
        print(f"Writing Task 2 results of Branch {branch.id} process")
        output_array = json.load(open("branch_outputs.json"))
        output_array.append(branch.return_task_2_output())
        output = json.dumps(output_array)
        with open("branch_outputs.json", "w") as output_file:
            output_file.write(output)

    with task_3_lock:
        print(f"Writing Task 3 results of Branch {branch.id} process")
        output_array = json.load(open("events_outputs.json"))
        output_array.extend(branch.return_task_3_output())
        output = json.dumps(output_array)
        with open("events_outputs.json", "w") as output_file:
            output_file.write(output)
    server.wait_for_termination()


# Start customer gRPC client processes
def serve_customer(obj: Customer, task_1_lock, task_3_lock):
    obj.createStub()
    obj.executeEvents()
    with task_1_lock:
        print(f"Writing Task 1 results of Customer {obj.id} process")
        output_array = json.load(open("customer_outputs.json"))
        output_array.append(obj.task_1_output())
        output = json.dumps(output_array)
        with open("customer_outputs.json", "w") as output_file:
            output_file.write(output)
        sleep(0.25)
    with task_3_lock:
        print(f"Writing Task 3 results of Customer {obj.id} process")
        output_array = json.load(open("events_outputs.json"))
        output_array.extend(obj.return_task_3_output())
        output = json.dumps(output_array)
        with open("events_outputs.json", "w") as output_file:
            output_file.write(output)


def sort_json_by_type_and_id(json_data):
    """Sorts JSON data by type and id, with customer types coming first within each id group.

    Args:
      json_data: A list of JSON objects.

    Returns:
      A list of JSON objects sorted by type and id, with customer types coming first
      within each id group.
    """

    # Create a dictionary to store the JSON objects grouped by id.
    id_to_json_objects = {}
    for json_object in json_data:
        _id = json_object["id"]
        if _id not in id_to_json_objects:
            id_to_json_objects[_id] = []
        id_to_json_objects[_id].append(json_object)

    # Sort the JSON objects in each id group by type, with customer types coming first.
    for json_objects in id_to_json_objects.values():
        json_objects.sort(key=lambda json_object: json_object["type"], reverse=True)

    # Flatten the dictionary of id groups into a single list of JSON objects.
    sorted_json_data = []
    for json_objects in id_to_json_objects.values():
        sorted_json_data.extend(json_objects)

    return sorted_json_data


# Parse JSON & create objects/processes
def serve(processes):
    # List of Customer objects
    customers = []
    # List of Customer processes
    customer_processes = []
    # List of Branch objects
    branches = []
    # List of Branch IDs
    branch_ids = []
    # List of Branch processes
    branch_processes = []

    stub_list = []
    # Instantiate the objects of the Branch class
    for item in processes:
        if item[constants.TYPE_FIELD] == constants.BRANCH:
            branch_obj = Branch(
                id=item[constants.ID_FIELD], balance=item[constants.BALANCE_FIELD]
            )
            # Create a Branch stub for each object of the Branch class
            current_branch_stub = branch_obj.createStub()
            stub_list.append(current_branch_stub)
    # Instantiate Branch objects
    for process in processes:
        if process[constants.TYPE_FIELD] == constants.BRANCH:
            branch = Branch(
                process[constants.ID_FIELD],
                process[constants.BALANCE_FIELD],
                branch_ids,
            )
            branches.append(branch)
            branch_ids.append(branch.id)

    # Spawn Branch processes
    task_2_lock = multiprocessing.Lock()
    task_3_lock = multiprocessing.Lock()
    for branch in branches:
        branch.other_branches = branches
        # branch_process = threading.Thread(target=serve_branch, args=(branch,))
        branch_process = multiprocessing.Process(
            target=serve_branch, args=(branch, task_2_lock, task_3_lock)
        )
        branch_processes.append(branch_process)
        branch_process.start()

    # Allow branch processes to start
    sleep(0.25)

    # Instantiate Customer objects
    for process in processes:
        if process[constants.TYPE_FIELD] == constants.CUSTOMER:
            for i, each_item in enumerate(process[constants.CUSTOMER_REQUESTS]):
                each_item[constants.LOGICAL_CLOCK_FIELD] = i + 1
            customer = Customer(
                process[constants.ID_FIELD],
                process[constants.CUSTOMER_REQUESTS],
            )
            customers.append(customer)

    # Start Customer processes concurrently
    task_1_lock = multiprocessing.Lock()
    for customer in customers:
        customer_process = multiprocessing.Process(
            target=serve_customer, args=(customer, task_1_lock, task_3_lock)
        )
        customer_processes.append(customer_process)
        customer_process.start()

    # Allow customers to complete processing before terminating
    sleep(0.5)
    # Wait till all Customer processes complete
    for customerProcess in customer_processes:
        customerProcess.join()

    # Allow branches to complete processing before terminating
    sleep(10)

    # Terminate Branch processes
    for branch_process in branch_processes:
        branch_process.terminate()

    output_array = json.load(open("events_outputs.json"))

    grouped_data = groupby(
        sorted(
            output_array, key=lambda x: (x["customer-request-id"], x["logical_clock"])
        ),
        key=lambda x: x["customer-request-id"],
    )

    inter_data = [
        {
            "customer-request-id": k,
            "events": sorted(list(g), key=lambda x: x["logical_clock"]),
        }
        for k, g in grouped_data
    ]

    flattened_data = []
    for data in inter_data:
        flattened_data.extend(data["events"])

    output = json.dumps(flattened_data)
    with open("events_outputs.json", "w") as output_file:
        output_file.write(output)


if __name__ == "__main__":
    # Setup command line argument for 'input_file'
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file")
    args = parser.parse_args()

    try:
        # Load JSON file from 'input_file' arg
        input = json.load(open(args.input_file))

        with open("customer_outputs.json", "w") as f:
            f.write("[]")
        with open("branch_outputs.json", "w") as f:
            f.write("[]")
        with open("events_outputs.json", "w") as f:
            f.write("[]")
        # Create objects/processes from input file
        serve(input)

        # Write events to output file
        sleep(1.5)
    except FileNotFoundError:
        print(f"Could not find input file '{args.input_file}'")
