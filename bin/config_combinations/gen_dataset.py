import csv
import sys
import subprocess
import re

workload_cmds = {'scan': 'bin/workloads/sql/scan/spark/run.sh',
                 'join': 'bin/workloads/sql/join/spark/run.sh',
                 'aggregation': 'bin/workloads/sql/aggregation/spark/run.sh',
                 'wordcount': 'bin/workloads/micro/wordcount/spark/run.sh',
                 'terasort': 'bin/workloads/micro/terasort/spark/run.sh',
                 'sort': 'bin/workloads/micro/sort/spark/run.sh'}


def update_config_file(file_path, key, new_value):
    """
    Update a key-value pair in the configuration file.
    Args:
        file_path (str): Path to the configuration file to update
        key (str): The key to update.
        new_value (str): The new value for the key.
    """
    # Read the configuration file
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Update the key-value pair
    updated_lines = []
    for line in lines:
        if line.strip().startswith(key):
            updated_lines.append(f"{key} \t\t {new_value}\n")
        else:
            updated_lines.append(line)

    # Write the changes back to the file
    with open(file_path, 'w') as file:
        file.writelines(updated_lines)

    print(f"Configuration '{key}' updated to '{new_value}' successfully!")


def prep_configurations(path):
    """
    Prepare configurations and run a workload. Then save the output to a csv located in report/dataset.csv
    Args:
        path (str): Path to the configuration file.
    """
    # Open the CSV file in read mode
    with open(path, mode='r') as file:
        # Create a CSV reader object
        reader = csv.reader(file)

        # Skip the header row
        next(reader)

        # Iterate over each row in the CSV file
        for row in reader:
            print("===============Execution starting==================")
            exec_memory = row[1]
            serializer = row[2]
            exec_instances = row[3]
            exec_cores = row[4]
            def_parallelism = row[5]
            shuffle_partitions = row[6]
            broadcast_join_threshold = row[7]
            datasize = row[8]
            workload = row[9]

            update_config_file('conf/hibench.conf', 'hibench.scale.profile', datasize)
            update_config_file('conf/hibench.conf', 'hibench.default.map.parallelism', def_parallelism)
            update_config_file('conf/hibench.conf', 'hibench.default.shuffle.parallelism', shuffle_partitions)

            update_config_file('conf/spark.conf', 'spark.executor.memory', f"{exec_memory}g")
            update_config_file('conf/spark.conf', 'hibench.yarn.executor.num', exec_instances)
            update_config_file('conf/spark.conf', 'hibench.yarn.executor.cores', exec_cores)
            update_config_file('conf/spark.conf', 'spark.sql.autoBroadcastJoinThreshold',
                               f"{broadcast_join_threshold}m")
            update_config_file('conf/spark.conf', 'spark.serializer', serializer)
            cmd = workload_cmds.get(workload)
            print(subprocess.run([cmd], capture_output=True))

            # Execute the Bash command to get the last line of the file
            result = subprocess.run(['tail', '-n', '1', "report/hibench.report"], capture_output=True, text=True,
                                    check=True)
            # Capture the output and return it
            last_line_of_report = result.stdout.strip()

            # Remove multiple spaces using regular expression
            cleaned_string = re.sub(r'\s+', ' ', last_line_of_report)

            # Split the cleaned string by space
            split_string = cleaned_string.split()

            row.append(split_string[0])
            row.append(split_string[1])
            row.append(split_string[2])
            row.append(split_string[3])
            row.append(split_string[4])
            row.append(split_string[5])
            row.append(split_string[6])
            print("Output row==>")
            print(row)
            with open("report/dataset.csv", mode='a', newline='') as file:
                # Create a CSV reader object
                writer = csv.writer(file)
                writer.writerow(row)
            print("===============Execution ending==================")
            break


if __name__ == "__main__":
    if len(sys.argv) < 1:
        raise Exception(
            "Please supply the dataset generate outputs")
    dataset = sys.argv[1]
    prep_configurations(dataset)
