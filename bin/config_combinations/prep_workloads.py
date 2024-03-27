from gen_dataset import update_config_file
import subprocess

scale = ['tiny', 'small', 'large', 'huge', 'gigantic', 'bigdata']

workload_cmds = {'scan': 'bin/workloads/sql/scan/prepare/prepare.sh',
                 'join': 'bin/workloads/sql/join/prepare/prepare.sh',
                 'aggregation': 'bin/workloads/sql/aggregation/prepare/prepare.sh',
                 'wordcount': 'bin/workloads/micro/wordcount/prepare/prepare.sh',
                 'terasort': 'bin/workloads/micro/terasort/prepare/prepare.sh',
                 'sort': 'bin/workloads/micro/sort/prepare/prepare.sh'}

for workload in workload_cmds.values():
    for datasize in scale:
        print(workload, datasize)
        update_config_file('conf/hibench.conf', 'hibench.scale.profile', datasize)
        print(subprocess.run([workload], capture_output=True))
