import argparse
import os
import shutil
import subprocess
import yaml

ROOT_DIR = os.path.dirname(os.path.expandvars(os.path.expanduser(__file__)))
ORIG_CONFIG_FILE = os.path.join(ROOT_DIR, 'config.yml')
if not os.path.isfile(ORIG_CONFIG_FILE):
    print(f"Failed to locate config file, cannot continue...")
    exit(1)
CONFIG_FILE = ORIG_CONFIG_FILE.replace('config.yml', 'node.yml')


def __disable_overlay():
    """
    If script fails at any point then disable
    """
    try:
        subprocess.call(['bash', ORIG_CONFIG_FILE.replace('config.yml', 'export_nebula.sh')])
    except Exception as error:
        print(f'Failed to disable lighthouse configs')
        raise


def __read_configs():
    """
    Read configuration file
    :return:
        content in configuration file
    """
    try:
        with open(ORIG_CONFIG_FILE, 'r') as yml_file:
            try:
                return yaml.safe_load(yml_file)
            except Exception as error:
                print(f"Failed to read configs from {CONFIG_FILE} (Error: {error})")
                __disable_overlay()
    except Exception as error:
        print(f"Failed to open configs file {CONFIG_FILE} (Error: {error})")
        __disable_overlay()


def __static_host_map(lighthouse_ip:str, lighthouse_node_ip:str):
    """
    For non-lighthouse nebula nodes, set the static map values, if both lighthouse and lighthouse_node IPs are available
    :args:
        lighthouse_ip:str - Lighthouse Nebula IP address
        lighthouse_node_ip:str - Lighthouse Node IP address
    :return:
        list policy for configs, if fails prints error message
    """
    if not lighthouse_ip or not lighthouse_node_ip:
        print(f"Missing lighthouse IP or physical node IP, cannot configure Nebula overlay for a non-lighthouse node")
        __disable_overlay()
    return f'{lighthouse_ip}: ["{lighthouse_node_ip}:4242"]'


def __write_configs(configs:dict):
    try:
        with open(CONFIG_FILE, 'w') as yml_file:
            try:
                yaml.dump(configs, yml_file, default_flow_style=False, Dumper=yaml.Dumper)
            except Exception as error:
                print(f"Failed to write configs into {CONFIG_FILE} (Error: {error})")
                __disable_overlay()
    except Exception as error:
        print(f"Failed to open configs file {CONFIG_FILE} (Error:{error})")
        __disable_overlay()


def main():
    """
    Generate configuration file for nebula based on user input
    positional arguments:
      cidr                  CIDR address
      tcp_port              AnyLog TCP port
      rest_port             AnyLog REST port
    optional arguments:
      -h, --help                                show this help message and exit
      --broker-port         BROKER_PORT         AnyLog Broker port
      --is-lighthouse       [IS_LIGHTHOUSE]     whether node is of type Lighthouse
      --lighthouse-node-ip  LIGHTHOUSE_NODE_IP  Lighthouse Node IP address
      --remote-cli          [REMOTE_CLI]        Open port 31800 for Remote-CLI
      --grafana             [GRAFANA]           Open port 3000 for Grafana
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('cidr', type=str, default='10.10.1.1/24', help='CIDR address')
    parser.add_argument('tcp_port', type=int, default=32048, help='AnyLog TCP port')
    parser.add_argument('rest_port', type=int, default=32049, help='AnyLog REST port')
    parser.add_argument('--broker-port', type=int, default=None, help='AnyLog Broker port')
    parser.add_argument("--is-lighthouse", type=bool, nargs='?', default=False, const=True, help='whether node is of type Lighthouse')
    parser.add_argument('--lighthouse-node-ip', type=str, default=None, help='Lighthouse Node IP address')
    parser.add_argument('--remote-cli', type=bool, default=False, nargs='?', const=True, help='Open port 31800 for Remote-CLI')
    parser.add_argument('--grafana', type=bool, default=False, nargs='?', const=True, help='Open port 3000 for Grafana')
    args = parser.parse_args()

    lighthouse_ip = args.cidr.split("/")[0]
    configs = __read_configs()

    configs['pki'] = {
        "ca": os.path.join(ROOT_DIR, 'ca.crt'),
        'cert': os.path.join(ROOT_DIR,  'host.crt'),
        'key': os.path.join(ROOT_DIR, 'host.key')
    }
    for section in range(len(configs['firewall']['inbound'])):
        if 'local_cidr' in configs['firewall']['inbound'][section]:
            configs['firewall']['inbound'][section]['local_cidr']  = args.cidr
    configs['lighthouse']['am_lighthouse'] = args.is_lighthouse
    del configs['static_host_map']
    if args.is_lighthouse is False:
        # configs['static_host_map'] = __static_host_map(lighthouse_ip=lighthouse_ip, lighthouse_node_ip=args.lighthouse_node_ip)
        configs['static_host_map'] = {
            lighthouse_ip: [f"{args.lighthouse_node_ip}:4242"]
        }
        configs['lighthouse']['hosts'] = [lighthouse_ip]
    else: 
        configs['lighthouse']['hosts'] = []

    """
    Configure the private interface. Note: addr is baked into the nebula certificate
    When tun is disabled, a lighthouse can be started without a local tun interface (and therefore without root)
    """
    configs['tun']['disable'] = False

    configs['firewall']['inbound'].append({
        'port': 4242,
        'proto': 'udp',
        'host': 'any'
    })
    for port in [args.tcp_port, args.rest_port]:
        configs['firewall']['inbound'].append({
            'port': port,
            'proto': 'tcp',
            'host': 'any'
        })

    # broker port
    if args.broker_port and args.broker_port != "":
        configs['firewall']['inbound'].append({
            'port': args.broker_port,
            'proto': 'tcp',
            'host': 'any'
        })

    # remote-cli
    if args.remote_cli is True:
        configs['firewall']['inbound'].append({
            'port': 31800,
            'proto': 'tcp',
            'host': 'any'
        })

    # Grafana
    if args.grafana is True:
        configs['firewall']['inbound'].append({
            'port': 3000,
            'proto': 'tcp',
            'host': 'any'
        })

    __write_configs(configs=configs)
    __disable_overlay()


if __name__ == '__main__':
    if not os.path.isfile(CONFIG_FILE):
        shutil.copy(ORIG_CONFIG_FILE, CONFIG_FILE)
    main()

