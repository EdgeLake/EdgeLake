import ipaddress


def validate_ip_in_cidr(ip: str, cidr: str) -> bool:
    """
    Validate if the given IP address is in the CIDR range.

    :param ip: The IP address as a string.
    :param cidr: The CIDR notation as a string.
    :return: True if IP is within CIDR range, otherwise False.
    """
    try:
        ip_addr = ipaddress.ip_address(ip)
        network = ipaddress.ip_network(cidr, strict=False)  # Allow non-strict network matching
        return ip_addr in network
    except ValueError as e:
        print(f"Invalid IP or CIDR provided: {e}")
        return False


# Example usage
ip = '192.168.1.10'
cidr = '192.168.1.0/24'

if validate_ip_in_cidr(ip, cidr):
    print(f"IP {ip} is within the CIDR {cidr}")
else:
    print(f"IP {ip} is not within the CIDR {cidr}")
