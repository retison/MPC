import requests

from flow_control.mpc_hash.hash_flow_guest import HashFlowGuest
from flow_control.mpc_rsa.rsa_flow_guest import RSAFlowGuest

if __name__ == "__main__":
    x = RSAFlowGuest("20240129212301519.2_da8ed3")
    x.run_protocol()