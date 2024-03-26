import requests

from flow_control.mpc_arith.arith_flow_guest import ArithFlowGuest
from flow_control.mpc_aggre.aggre_flow_guest import RSAFlowGuest

if __name__ == "__main__":
    x = RSAFlowGuest("20240129212301519.2_da8ed3")
    x.run_protocol()