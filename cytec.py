import asyncio
from copy import deepcopy
from typing import Optional, Any
from functools import partial, reduce
from itertools import accumulate
from operator import concat
from dataclasses import dataclass, field, replace
from pathlib import Path
from pyvisa import ResourceManager, Resource, VisaIOError
from time import perf_counter
import pyvisa.constants

class CytecInitializationError(Exception):
    pass

class CytecSwitchingError(Exception):
    pass

def default_field(x):
    return field(default_factory = lambda: deepcopy(x))

CytecConnections = list[list[Optional[bool]]]

#MAX_COMMAND_LENGTH: int = 50
MAX_COMMAND_LENGTH: int = 60
SWITCHING_DELAY: float = 20e-3

@dataclass
class CytecState:
    modules: int = 8
    outputs: int = 16
    answerback: bool = True
    echo: bool = False
    verbose: bool = False
    connections: CytecConnections = default_field([[]])

@dataclass
class Cytec:
    resource: Resource
    state: CytecState

async def at(resource_name: str, state: CytecState = CytecState(), rm: ResourceManager = ResourceManager()):
    resource = rm.open_resource(resource_name)
    resource.read_termination = "\r"
    resource.write_termination = "\r"
    resource.flow_control = pyvisa.constants.ControlFlow.rts_cts
    resource.timeout = 500

    return await from_serial_resource(resource, state)

async def from_serial_resource(resource: Resource, state: CytecState = CytecState()):
    answerback_only = "A0 73;E0 73;V0 73;A1 73"
    try:
        rv = resource.query(answerback_only)
        if rv != "0":
            raise CytecInitializationError(f"Query of '{answerback_only}' failed")
    except VisaIOError as e:
        if resource.last_status != pyvisa.constants.StatusCode.error_serial_framing:
            raise CytecInitializationError(f"Query of '{answerback_only}' failed due to a visa error", e)
        rv = resource.query(answerback_only)
        if rv != "0":
            raise CytecInitializationError(f"Query of '{answerback_only}' failed after initial framing error")

    try:
        system_config = ";".join([
            "P2 1 73", # Set VX style chassis
            f"P10 {state.modules} 73", # Set number of switching modules
            f"P20 {state.outputs} 73", # Set number of outputs on motherboard
            ])
        resource.write(system_config)
        rv = resource.read_bytes(2 * 3)
        if rv != 3 * b"0\r":
            raise CytecInitializationError(f"Write of system configuration '{system_config}' failed")
    except VisaIOError as e:
        raise CytecInitializationError(f"Write of system configuration '{system_config}' failed due to a VISA error", e)

    init = Cytec(resource, state)
    return await connections(init)

def transpose(x: list[list[Any]]):
    return list(map(list, zip(*x)))

async def silenced(c: Cytec, force: bool = False):
    r, s = c.resource, c.state
    if force or s.answerback or s.echo or s.verbose:
        r.write("A0 73;E0 73;V0 73")
    return Cytec(r, replace(s,
        answerback = False,
        echo = False,
        verbose = False))

async def connections(c: Cytec):
    cn = await silenced(c)
    r, s = cn.resource, cn.state

    t0 = perf_counter()
    r.write("S")
    status = r.read_bytes((s.modules + 1) * s.outputs) # status bytes + \r for each line (no answerback)
    t1 = perf_counter()
    print(f"status time: {t1 - t0}")
    relay_states = {b"0"[0]: False, b"1"[0]: True}
    on = b"1"[0]
    new_connections = transpose([
            [relay_states.get(b, None) for b in columns]
            for columns in status.split(b"\r")[:-1]])

    new_s = replace(s, connections = new_connections)

    return Cytec(r, new_s)

def connections_string(c: CytecConnections):
    return "\n".join("".join(['1' if x else '0' for x in xs]) for xs in transpose(c))

def set_connection(c: CytecConnections, module: int, output: int, value: bool):
    new_c = deepcopy(c)
    new_c[module][output] = value
    return new_c

latch = partial(set_connection, value = True)
unlatch = partial(set_connection, value = False)

# Modified from https://stackoverflow.com/a/66538511
def splitsum(xs: list[Any], maximum_sum: int, on = lambda x: x):
    result, t = [[]], 0
    for x in xs:
        n = on(x)
        r, v, t = (result, [x], n) if t + n > maximum_sum else (result[-1], x, t + n)
        r.append(v)
    return result

async def set_connections(c: Cytec, new_connections: CytecConnections,
        update_connections_first = True,
        verify_after = True):
    t0 = perf_counter()
    if update_connections_first:
        return await set_connections(await connections(c), new_connections,
                update_connections_first = False,
                verify_after = verify_after)
    t1 = perf_counter()

    commands = [[("L" if q else "U", i, j)
        for j, (p, q) in enumerate(zip(xs, ys)) if p ^ q]
        for i, (xs, ys) in enumerate(zip(c.state.connections, new_connections))]

    grouped_commands_nested = [[f"{command}{i} {j}" if n == 0 else f"{command}{j}"
            for n, (command, i, j) in enumerate(output_commands)]
            for output_commands in commands]

    grouped_commands = [x for xs in grouped_commands_nested for x in xs]

    # +1 for the ";" separator between commands
    # A trailing ';' is the same length as the final '\r' in a command
    lengths = map(lambda c: len(c) + 1, grouped_commands)
    packs = splitsum(zip(grouped_commands, lengths), MAX_COMMAND_LENGTH, on = lambda x: x[1])
    packed_commands = [";".join([c for (c, _) in pack]) for pack in packs]

    cpre = await silenced(c)
    last_time = perf_counter()
    for packed_command in packed_commands:
        t2 = perf_counter()
        print(packed_command)
        cpre.resource.write(packed_command)
        t3 = perf_counter()
        last_time = perf_counter()

    if verify_after:
        while perf_counter() < last_time + SWITCHING_DELAY:
            pass
        t4 = perf_counter()
        cpost = await connections(cpre)
        #if cpost.state.connections != new_connections:
            #raise CytecSwitchingError()
        t5 = perf_counter()
        print(t1 - t0)
        print(t2 - t1)
        print(t3 - t2)
        print(t4 - t3)
        print(t5 - t4)
        print(f"= {t5 - t0}")
        return cpost

    return Cytec(c.resource, replace(c.state, connections = new_connections))

