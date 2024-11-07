import asyncio
import datetime
import json
import logging
import os
import time
from typing import Any

import execution
import server

_LOG_TIME = True
try: _LOG_TIME = os.getenv("COMFYUI_PROFILER_LOG_TIME", "true").lower() in ['true', '1']
except: pass

_PRECISION = 4
try: _PRECISION = int(os.getenv("COMFYUI_PROFILER_PRECISION", _PRECISION))
except: pass

exist_recursive_execute = execution.recursive_execute
exist_PromptExecutor_execute = execution.PromptExecutor.execute

profiler_data = {}
profiler_outputs = []

class HijackFormatter(logging.Formatter):
    job_id = None
    template_json_name = None
    service_name = None

    def formatTime(self, record, datefmt=None) -> str:
        # https://stackoverflow.com/questions/50873446/python-logger-output-dates-in-is8601-format
        return datetime.datetime.fromtimestamp(
            record.created,
            datetime.timezone.utc
        ).astimezone().isoformat(sep="T", timespec="milliseconds")

    def format(self, log):
        return json.dumps({
            "level": log.levelname,
            "message": log.getMessage(),
            "timestamp": self.formatTime(log),
            "job_id": HijackFormatter.job_id,
            "template_json_name": HijackFormatter.template_json_name,
            "service_name": HijackFormatter.service_name,
        })

    @staticmethod
    def hijack_logging():
        root_logger = logging.getLogger()

        # remove all existing handlers
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

        formatter = HijackFormatter()
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

HijackFormatter.hijack_logging()


async def send_message(data) -> None:
    s = server.PromptServer.instance
    await s.send_json('profiler', data)

def get_input_unique_ids(inputs) -> list:
    ret = []
    for key in inputs:
        input_data = inputs[key]
        if isinstance(input_data, list):
            ret.append(input_data[0])

    return ret


def get_total_inputs_time(current_item, prompt, calculated_inputs) -> tuple:
    input_unique_ids = get_input_unique_ids(prompt[current_item]['inputs'])
    total_time = profiler_data['nodes'].get(current_item, 0)
    calculated_nodes = calculated_inputs + [current_item]
    for id in input_unique_ids:
        if id in calculated_inputs:
            continue

        calculated_nodes += [id]
        t, calculated_inputs = get_total_inputs_time(id, prompt, calculated_nodes)
        total_time += t

    return total_time, calculated_nodes


def new_recursive_execute(server, prompt, outputs, current_item, extra_data, executed, prompt_id, outputs_ui, object_storage) -> Any:
    if not profiler_data.get('prompt_id') or profiler_data.get('prompt_id') != prompt_id:
        profiler_data['prompt_id'] = prompt_id
        profiler_data['nodes'] = {}
        profiler_outputs.clear()

    inputs = prompt[current_item]['inputs']
    input_unique_ids = get_input_unique_ids(inputs)
    executed_inputs = list(profiler_data['nodes'].keys())

    start_time = time.perf_counter()
    ret = exist_recursive_execute(server, prompt, outputs, current_item, extra_data, executed, prompt_id, outputs_ui, object_storage)
    end_time = time.perf_counter()

    profiler_data['nodes'][current_item] = 0
    this_time_nodes_time, _ = get_total_inputs_time(current_item, prompt, executed_inputs)
    profiler_data['nodes'][current_item] = end_time - start_time - this_time_nodes_time
    total_inputs_time, _ = get_total_inputs_time(current_item, prompt, [])

    asyncio.run(send_message({
        'node': current_item,
        'current_time': profiler_data['nodes'][current_item],
        'total_inputs_time': total_inputs_time,
        'prompt_id': prompt_id
    }))

    inputs_str = ''
    if len(input_unique_ids) > 0:
        inputs_str = '('
        for id in input_unique_ids:
            inputs_str += f'#{id} '
        inputs_str = inputs_str[:-1] + ')'

    profiler_outputs.append(f"[profiler] #{current_item} {prompt[current_item]['class_type']}: \
{round(profiler_data['nodes'][current_item], _PRECISION)} seconds, total {round(total_inputs_time, _PRECISION)} seconds{inputs_str}")

    return ret


def new_prompt_executor_execute(self, prompt, prompt_id, extra_data={}, execute_outputs=[]) -> Any:
    HijackFormatter.job_id = extra_data.get('job_id', None)
    HijackFormatter.template_json_name = extra_data.get('template_json_name', None)
    HijackFormatter.service_name = extra_data.get('service_name', None)

    ret = exist_PromptExecutor_execute(self, prompt, prompt_id, extra_data=extra_data, execute_outputs=execute_outputs)
    if _LOG_TIME:
        for profiler_output in profiler_outputs:
            logging.info(profiler_output)

    HijackFormatter.job_id = None
    HijackFormatter.template_json_name = None
    HijackFormatter.service_name = None
    return ret

execution.recursive_execute = new_recursive_execute
execution.PromptExecutor.execute = new_prompt_executor_execute

WEB_DIRECTORY = "."
NODE_CLASS_MAPPINGS = {}
