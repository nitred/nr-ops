import logging

from nr_ops.main_lambda import run_lambda

logger = logging.getLogger(__name__)


def handler(event, context):
    output = [msg.data for msg in run_lambda(b64_config_yaml=event["b64_config_yaml"])]
    response = {
        "context": {
            "function_name": context.function_name,
            "function_version": context.function_version,
            "invoked_function_arn": context.invoked_function_arn,
            "memory_limit_in_mb": context.memory_limit_in_mb,
            "aws_request_id": context.aws_request_id,
            "log_group_name": context.log_group_name,
            "log_stream_name": context.log_stream_name,
        },
        "output": output,
    }
    return response
