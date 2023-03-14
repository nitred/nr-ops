import json
import logging

from nr_ops.main_lambda import run_lambda

logger = logging.getLogger(__name__)


def handler(event, context):
    """."""
    # Handle invocation via CLI/Function/Test
    if "b64_config_yaml" in event:
        b64_config_yaml = event["b64_config_yaml"]
        root_data = event.get("root_data", None)
        if not b64_config_yaml:
            raise ValueError("b64_config_yaml not provided")
        msg = (
            f"handler: Found b64_config_yaml in event! "
            f"{type(b64_config_yaml)=} | {type(root_data)=}"
        )
        print(msg)
        logger.info(msg)
    # Handle invocation via API/HTTP
    elif "b64_config_yaml" in event.get("body", ""):
        body = json.loads(event["body"])
        b64_config_yaml = body["b64_config_yaml"]
        root_data = body.get("root_data", None)
        msg = (
            f"handler: Found b64_config_yaml in event['body']! "
            f"{type(b64_config_yaml)=} | {type(root_data)=}"
        )
        print(msg)
        logger.info(msg)
    else:
        raise ValueError("Missing b64_config_yaml in both event and event['body']")

    output = [
        msg.data
        for msg in run_lambda(
            b64_config_yaml=b64_config_yaml,
            root_data=root_data,
        )
    ]

    msg = f"handler: Computed output! {len(output)=}"
    if len(output) > 0:
        msg += f" | {type(output[0])=}"
    print(msg)
    logger.info(msg)

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
