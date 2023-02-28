def validate_hook_type_and_config(cls, values):
    """Validate that the config matches the type."""
    hook_type = values.get("hook_type")
    hook_config = values.get("hook_config")

    from nr_ops.ops.op_collection import OP_COLLECTION

    if hook_type not in OP_COLLECTION:
        raise ValueError(
            f"Hook with {hook_type=} was NOT FOUND in OP_COLLECTION. "
            f"Please check for typos in the `hook_type` or make sure the operator "
            f"class is correctly imported in OP_COLLECTION. FYI, a hook is also "
            f"an Connector Op that must exist in the OP_COLLECTION."
        )

    # Check if hook_config is a valid config for op_type.
    hook_model_cls = OP_COLLECTION[hook_type].OP_CONFIG_MODEL
    # hook_model is a pydantic model that is used to validate the hook_config.
    hook_model_cls(**hook_config)

    return values
