def custom_raise_exception(msg: str):
    """A function that accepts as string msg and raises an Exception with that msg.

    This is a useful function to use in the eval() function since eval() does not
    allow raising exceptions using the `raise` keyword.

    Args:
        msg (str): The message to raise.
    """
    raise Exception(msg)
