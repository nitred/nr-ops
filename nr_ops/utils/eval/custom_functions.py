import logging

logger = logging.getLogger(__name__)


def custom_raise_exception(msg: str):
    """A function that accepts as string msg and raises an Exception with that msg.

    This is a useful function to use in the eval() function since eval() does not
    allow raising exceptions using the `raise` keyword.

    Args:
        msg (str): The message to raise.
    """
    raise Exception(msg)


def custom_generate_batches_from_list(data: list, batch_size: int):
    """A function that accepts a list and batch size and returns a generator that
    yields batches from the input list.

    Args:
        data (list): The list to batch.
        batch_size (int): The size of each batch.

    Yields:
        list: A batch of items from input list.
    """
    logger.info(
        f"custom_generate_batches_from_list: Batching iterable of {type(data)=} "
        f"and {len(data)=} into batches of {batch_size=}."
    )
    for i in range(0, len(data), batch_size):
        yield data[i : i + batch_size]
