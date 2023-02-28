import sqlalchemy.dialects.postgresql


def lookup_sqlalchemy_dtype(dtype: str):
    """Get dtype from full str."""
    if dtype.startswith("sqlalchemy.dialects.postgresql"):
        return getattr(sqlalchemy.dialects.postgresql, dtype.split(".")[-1])
    else:
        raise NotImplementedError()
